use crate::task::TaskManager;
use chrono::{DateTime, TimeZone, Utc};
use common::interface::ModuleTrait;
use common::model::entity::{
    account, module, platform, rel_account_platform, rel_module_account, rel_module_platform,
};
use common::model::message::{TaskModel};
use common::state::State;
use cron::Schedule;
use log::{debug, error, info, warn};
use queue::QueueManager;
use sea_orm::{ColumnTrait, EntityTrait, QueryFilter};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::RwLock;
use tokio::time::{Duration, sleep};

pub struct CronScheduler {
    task_manager: Arc<TaskManager>,
    state: Arc<State>,
    queue_manager: Arc<QueueManager>,
    // Cache for parsed schedules to avoid re-parsing every tick
    // Key: Module Name, Value: Schedule
    schedule_cache: RwLock<HashMap<String, Schedule>>,
    // Track the last processed minute to avoid redundant checks within the same minute
    last_tick: tokio::sync::Mutex<Option<DateTime<Utc>>>,
}

impl CronScheduler {
    pub fn new(
        task_manager: Arc<TaskManager>,
        state: Arc<State>,
        queue_manager: Arc<QueueManager>,
    ) -> Self {
        Self {
            task_manager,
            state,
            queue_manager,
            schedule_cache: RwLock::new(HashMap::new()),
            last_tick: tokio::sync::Mutex::new(None),
        }
    }

    pub fn start(self: Arc<Self>) {
        tokio::spawn(async move {
            self.run().await;
        });
    }

    async fn run(&self) {
        info!("CronScheduler started");
        loop {
            let now = Utc::now();
            let current_minute_ts = now.timestamp() / 60 * 60;

            if let Some(current_minute) = Utc.timestamp_opt(current_minute_ts, 0).single() {
                let mut last_tick_guard = self.last_tick.lock().await;
                let should_run = match *last_tick_guard {
                    Some(last) => current_minute > last,
                    None => true,
                };

                if should_run {
                    *last_tick_guard = Some(current_minute);
                    drop(last_tick_guard); // Release lock before processing

                    self.process_tick(current_minute).await;
                }
            }

            // Check every second to execute as close to the minute boundary as possible
            sleep(Duration::from_secs(1)).await;
        }
    }

    async fn process_tick(&self, current_minute: DateTime<Utc>) {
        let modules = self.task_manager.get_all_modules().await;

        for module in modules {
            let module_name = module.name();
            // 1. Try to get schedule from cache
            let schedule_opt = {
                let cache = self.schedule_cache.read().unwrap();
                cache.get(&module_name).cloned()
            };

            let schedule = if let Some(s) = schedule_opt {
                s
            } else {
                // 2. If not in cache, get from module and cache it
                if let Some(cron_config) = module.cron() {
                    let s = cron_config.schedule;
                    let mut cache = self.schedule_cache.write().unwrap();
                    cache.insert(module_name.clone(), s.clone());
                    s
                } else {
                    continue;
                }
            };

            // 3. Check schedule
            if self.is_schedule_match(&schedule, current_minute) {
                // Try lock
                let lock_key = format!("cron:{}:{}", module.name(), current_minute.timestamp());
                // TTL = 65 seconds
                match self
                    .state
                    .cache_service
                    .set_nx(&lock_key, b"1", Some(Duration::from_secs(65)))
                    .await
                {
                    Ok(true) => {
                        info!(
                            "Triggering cron task for module: {} at {}",
                            module_name, current_minute
                        );
                        self.trigger_module(module, current_minute).await;
                    }
                    Ok(false) => {
                        debug!(
                            "Cron task for module {} at {} already running or locked",
                            module_name, current_minute
                        );
                    }
                    Err(e) => {
                        error!("Failed to acquire lock for cron task: {}", e);
                    }
                }
            }
        }
    }

    fn is_schedule_match(&self, schedule: &Schedule, target: DateTime<Utc>) -> bool {
        // Check if `target` is in the schedule.
        // We look for the next occurrence AFTER `target - 1 sec`.
        // If it equals `target`, it's a match.
        let check_time = target - chrono::Duration::seconds(1);
        if let Some(next) = schedule.after(&check_time).next() {
            return next == target;
        }
        false
    }

    async fn trigger_module(&self, target_module: Arc<dyn ModuleTrait>, _run_time: DateTime<Utc>) {
        let contexts_result = self.fetch_execution_contexts(target_module.name()).await;

        let contexts = match contexts_result {
            Ok(ctx) => ctx,
            Err(e) => {
                error!(
                    "Failed to fetch execution contexts for module {}: {}",
                    target_module.name(),
                    e
                );
                return;
            }
        };

        if contexts.is_empty() {
            warn!(
                "Module {} triggered but no valid execution contexts (platform/account) found",
                target_module.name()
            );
            return;
        }

        let tasks: Vec<TaskModel> = contexts
            .into_iter()
            .map(|(acc_name, plat_name)| TaskModel {
                account: acc_name,
                platform: plat_name,
                module: Some(vec![target_module.name()]),
                run_id: uuid::Uuid::now_v7(),
            })
            .collect();
        info!("success create tasks for module {:?}", tasks);

        let topic = "task";

        if let Some(backend) = &self.queue_manager.backend {
            for task in tasks {
                match serde_json::to_vec(&task) {
                    Ok(payload) => {
                        if let Err(e) = backend.publish(topic, &payload).await {
                            error!(
                                "Failed to publish cron task for module {}: {}",
                                target_module.name(),
                                e
                            );
                        } else {
                            debug!("Published cron task for module {}", target_module.name());
                        }
                    }
                    Err(e) => error!("Failed to serialize task: {}", e),
                }
            }
        } else {
            warn!("No queue backend available for cron task");
        }
    }

    async fn fetch_execution_contexts(
        &self,
        module_name: String,
    ) -> Result<Vec<(String, String)>, sea_orm::DbErr> {
        // 1. Get Module ID
        let module_entity_opt = module::Entity::find()
            .filter(module::Column::Name.eq(&module_name))
            .one(&*self.state.db)
            .await?;

        let module_id = match module_entity_opt {
            Some(m) => m.id,
            None => {
                warn!("Module {} not found in database", module_name);
                return Ok(vec![]);
            }
        };

        // 2. Get Enabled Accounts for Module
        // We only care about accounts explicitly linked to this module.
        let account_rels = rel_module_account::Entity::find()
            .filter(rel_module_account::Column::ModuleId.eq(module_id))
            .filter(rel_module_account::Column::Enabled.eq(true))
            .all(&*self.state.db)
            .await?;

        if account_rels.is_empty() {
            debug!("No enabled accounts found for module {}", module_name);
            return Ok(vec![]);
        }

        let account_ids: Vec<i32> = account_rels.iter().map(|r| r.account_id).collect();

        // 3. Get Enabled Platforms for Module
        let platform_rels = rel_module_platform::Entity::find()
            .filter(rel_module_platform::Column::ModuleId.eq(module_id))
            .filter(rel_module_platform::Column::Enabled.eq(true))
            .all(&*self.state.db)
            .await?;

        if platform_rels.is_empty() {
            warn!(
                "Module {} has enabled accounts but no enabled platforms",
                module_name
            );
            return Ok(vec![]);
        }

        let valid_platform_ids: HashSet<i32> =
            platform_rels.iter().map(|r| r.platform_id).collect();

        // 4. Find Valid (Account, Platform) Pairs
        // Query rel_account_platform where account is in our list AND platform is in our valid list
        let valid_acc_plat_rels = rel_account_platform::Entity::find()
            .filter(rel_account_platform::Column::AccountId.is_in(account_ids.clone()))
            .filter(rel_account_platform::Column::PlatformId.is_in(valid_platform_ids.clone()))
            .filter(rel_account_platform::Column::Enabled.eq(true))
            .all(&*self.state.db)
            .await?;

        if valid_acc_plat_rels.is_empty() {
            return Ok(vec![]);
        }

        // 5. Resolve Names
        let valid_acc_ids: Vec<i32> = valid_acc_plat_rels.iter().map(|r| r.account_id).collect();
        let accounts = account::Entity::find()
            .filter(account::Column::Id.is_in(valid_acc_ids))
            .all(&*self.state.db)
            .await?;

        let platforms = platform::Entity::find()
            .filter(platform::Column::Id.is_in(valid_platform_ids))
            .all(&*self.state.db)
            .await?;

        let account_map: HashMap<i32, String> =
            accounts.into_iter().map(|a| (a.id, a.name)).collect();
        let platform_map: HashMap<i32, String> =
            platforms.into_iter().map(|p| (p.id, p.name)).collect();

        let mut results = Vec::new();
        for rel in valid_acc_plat_rels {
            if let (Some(acc_name), Some(plat_name)) = (
                account_map.get(&rel.account_id),
                platform_map.get(&rel.platform_id),
            ) {
                results.push((acc_name.clone(), plat_name.clone()));
            }
        }

        Ok(results)
    }
}

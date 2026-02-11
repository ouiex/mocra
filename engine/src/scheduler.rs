use crate::task::TaskManager;
use chrono::{DateTime, TimeZone, Utc};
use common::model::entity::{
    account, module, platform, rel_account_platform, rel_module_account, rel_module_platform,
};
use common::model::message::TaskModel;
use common::model::CronConfig;
use common::state::State;
use cron::Schedule;
use log::{error, info, warn};
use queue::{QueuedItem, QueueManager};
use sea_orm::{ColumnTrait, EntityTrait, QueryFilter, QuerySelect, JoinType, RelationTrait};
use sea_orm::prelude::Expr;
use dashmap::DashMap;
use std::collections::{HashSet, hash_map::DefaultHasher};
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::time::{Duration, sleep};
use std::time::{SystemTime, UNIX_EPOCH};
use futures::StreamExt;

use sync::LeaderElector;
use tokio::sync::broadcast;
use metrics::{counter, histogram};

/// Scheduler for triggering time-based tasks.
/// 
/// Uses a Leader Election mechanism to ensure unique execution and reduce contention.
/// Only the leader node participates in scheduling.
pub struct CronScheduler {
    task_manager: Arc<TaskManager>,
    state: Arc<State>,
    queue_manager: Arc<QueueManager>,
    // Cache for parsed schedules and contexts
    // Key: Module Name, Value: (Schedule, Contexts)
    schedule_cache: DashMap<String, (Arc<Schedule>, Arc<Vec<(String, String)>>)>,
    // Cache for cron configs to avoid repeated parsing
    cron_config_cache: DashMap<String, Option<CronConfig>>,
    // Cache to avoid repeating right_now executions for same contexts
    right_now_context_hash: DashMap<String, u64>,
    shutdown_rx: broadcast::Receiver<()>,
    last_version: AtomicU64,
    last_module_hash: AtomicU64,
    last_refresh_at_ms: AtomicU64,
    leader_elector: Arc<LeaderElector>,
}

impl CronScheduler {
    /// Creates a new CronScheduler instance.
    pub async fn new(
        task_manager: Arc<TaskManager>,
        state: Arc<State>,
        queue_manager: Arc<QueueManager>,
        shutdown_rx: broadcast::Receiver<()>,
        leader_elector: Arc<LeaderElector>,
    ) -> Self {
        Self {
            task_manager,
            state,
            queue_manager,
            schedule_cache: DashMap::new(),
            cron_config_cache: DashMap::new(),
            right_now_context_hash: DashMap::new(),
            shutdown_rx,
            last_version: AtomicU64::new(0),
            last_module_hash: AtomicU64::new(0),
            last_refresh_at_ms: AtomicU64::new(0),
            leader_elector,
        }
    }

    async fn get_misfire_tolerance(&self) -> i64 {
        let config = self.state.config.read().await;
        config.scheduler
            .as_ref()
            .and_then(|s| s.misfire_tolerance_secs)
            .unwrap_or(300)
    }

    /// Starts the scheduler loop in background tasks.
    ///
    /// Consumes the Arc<Self> to spawn tasks.
    pub fn start(self: Arc<Self>) {
        // Spawn Refresh Loop
        let this = self.clone();
        tokio::spawn(async move {
            this.refresh_loop().await;
        });

        // Spawn Main Loop
        tokio::spawn(async move {
            self.run().await;
        });
    }

    async fn refresh_loop(&self) {
        info!("CronScheduler refresh loop started");
        let mut shutdown = self.shutdown_rx.resubscribe();
        loop {
            self.refresh_cache().await;
            // Refresh at configured interval to pick up changes
            let refresh_interval_secs = self
                .state
                .config
                .read()
                .await
                .scheduler
                .as_ref()
                .and_then(|s| s.refresh_interval_secs)
                .unwrap_or(60);
            tokio::select! {
                _ = shutdown.recv() => {
                    info!("CronScheduler refresh loop received shutdown signal");
                    break;
                }
                _ = sleep(Duration::from_secs(refresh_interval_secs)) => {}
            }
        }
    }

    async fn refresh_cache(&self) {
        // Check Redis for version to avoid unnecessary DB queries
        let namespace = self.state.cache_service.namespace();
        let redis_version_key = if namespace.is_empty() {
            "scheduler:config_version".to_string()
        } else {
            format!("{namespace}:scheduler:config_version")
        };
        let remote_version_bytes = self
            .state
            .cache_service
            .get(&redis_version_key)
            .await
            .ok()
            .flatten();
        let remote_version: u64 = if let Some(bytes) = remote_version_bytes {
            String::from_utf8(bytes).ok().and_then(|s| s.parse().ok()).unwrap_or(0)
        } else {
            0
        };

        let local_version = self.last_version.load(Ordering::Relaxed);
        let modules = self.task_manager.get_all_modules().await;
        let module_signatures: Vec<(String, i32)> = modules
            .iter()
            .map(|m| (m.name(), m.version()))
            .collect();
        let module_names: Vec<String> = module_signatures
            .iter()
            .map(|(name, _)| name.clone())
            .collect();
        let module_hash = Self::hash_module_signatures(&module_signatures);
        let local_module_hash = self.last_module_hash.load(Ordering::Relaxed);

        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        let last_refresh_at_ms = self.last_refresh_at_ms.load(Ordering::Relaxed);
        let max_staleness_secs = self
            .state
            .config
            .read()
            .await
            .scheduler
            .as_ref()
            .and_then(|s| s.max_staleness_secs)
            .unwrap_or(120);
        let staleness_exceeded = Self::staleness_exceeded(now_ms, last_refresh_at_ms, max_staleness_secs);

        // If version hasn't changed (and is not 0) and module set unchanged, skip refresh.
        // Note: 0 usually means "not set" or "force refresh"
        if !staleness_exceeded && remote_version > 0 && remote_version == local_version && module_hash == local_module_hash {
             return; 
        }
        if module_hash != local_module_hash {
            self.cron_config_cache.clear();
        }
        let module_set: HashSet<String> = module_names.iter().cloned().collect();

        let start = std::time::Instant::now();
        match self.fetch_all_enabled_contexts().await {
            Ok(contexts) => {
                let fetch_duration = start.elapsed();
                
                let context_count = contexts.len();
                
                // Group by module
                let mut context_map: std::collections::HashMap<String, Vec<(String, String)>> = std::collections::HashMap::new();
                for (m, a, p) in contexts {
                    context_map.entry(m).or_default().push((a, p));
                }

                for (module, name) in modules.into_iter().zip(module_names.iter()) {
                    let cron_config = if let Some(entry) = self.cron_config_cache.get(name) {
                        entry.value().clone()
                    } else {
                        let config = module.cron();
                        self.cron_config_cache.insert(name.clone(), config.clone());
                        config
                    };
                    // We only care if the module has a cron schedule
                    if let Some(cron_config) = cron_config {
                         if !cron_config.enable {
                              self.schedule_cache.remove(name);
                              self.right_now_context_hash.remove(name);
                              continue;
                          }
                          let contexts = context_map.remove(name).unwrap_or_default();
                          if cron_config.right_now || cron_config.run_now_and_schedule {
                               if contexts.is_empty() {
                                    self.schedule_cache.remove(name);
                                    self.right_now_context_hash.remove(name);
                                    continue;
                                }

                                let context_hash = Self::hash_contexts(&contexts);
                                let last_hash = self.right_now_context_hash.get(name).map(|entry| *entry.value());
                                if last_hash != Some(context_hash) {
                                    self.right_now_context_hash.insert(name.clone(), context_hash);
                                    let now = Utc::now();
                                    self.process_module_contexts(name, &contexts, now).await;
                                }
                                if cron_config.right_now {
                                    self.schedule_cache.remove(name);
                                    continue;
                                }
                            }
                          if !contexts.is_empty() {
                               let schedule = Arc::new(cron_config.schedule.clone());
                               self.schedule_cache.insert(name.clone(), (schedule, Arc::new(contexts)));
                          } else {
                                // No active contexts, remove from cache to avoid checking
                                self.schedule_cache.remove(name);
                          }
                    } else {
                        // Module has no cron, ensure it's not in cache
                        self.schedule_cache.remove(name);
                    }
                }

                let stale_keys: Vec<String> = self
                    .schedule_cache
                    .iter()
                    .filter_map(|entry| {
                        if module_set.contains(entry.key()) {
                            None
                        } else {
                            Some(entry.key().clone())
                        }
                    })
                    .collect();
                for key in stale_keys {
                    self.schedule_cache.remove(&key);
                }
                let stale_cron_keys: Vec<String> = self
                    .cron_config_cache
                    .iter()
                    .filter_map(|entry| {
                        if module_set.contains(entry.key()) {
                            None
                        } else {
                            Some(entry.key().clone())
                        }
                    })
                    .collect();
                for key in stale_cron_keys {
                    self.cron_config_cache.remove(&key);
                }
                let stale_right_now_keys: Vec<String> = self
                    .right_now_context_hash
                    .iter()
                    .filter_map(|entry| {
                        if module_set.contains(entry.key()) {
                            None
                        } else {
                            Some(entry.key().clone())
                        }
                    })
                    .collect();
                for key in stale_right_now_keys {
                    self.right_now_context_hash.remove(&key);
                }
                
                // Update local version after successful refresh
                if remote_version > 0 {
                    self.last_version.store(remote_version, Ordering::Relaxed);
                }
                self.last_module_hash.store(module_hash, Ordering::Relaxed);
                self.last_refresh_at_ms.store(now_ms, Ordering::Relaxed);

                let process_duration = start.elapsed() - fetch_duration;
                info!(
                    "CronScheduler cache refreshed in {:?}. Fetch: {:?}, Process: {:?}. Total contexts: {}. Active scheduled modules: {}", 
                    start.elapsed(), fetch_duration, process_duration, context_count, self.schedule_cache.len()
                );
            },
            Err(e) => {
                error!("Failed to refresh cron contexts: {}", e);
            }
        }
    }

    fn hash_module_signatures(signatures: &[(String, i32)]) -> u64 {
        let mut sorted = signatures.to_vec();
        sorted.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));
        let mut hasher = DefaultHasher::new();
        for (name, version) in sorted {
            name.hash(&mut hasher);
            version.hash(&mut hasher);
        }
        hasher.finish()
    }

    fn hash_contexts(contexts: &[(String, String)]) -> u64 {
        let mut sorted: Vec<(String, String)> = contexts.to_vec();
        sorted.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));
        let mut hasher = DefaultHasher::new();
        for (account, platform) in sorted {
            account.hash(&mut hasher);
            platform.hash(&mut hasher);
        }
        hasher.finish()
    }

    /// Main scheduler loop.
    ///
    /// Triggers tasks at the top of every minute.
    async fn run(self: Arc<Self>) {
        // Start the leader elector in background
        {
            let elector = self.leader_elector.clone();
            tokio::spawn(async move {
                elector.start().await;
            });
        }

        info!("CronScheduler started (Leader Election Mode)");
        let mut last_tick: Option<DateTime<Utc>> = None;

        loop {
            let now = Utc::now();
            let current_minute_ts = now.timestamp() / 60 * 60;

            if let Some(current_minute) = Utc.timestamp_opt(current_minute_ts, 0).single() {
                // Only Leader processes ticks
                if self.leader_elector.is_leader() {
                    // Misfire Handling Logic
                    if let Some(last_run) = last_tick {
                        let diff = current_minute.signed_duration_since(last_run).num_seconds();
                        
                        if diff > 60 {
                            // We missed some ticks!
                            let misfire_tolerance = self.get_misfire_tolerance().await;
                            if diff <= misfire_tolerance {
                                 info!("Detected missed ticks. Catching up from {} to {}", last_run, current_minute);
                                 // Catch up logic: Run for each missed minute
                                 let mut cursor = last_run + chrono::Duration::seconds(60);
                                 while cursor <= current_minute {
                                     self.clone().process_tick(cursor).await;
                                     cursor += chrono::Duration::seconds(60);
                                 }
                                 last_tick = Some(current_minute);
                            } else {
                                 warn!("Missed ticks gap ({}) exceeds tolerance ({}). Skipping catch-up, setting last_tick to now.", diff, misfire_tolerance);
                                 last_tick = Some(current_minute);
                                 self.clone().process_tick(current_minute).await;
                            }
                        } else if diff > 0 {
                             // Normal tick
                             last_tick = Some(current_minute);
                             self.clone().process_tick(current_minute).await;
                        }
                    } else {
                         // First run
                         last_tick = Some(current_minute);
                         self.clone().process_tick(current_minute).await;
                    }
                } else {
                     // Not leader, just update last_tick to stay in sync roughly, or do nothing.
                     // Updating last_tick ensures if we BECOME leader, we don't think we missed hours of ticks.
                     last_tick = Some(current_minute);
                }
            }

            // Sleep until the start of the next minute
            let now = Utc::now();
            let next_minute = (now.timestamp() / 60 + 1) * 60;
            // Ensure sleep_duration is non-negative
            let sleep_secs = (next_minute - now.timestamp()).max(0) as u64;
            let sleep_duration = std::time::Duration::from_secs(sleep_secs);
            // Add a small buffer to ensure we land inside the next minute
            
            let mut shutdown = self.shutdown_rx.resubscribe();
            tokio::select! {
                _ = shutdown.recv() => {
                    info!("CronScheduler main loop received shutdown signal");
                    break;
                }
                _ = sleep(sleep_duration + Duration::from_millis(100)) => {}
            }
        }
    }

    async fn process_tick(self: Arc<Self>, current_minute: DateTime<Utc>) {
        // Iterate over cached schedules
        // DashMap iter() locks shards, but for reading it's fine.
        // We collect keys first to avoid holding locks while processing if we want, 
        // but DashMap is designed for concurrent access.
        
        let mut tasks = Vec::new();
        
        for r in self.schedule_cache.iter() {
            let (module_name, (schedule, contexts)) = r.pair();
            
            if Self::is_schedule_match(schedule, current_minute) {
                tasks.push((module_name.clone(), contexts.clone()));
            }
        }

        // Process matches
        let start = std::time::Instant::now();
        let mut total_triggered = 0;
        
        for (module_name, contexts) in tasks {
            let this = self.clone();
            total_triggered += contexts.len();
            tokio::spawn(async move {
                 this.process_module_contexts(&module_name, &contexts, current_minute).await;
            });
        }
        
        let duration = start.elapsed().as_secs_f64();
        histogram!("scheduler_tick_duration_seconds").record(duration);
        if total_triggered > 0 {
            info!("Scheduler tick processed {} potential tasks in {:.4}s", total_triggered, duration);
        }
    }

    async fn process_module_contexts(&self, module_name: &str, contexts: &[(String, String)], current_minute: DateTime<Utc>) {
        // Iterate over each context and try to acquire lock individually
        // Parallelize using for_each_concurrent to maximize throughput
        let concurrency = self.state.config.read().await.scheduler
            .as_ref()
            .and_then(|s| s.concurrency)
            .unwrap_or(100);

        let timestamp = current_minute.timestamp();
        let namespace_prefix = {
            let ns = self.state.cache_service.namespace();
            if ns.is_empty() {
                None
            } else {
                Some(ns.to_string())
            }
        };
        let namespace_prefix = Arc::new(namespace_prefix);
        // Increased batch size for better throughput
        let batch_size = 500;

        futures::stream::iter(contexts.chunks(batch_size))
            .for_each_concurrent(Some((concurrency + batch_size - 1) / batch_size), |batch| {
                let namespace_prefix = Arc::clone(&namespace_prefix);
                async move {
                let mut keys = Vec::with_capacity(batch.len());
                // Store references to batch items to map back results
                let mut batch_items = Vec::with_capacity(batch.len());

                for (account, platform) in batch {
                    let key = if let Some(prefix) = namespace_prefix.as_ref() {
                        format!("{prefix}:cron:{}:{}:{}:{}", module_name, account, platform, timestamp)
                    } else {
                        format!("cron:{}:{}:{}:{}", module_name, account, platform, timestamp)
                    };
                    keys.push(key);
                    batch_items.push((account, platform));
                }

                let key_refs: Vec<&str> = keys.iter().map(|s| s.as_str()).collect();

                let lock_start = std::time::Instant::now();
                // TTL = 600 seconds (10 mins)
                match self
                    .state
                    .cache_service
                    .set_nx_batch(&key_refs, b"1", Some(Duration::from_secs(600)))
                    .await
                {
                    Ok(results) => {
                        let lock_duration = lock_start.elapsed().as_secs_f64();
                        histogram!("scheduler_lock_acquisition_seconds").record(lock_duration);
                        for (i, success) in results.into_iter().enumerate() {
                            if success {
                                let (account, platform) = batch_items[i];
                                info!(
                                    "Triggering cron task for module: {} [{}@{}] at {}",
                                    module_name, account, platform, current_minute
                                );
                                
                                self.trigger_single_task(module_name, account, platform).await;
                            }
                        }
                    }
                    Err(e) => {
                        error!("Failed to acquire batch locks for cron task: {}", e);
                    }
                }
            }
            })
            .await;
    }
    
    fn is_schedule_match(schedule: &Schedule, target: DateTime<Utc>) -> bool {
        // Check if `target` is in the schedule.
        // We look for the next occurrence AFTER `target - 1 sec`.
        // If it equals `target`, it's a match.
        let check_time = target - chrono::Duration::seconds(1);
        if let Some(next) = schedule.after(&check_time).next() {
            return next == target;
        }
        false
    }

    async fn trigger_single_task(&self, module_name: &str, account: &str, platform: &str) {
        counter!("scheduled_tasks_total", "module" => module_name.to_string()).increment(1);
        let task = TaskModel {
            account: account.to_string(),
            platform: platform.to_string(),
            module: Some(vec![module_name.to_string()]),
            run_id: uuid::Uuid::now_v7(),
            priority: common::model::Priority::Normal,
        };
        
        // Push to queue
        let sender = self.queue_manager.get_task_push_channel();
        if let Err(e) = sender.send(QueuedItem::new(task)).await {
             error!("Failed to push cron task to queue for module {} [{}@{}]: {}", module_name, account, platform, e);
        }
    }

    async fn fetch_all_enabled_contexts(
        &self,
    ) -> Result<Vec<(String, String, String)>, sea_orm::DbErr> {
        // Optimized Single Query for ALL enabled contexts
        let results: Vec<(String, String, String)> = module::Entity::find()
            .join(JoinType::InnerJoin, module::Relation::RelModuleAccount.def())
            .join(JoinType::InnerJoin, rel_module_account::Relation::Account.def())
            .join(JoinType::InnerJoin, account::Relation::RelAccountPlatform.def())
            .join(JoinType::InnerJoin, rel_account_platform::Relation::Platform.def())
            .join(JoinType::InnerJoin, platform::Relation::RelModulePlatform.def())
            .filter(
                Expr::col((rel_module_platform::Entity, rel_module_platform::Column::ModuleId))
                    .eq(Expr::col((module::Entity, module::Column::Id)))
            )
            .filter(rel_module_account::Column::Enabled.eq(true))
            .filter(rel_account_platform::Column::Enabled.eq(true))
            .filter(rel_module_platform::Column::Enabled.eq(true))
            .filter(module::Column::Enabled.eq(true))
            .select_only()
            .column(module::Column::Name)
            .column(account::Column::Name)
            .column(platform::Column::Name)
            .into_tuple()
            .all(&*self.state.db)
            .await?;

        Ok(results)
    }

    fn staleness_exceeded(now_ms: u64, last_refresh_at_ms: u64, max_staleness_secs: u64) -> bool {
        if last_refresh_at_ms == 0 {
            return false;
        }
        now_ms.saturating_sub(last_refresh_at_ms) > max_staleness_secs.saturating_mul(1000)
    }
}

#[cfg(test)]
mod staleness_tests {
    use super::CronScheduler;

    #[test]
    fn test_staleness_exceeded() {
        let now_ms = 10_000;
        assert!(!CronScheduler::staleness_exceeded(now_ms, 0, 120));
        assert!(!CronScheduler::staleness_exceeded(now_ms, 9_500, 1));
        assert!(CronScheduler::staleness_exceeded(now_ms, 8_000, 1));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;
    use chrono::TimeZone;

    #[test]
    fn test_is_schedule_match() {
        // Every minute
        let schedule = Schedule::from_str("* * * * * *").unwrap();
        
        let target = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
        assert!(CronScheduler::is_schedule_match(&schedule, target));

        // 1 second later (should not match as cron is per second resolution in this crate usually, but logic checks exact match)
        // logic: target - 1s. next after that. should be target.
        // If target is 00:00:01. target-1 = 00:00:00. next after 00:00:00 is 00:00:01. Match.
        let target_sec = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 1).unwrap();
        assert!(CronScheduler::is_schedule_match(&schedule, target_sec));
    }

    #[test]
    fn test_specific_schedule_match() {
        // At 05 minutes past the hour
        let schedule = Schedule::from_str("0 5 * * * *").unwrap();
        
        let match_time = Utc.with_ymd_and_hms(2024, 1, 1, 10, 5, 0).unwrap();
        assert!(CronScheduler::is_schedule_match(&schedule, match_time));

        let no_match_time = Utc.with_ymd_and_hms(2024, 1, 1, 10, 6, 0).unwrap();
        assert!(!CronScheduler::is_schedule_match(&schedule, no_match_time));
    }
}

use std::sync::Arc;
use tokio::time::{sleep, Duration};
use chrono::{Utc, DateTime, TimeZone, Timelike};
use log::{info, error, debug, warn};
use sea_orm::{EntityTrait, QueryFilter, ColumnTrait, QuerySelect, RelationTrait, JoinType, QueryOrder};
use common::interface::ModuleTrait;
use common::model::message::{TaskModel, TopicType};
use common::model::entity::{rel_module_account, account, module};
use common::state::State;
use common::model::entity::prelude::*;
use queue::QueueManager;
use crate::task::TaskManager;
use cron::Schedule;

pub struct CronScheduler {
    task_manager: Arc<TaskManager>,
    state: Arc<State>,
    queue_manager: Arc<QueueManager>,
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
            // Align to minute start
            if let Some(current_minute) = Utc.timestamp_opt(now.timestamp() / 60 * 60, 0).single() {
                 self.process_tick(current_minute).await;
            }
            
            // Wait until next tick check. 
            sleep(Duration::from_secs(10)).await;
        }
    }
    
    async fn process_tick(&self, current_minute: DateTime<Utc>) {
        let modules = self.task_manager.get_all_modules().await;
        for module in modules {
            if let Some(cron_config) = module.cron() {
                 if self.is_schedule_match(&cron_config.schedule, current_minute) {
                     // Try lock
                     let lock_key = format!("cron:{}:{}", module.name(), current_minute.timestamp());
                     // TTL = 65 seconds
                     match self.state.cache_service.set_nx(&lock_key, b"1", Some(Duration::from_secs(65))).await {
                        Ok(true) => {
                             info!("Triggering cron task for module: {} at {}", module.name(), current_minute);
                             self.trigger_module(module, current_minute).await;
                        }
                        Ok(false) => {
                             debug!("Cron task for module {} at {} already running or locked", module.name(), current_minute);
                        }
                        Err(e) => {
                             error!("Failed to acquire lock for cron task: {}", e);
                        }
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
         let mut tasks = Vec::new();
         if target_module.should_login() {
             // Fetch accounts
             match self.fetch_accounts(target_module.name()).await {
                 Ok(accounts) => {
                     if accounts.is_empty() {
                         warn!("Module {} requires login but no accounts found", target_module.name());
                     }
                     for acc in accounts {
                         tasks.push(TaskModel {
                             account: acc,
                             platform: "".to_string(), 
                             module: Some(vec![target_module.name()]),
                             run_id: uuid::Uuid::now_v7(),
                         });
                     }
                 }
                 Err(e) => {
                     error!("Failed to fetch accounts for module {}: {}", target_module.name(), e);
                 }
             }
         } else {
             tasks.push(TaskModel {
                     account: "".to_string(),
                     platform: "".to_string(),
                     module: Some(vec![target_module.name()]),
                     run_id: uuid::Uuid::now_v7(),
             });
         }
         
         if tasks.is_empty() {
             return;
         }

         let namespace = {
              let cfg = self.state.config.read().await;
              cfg.name.clone()
         };
         let topic = TopicType::Task.get_name(&namespace);
         
         for task in tasks {
              match serde_json::to_vec(&task) {
                  Ok(payload) => {
                      if let Some(backend) = &self.queue_manager.backend {
                           if let Err(e) = backend.publish(&topic, &payload).await {
                               error!("Failed to publish cron task for module {}: {}", target_module.name(), e);
                           } else {
                               debug!("Published cron task for module {}", target_module.name());
                           }
                      } else {
                           warn!("No queue backend available for cron task");
                      }
                  }
                  Err(e) => error!("Failed to serialize task: {}", e),
              }
         }
    }
    
    async fn fetch_accounts(&self, module_name: String) -> Result<Vec<String>, sea_orm::DbErr> {
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

         // 2. Get Account IDs linked to module where enabled=true
         let relations = rel_module_account::Entity::find()
             .filter(rel_module_account::Column::ModuleId.eq(module_id))
             .filter(rel_module_account::Column::Enabled.eq(true))
             .all(&*self.state.db)
             .await?;
             
         if relations.is_empty() {
             return Ok(vec![]);
         }
         
         let account_ids: Vec<i32> = relations.into_iter().map(|r| r.account_id).collect();

         // 3. Get Account Names
         let accounts = account::Entity::find()
             .filter(account::Column::Id.is_in(account_ids))
             .all(&*self.state.db)
             .await?;
             
         Ok(accounts.into_iter().map(|a| a.name).collect())
    }
}

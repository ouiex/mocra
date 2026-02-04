use crate::events::{SystemEvent, EventTask};
use sea_orm::{DatabaseConnection, EntityTrait, Set, ActiveValue::NotSet};
use std::sync::Arc;
use tokio::sync::mpsc::Receiver;
use utils::batch_buffer::BatchBuffer;
use common::model::entity::log::{ActiveModel as LogActiveModel, Entity as LogEntity};
use common::model::entity::task_result::{ActiveModel as TaskResultActiveModel, Entity as TaskResultEntity};
use std::time::Duration;
use chrono::{DateTime, Utc};

pub struct DbEventHandler {
    log_buffer: Arc<BatchBuffer<LogActiveModel>>,
    task_result_buffer: Arc<BatchBuffer<TaskResultActiveModel>>,
}

impl DbEventHandler {
    pub fn new(db: Arc<DatabaseConnection>) -> Self {
        let db_log = db.clone();
        let log_buffer = BatchBuffer::new(
            10000,
            100,
            Duration::from_secs(5),
            move |logs| {
                let db = db_log.clone();
                async move {
                    if logs.is_empty() { return; }
                    match LogEntity::insert_many(logs).exec(&*db).await {
                        Ok(_) => {},
                        Err(e) => log::error!("Failed to batch insert logs: {}", e),
                    }
                }
            }
        );

        let db_task = db.clone();
        let task_result_buffer = BatchBuffer::new(
            1000,
            50,
            Duration::from_secs(5),
            move |results| {
                let db = db_task.clone();
                async move {
                    if results.is_empty() { return; }
                    match TaskResultEntity::insert_many(results).exec(&*db).await {
                         Ok(_) => {},
                         Err(e) => log::error!("Failed to batch insert task results: {}", e),
                    }
                }
            }
        );

        Self {
            log_buffer: Arc::new(log_buffer),
            task_result_buffer: Arc::new(task_result_buffer),
        }
    }

    pub async fn start(self, mut rx: Receiver<SystemEvent>) {
        tokio::spawn(async move {
            while let Some(event) = rx.recv().await {
                 let timestamp =  DateTime::from_timestamp_millis(event.timestamp() as i64).unwrap_or(Utc::now());
        
                let (task_id, request_id, status, level, message) = match &event {
                    SystemEvent::Task(e) => {
                        match e {
                            EventTask::TaskReceived(t) => (format!("{}-{}", t.account, t.platform), None, "Received".to_string(), "INFO".to_string(), format!("Task received: {:?}", t)),
                            EventTask::TaskStarted(t) => (format!("{}-{}", t.account, t.platform), None, "Started".to_string(), "INFO".to_string(), format!("Task started: {:?}", t)),
                            EventTask::TaskCompleted(t) => (format!("{}-{}", t.account, t.platform), None, "Completed".to_string(), "INFO".to_string(), format!("Task completed: {:?}", t)),
                            EventTask::TaskFailed(t) => (format!("{}-{}", t.data.account, t.data.platform), None, "Failed".to_string(), "ERROR".to_string(), format!("Task failed: {}", t.error)),
                            EventTask::TaskRetry(t) => (format!("{}-{}", t.data.account, t.data.platform), None, "Retry".to_string(), "WARN".to_string(), format!("Task retry: {}", t.reason)),
                        }
                    },
                    SystemEvent::Request(e) => {
                        match e {
                            crate::events::EventRequest::RequestReceived(r) => (format!("{}-{}", r.account, r.platform), Some(r.request_id), "Received".to_string(), "INFO".to_string(), format!("Request received: {}", r.url)),
                            crate::events::EventRequest::RequestStarted(r) => (format!("{}-{}", r.account, r.platform), Some(r.request_id), "Started".to_string(), "INFO".to_string(), format!("Request started: {}", r.url)),
                            crate::events::EventRequest::RequestCompleted(r) => (format!("{}-{}", r.account, r.platform), Some(r.request_id), "Completed".to_string(), "INFO".to_string(), format!("Request completed: {}", r.url)),
                            crate::events::EventRequest::RequestFailed(r) => (format!("{}-{}", r.data.account, r.data.platform), Some(r.data.request_id), "Failed".to_string(), "ERROR".to_string(), format!("Request failed: {}", r.error)),
                            crate::events::EventRequest::RequestRetry(r) => (format!("{}-{}", r.data.account, r.data.platform), Some(r.data.request_id), "Retry".to_string(), "WARN".to_string(), format!("Request retry: {}", r.reason)),
                        }
                    }
                    _ => continue,
                };
                
                let log_entry = LogActiveModel {
                    id: NotSet,
                    task_id: Set(task_id),
                    request_id: Set(request_id),
                    status: Set(status),
                    level: Set(level),
                    message: Set(message),
                    timestamp: Set(timestamp.naive_utc()),
                    traceback: Set(None),
                };
                
                let _ = self.log_buffer.add(log_entry).await;

                // Handle Task Result
                if let SystemEvent::Task(task_event) = event {
                     match task_event {
                         EventTask::TaskCompleted(e) => {
                             let result = TaskResultActiveModel {
                                 id: NotSet,
                                 task_id: Set(format!("{}-{}", e.account, e.platform)),
                                 status: Set("Completed".to_string()),
                                 start_time: Set(Utc::now().naive_utc()), // Approximate
                                 end_time: Set(Some(Utc::now().naive_utc())),
                                 result: Set(Some("Success".to_string())),
                                 error: Set(None),
                                 updated_at: Set(Utc::now().naive_utc()),
                             };
                             let _ = self.task_result_buffer.add(result).await;
                         },
                         EventTask::TaskFailed(e) => {
                                let result = TaskResultActiveModel {
                                    id: NotSet,
                                    task_id: Set(format!("{}-{}", e.data.account, e.data.platform)),
                                    status: Set("Failed".to_string()),
                                    start_time: Set(Utc::now().naive_utc()),
                                    end_time: Set(Some(Utc::now().naive_utc())),
                                    result: Set(None),
                                    error: Set(Some(e.error.clone())),
                                    updated_at: Set(Utc::now().naive_utc()),
                                };
                                let _ = self.task_result_buffer.add(result).await;
                        },
                        _ => {}
                    }
                }
            }
        });
    }
}

// #[async_trait]
// impl EventHandler for DbEventHandler {
//    async fn handle(&self, event: &SystemEvent) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
//        let timestamp =  DateTime::from_timestamp_millis(event.timestamp() as i64).unwrap_or(Utc::now());
//        
//        let (task_id, request_id, status, level, message) = match event {
//            SystemEvent::Task(e) => {
//                 match e {
//                     EventTask::TaskReceived(t) => (format!("{}-{}", t.account, t.platform), None, "Received".to_string(), "INFO".to_string(), format!("Task received: {:?}", t)),
//                     EventTask::TaskStarted(t) => (format!("{}-{}", t.account, t.platform), None, "Started".to_string(), "INFO".to_string(), format!("Task started: {:?}", t)),
//                     EventTask::TaskCompleted(t) => (format!("{}-{}", t.account, t.platform), None, "Completed".to_string(), "INFO".to_string(), format!("Task completed: {:?}", t)),
//                     EventTask::TaskFailed(t) => (format!("{}-{}", t.data.account, t.data.platform), None, "Failed".to_string(), "ERROR".to_string(), format!("Task failed: {}", t.error)),
//                     EventTask::TaskRetry(t) => (format!("{}-{}", t.data.account, t.data.platform), None, "Retry".to_string(), "WARN".to_string(), format!("Task retry: {}", t.reason)),
//                 }
//            },
//            SystemEvent::Request(e) => {
//                match e {
//                     crate::events::EventRequest::RequestReceived(r) => (format!("{}-{}", r.account, r.platform), Some(r.request_id), "Received".to_string(), "INFO".to_string(), format!("Request received: {}", r.url)),
//                     crate::events::EventRequest::RequestStarted(r) => (format!("{}-{}", r.account, r.platform), Some(r.request_id), "Started".to_string(), "INFO".to_string(), format!("Request started: {}", r.url)),
//                     crate::events::EventRequest::RequestCompleted(r) => (format!("{}-{}", r.account, r.platform), Some(r.request_id), "Completed".to_string(), "INFO".to_string(), format!("Request completed: {}", r.url)),
//                     crate::events::EventRequest::RequestFailed(r) => (format!("{}-{}", r.data.account, r.data.platform), Some(r.data.request_id), "Failed".to_string(), "ERROR".to_string(), format!("Request failed: {}", r.error)),
//                     crate::events::EventRequest::RequestRetry(r) => (format!("{}-{}", r.data.account, r.data.platform), Some(r.data.request_id), "Retry".to_string(), "WARN".to_string(), format!("Request retry: {}", r.reason)),
//                }
//            },
//            _ => ("system".to_string(), None, "info".to_string(), "INFO".to_string(), format!("{:?}", event)),
//        };
//
//        let log_entry = LogActiveModel {
//            id: NotSet,
//            task_id: Set(task_id.clone()),
//            request_id: Set(request_id),
//            status: Set(status.clone()),
//            level: Set(level),
//            message: Set(message),
//            timestamp: Set(timestamp.naive_utc()),
//            traceback: Set(None),
//        };
//        
//        let _ = self.log_buffer.add(log_entry).await;
//
//        // Handle Task Result
//        if let SystemEvent::Task(task_event) = event {
//             match task_event {
//                 EventTask::TaskCompleted(e) => {
//                     let result = TaskResultActiveModel {
//                         id: NotSet,
//                         task_id: Set(format!("{}-{}", e.account, e.platform)),
//                         status: Set("Completed".to_string()),
//                         start_time: Set(Utc::now().naive_utc()), // Approximate
//                         end_time: Set(Some(Utc::now().naive_utc())),
//                         result: Set(Some("Success".to_string())),
//                         error: Set(None),
//                         updated_at: Set(Utc::now().naive_utc()),
//                     };
//                     let _ = self.task_result_buffer.add(result).await;
//                 },
//                 EventTask::TaskFailed(e) => {
//                     let result = TaskResultActiveModel {
//                         id: NotSet,
//                         task_id: Set(format!("{}-{}", e.data.account, e.data.platform)),
//                         status: Set("Failed".to_string()),
//                         start_time: Set(Utc::now().naive_utc()), 
//                         end_time: Set(Some(Utc::now().naive_utc())),
//                         result: Set(None),
//                         error: Set(Some(e.error.clone())),
//                         updated_at: Set(Utc::now().naive_utc()),
//                     };
//                     let _ = self.task_result_buffer.add(result).await;
//                 },
//                 _ => {}
//             }
//        }
//
//        Ok(())
//    }
//    
//    fn name(&self) -> &'static str {
//        "DbEventHandler"
//    }
//}

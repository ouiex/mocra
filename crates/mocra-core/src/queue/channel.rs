use crate::common::model::message::TaskEvent;
use crate::common::model::message::{TaskErrorEvent, TaskParserEvent};
use crate::common::model::{Request, Response};
use crate::queue::QueuedItem;
use crate::utils::logger::LogModel;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::sync::mpsc::{Receiver, Sender, channel};

/// Message queue channel manager.
///
/// Data flow:
/// 1. Task Flow: watch TaskModel -> produce Request
/// 2. Request Flow: watch Request -> download -> produce Response
/// 3. Response Flow: watch Response -> parse -> finish, or produce a new Request
pub struct Channel {
    // --- 1. Initial task queue (Task Queue) ---
    // Producer: external API / seed generator
    // Consumer: task processor (TaskProcessor) -> produces Request
    pub task_sender: Sender<QueuedItem<TaskEvent>>,
    pub task_receiver: Arc<Mutex<Receiver<QueuedItem<TaskEvent>>>>,

    // --- 2. Request queue (Request Queue) ---
    // Producer: task processor / response processor (parse results)
    // Consumer: downloader (Downloader) -> performs the download -> produces Response
    pub request_sender: Sender<QueuedItem<Request>>,
    pub request_receiver: Arc<Mutex<Receiver<QueuedItem<Request>>>>,

    // (Optional) download-only queue, distinguishing requests before and after scheduling
    pub download_request_sender: Sender<QueuedItem<Request>>,
    pub download_request_receiver: Arc<Mutex<Receiver<QueuedItem<Request>>>>,

    // --- 3. Response queue (Response Queue) ---
    // Producer: downloader (Downloader)
    // Consumer: response processor (ResponseProcessor) -> parses data -> (store / produce a new
    // Request)
    pub response_sender: Sender<QueuedItem<Response>>,
    pub response_receiver: Arc<Mutex<Receiver<QueuedItem<Response>>>>,

    // --- 4. Auxiliary Queues ---

    // Error handling
    pub error_sender: Sender<QueuedItem<TaskErrorEvent>>,
    pub error_receiver: Arc<Mutex<Receiver<QueuedItem<TaskErrorEvent>>>>,

    // Log handling
    pub log_sender: Sender<QueuedItem<LogModel>>,
    pub log_receiver: Arc<Mutex<Receiver<QueuedItem<LogModel>>>>,

    // --- 5. Other / extension queues (Others) ---

    // Parser task queue (when parsing is decoupled from response handling)
    pub parser_task_sender: Sender<QueuedItem<TaskParserEvent>>,
    pub parser_task_receiver: Arc<Mutex<Receiver<QueuedItem<TaskParserEvent>>>>,

    // Remote / distributed node communication queues
    pub remote_response_sender: Sender<QueuedItem<Response>>,
    pub remote_response_receiver: Arc<Mutex<Receiver<QueuedItem<Response>>>>,

    pub remote_parser_task_sender: Sender<QueuedItem<TaskParserEvent>>,
    pub remote_parser_task_receiver: Arc<Mutex<Receiver<QueuedItem<TaskParserEvent>>>>,

    pub remote_error_sender: Sender<QueuedItem<TaskErrorEvent>>,
    pub remote_error_receiver: Arc<Mutex<Receiver<QueuedItem<TaskErrorEvent>>>>,

    // Distributed Task Queue (Outbound)
    pub remote_task_sender: Sender<QueuedItem<TaskEvent>>,
    pub remote_task_receiver: Arc<Mutex<Receiver<QueuedItem<TaskEvent>>>>,
}
impl Clone for Channel {
    fn clone(&self) -> Self {
        Channel {
            task_sender: self.task_sender.clone(),
            task_receiver: self.task_receiver.clone(),

            request_sender: self.request_sender.clone(),
            request_receiver: self.request_receiver.clone(),

            download_request_sender: self.download_request_sender.clone(),
            download_request_receiver: self.download_request_receiver.clone(),

            response_sender: self.response_sender.clone(),
            response_receiver: self.response_receiver.clone(),

            error_sender: self.error_sender.clone(),
            error_receiver: self.error_receiver.clone(),

            log_sender: self.log_sender.clone(),
            log_receiver: self.log_receiver.clone(),

            parser_task_sender: self.parser_task_sender.clone(),
            parser_task_receiver: self.parser_task_receiver.clone(),

            remote_response_sender: self.remote_response_sender.clone(),
            remote_response_receiver: self.remote_response_receiver.clone(),

            remote_parser_task_sender: self.remote_parser_task_sender.clone(),
            remote_parser_task_receiver: self.remote_parser_task_receiver.clone(),

            remote_error_sender: self.remote_error_sender.clone(),
            remote_error_receiver: self.remote_error_receiver.clone(),

            remote_task_sender: self.remote_task_sender.clone(),
            remote_task_receiver: self.remote_task_receiver.clone(),
        }
    }
}

impl Channel {
    pub fn new(capacity: usize) -> Self {
        // Optimization: Use a much larger buffer in Single Node Mode to reduce backpressure.
        // If "capacity" passed is 1000, it's too small for high-throughput single node.
        // We override or multiply it here locally.
        let effective_capacity = if capacity < 10000 { 10000 } else { capacity };

        let (task_sender, task_receiver) = channel(effective_capacity);
        let (request_sender, request_receiver) = channel(effective_capacity);
        let (download_request_sender, download_request_receiver) = channel(effective_capacity);
        let (response_sender, response_receiver) = channel(effective_capacity);
        let (error_sender, error_receiver) = channel(effective_capacity);
        let (log_sender, log_receiver) = channel(10000); // Increased log channel

        let (parser_task_sender, parser_task_receiver) = channel(effective_capacity);

        // Remote channels
        let (remote_response_sender, remote_response_receiver) = channel(effective_capacity);
        let (remote_parser_task_sender, remote_parser_task_receiver) = channel(effective_capacity);
        let (remote_error_sender, remote_error_receiver) = channel(effective_capacity);
        let (remote_task_sender, remote_task_receiver) = channel(effective_capacity);

        Channel {
            task_sender,
            task_receiver: Arc::new(Mutex::new(task_receiver)),

            request_sender,
            request_receiver: Arc::new(Mutex::new(request_receiver)),

            download_request_sender,
            download_request_receiver: Arc::new(Mutex::new(download_request_receiver)),

            response_sender,
            response_receiver: Arc::new(Mutex::new(response_receiver)),

            error_sender,
            error_receiver: Arc::new(Mutex::new(error_receiver)),

            log_sender,
            log_receiver: Arc::new(Mutex::new(log_receiver)),

            parser_task_sender,
            parser_task_receiver: Arc::new(Mutex::new(parser_task_receiver)),

            remote_response_sender,
            remote_response_receiver: Arc::new(Mutex::new(remote_response_receiver)),

            remote_parser_task_sender,
            remote_parser_task_receiver: Arc::new(Mutex::new(remote_parser_task_receiver)),

            remote_error_sender,
            remote_error_receiver: Arc::new(Mutex::new(remote_error_receiver)),

            remote_task_sender,
            remote_task_receiver: Arc::new(Mutex::new(remote_task_receiver)),
        }
    }
}

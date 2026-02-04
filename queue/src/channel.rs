use crate::QueuedItem;
use common::model::message::TaskModel;
use common::model::message::{ErrorTaskModel, ParserTaskModel};
use common::model::{Request, Response};
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::sync::mpsc::{Receiver, Sender, channel};
use utils::logger::LogModel;

/// 消息队列通道管理器
///
/// 数据流转流程：
/// 1. Task Flow: 监控 TaskModel -> 生成 Request
/// 2. Request Flow: 监控 Request -> 下载 -> 生成 Response
/// 3. Response Flow: 监控 Response -> 解析 -> 结束 或 生成新 Request
pub struct Channel {
    // --- 1. 初始任务队列 (Task Queue) ---
    // 生产者: 外部API / 种子生成器
    // 消费者: 任务处理器 (TaskProcessor) -> 生成 Request
    pub task_sender: Sender<QueuedItem<TaskModel>>,
    pub task_receiver: Arc<Mutex<Receiver<QueuedItem<TaskModel>>>>,

    // --- 2. 请求队列 (Request Queue) ---
    // 生产者: 任务处理器 / 响应处理器 (解析结果)
    // 消费者: 下载器 (Downloader) -> 执行下载 -> 生成 Response
    pub request_sender: Sender<QueuedItem<Request>>,
    pub request_receiver: Arc<Mutex<Receiver<QueuedItem<Request>>>>,

    // (可选) 下载专用队列，用于区分调度前后的请求
    pub download_request_sender: Sender<QueuedItem<Request>>,
    pub download_request_receiver: Arc<Mutex<Receiver<QueuedItem<Request>>>>,

    // --- 3. 响应队列 (Response Queue) ---
    // 生产者: 下载器 (Downloader)
    // 消费者: 响应处理器 (ResponseProcessor) -> 解析数据 -> (存储 / 生成新 Request)
    pub response_sender: Sender<QueuedItem<Response>>,
    pub response_receiver: Arc<Mutex<Receiver<QueuedItem<Response>>>>,

    // --- 4. 辅助队列 (Auxiliary Queues) ---

    // 错误处理
    pub error_sender: Sender<QueuedItem<ErrorTaskModel>>,
    pub error_receiver: Arc<Mutex<Receiver<QueuedItem<ErrorTaskModel>>>>,

    // 日志处理
    pub log_sender: Sender<QueuedItem<LogModel>>,
    pub log_receiver: Arc<Mutex<Receiver<QueuedItem<LogModel>>>>,

    // --- 5. 其他/扩展队列 (Others) ---

    // 解析任务队列 (如果解析独立于响应处理)
    pub parser_task_sender: Sender<QueuedItem<ParserTaskModel>>,
    pub parser_task_receiver: Arc<Mutex<Receiver<QueuedItem<ParserTaskModel>>>>,

    // 远程/分布式节点通信队列
    pub remote_response_sender: Sender<QueuedItem<Response>>,
    pub remote_response_receiver: Arc<Mutex<Receiver<QueuedItem<Response>>>>,

    pub remote_parser_task_sender: Sender<QueuedItem<ParserTaskModel>>,
    pub remote_parser_task_receiver: Arc<Mutex<Receiver<QueuedItem<ParserTaskModel>>>>,

    pub remote_error_sender: Sender<QueuedItem<ErrorTaskModel>>,
    pub remote_error_receiver: Arc<Mutex<Receiver<QueuedItem<ErrorTaskModel>>>>,

    // Distributed Task Queue (Outbound)
    pub remote_task_sender: Sender<QueuedItem<TaskModel>>,
    pub remote_task_receiver: Arc<Mutex<Receiver<QueuedItem<TaskModel>>>>,
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

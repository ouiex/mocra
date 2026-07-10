//! NATS(JetStream)数据面队列后端(`queue-nats` 特性)。
//!
//! 用 JetStream 提供**持久化 + explicit ack + nack 重投/DLQ**,语义对齐 Kafka 后端;
//! 比 Kafka 轻得多(单二进制、无 ZooKeeper/cmake),适合内嵌 Raft 集群的数据面 —— 无需 Redis。
//!
//! - 一个 stream(`{ns}_stream`,subjects `{ns}.>`)承载所有 topic;
//! - 每个 topic 一个 **durable pull consumer**(同名 durable = 竞争消费 / 负载均衡);
//! - nack 按 [`NackPolicy`] 决定:重发原 subject(`attempt+1`)或投 DLQ subject。
//!
//! > 账号亲和(`hash(account)`)在 NATS 下暂为竞争消费(负载均衡,不粘账号);后续可用
//! > 「按 `owns_partition_key` 订阅分区 subject」实现(见路线图数据面亲和项)。

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use dashmap::DashMap;
use futures::StreamExt;
use log::{error, info, warn};
use tokio::sync::mpsc;

use crate::common::model::config::NatsConfig;
use crate::errors::Result;
use crate::errors::error::QueueError;
use crate::queue::{
    AckAction, HEADER_ATTEMPT, HEADER_NACK_REASON, Message, MqBackend, NackDisposition, NackPolicy,
    decide_nack, parse_attempt,
};

fn nats_err<E: std::fmt::Display>(e: E) -> crate::errors::Error {
    QueueError::OperationFailed(Box::new(std::io::Error::other(e.to_string()))).into()
}

/// NATS subject / stream 名 token 保护:非法字符替换为 `_`。
fn sanitize(s: &str) -> String {
    s.chars()
        .map(|c| match c {
            '.' | '*' | '>' | ' ' | '\t' => '_',
            c => c,
        })
        .collect()
}

fn to_header_map(headers: &HashMap<String, String>) -> async_nats::HeaderMap {
    let mut h = async_nats::HeaderMap::new();
    for (k, v) in headers {
        h.insert(k.as_str(), v.as_str());
    }
    h
}

/// JetStream 数据面后端。连接惰性建立(首次 publish/subscribe 时),适配同步构造。
pub struct NatsQueue {
    config: NatsConfig,
    namespace: String,
    stream_name: String,
    minid_time: u64,
    nack_policy: NackPolicy,
    ctx: Arc<tokio::sync::OnceCell<async_nats::jetstream::Context>>,
    stream_ready: Arc<tokio::sync::OnceCell<()>>,
}

impl NatsQueue {
    pub fn new(
        config: &NatsConfig,
        minid_time: u64,
        namespace: &str,
        nack_policy: NackPolicy,
    ) -> Result<Self> {
        Ok(Self {
            config: config.clone(),
            namespace: namespace.to_string(),
            stream_name: format!("{}_stream", sanitize(namespace)),
            minid_time,
            nack_policy,
            ctx: Arc::new(tokio::sync::OnceCell::new()),
            stream_ready: Arc::new(tokio::sync::OnceCell::new()),
        })
    }

    /// 惰性连接 NATS 并返回 JetStream 上下文(只连接一次)。
    async fn context(&self) -> Result<async_nats::jetstream::Context> {
        let ctx = self
            .ctx
            .get_or_try_init(|| async {
                let opts = async_nats::ConnectOptions::new();
                let opts = match (
                    &self.config.username,
                    &self.config.password,
                    &self.config.token,
                ) {
                    (Some(u), Some(p), _) => opts.user_and_password(u.clone(), p.clone()),
                    (_, _, Some(t)) => opts.token(t.clone()),
                    _ => opts,
                };
                let client = opts
                    .connect(self.config.url.clone())
                    .await
                    .map_err(nats_err)?;
                info!("NatsQueue connected to {}", self.config.url);
                Ok::<_, crate::errors::Error>(async_nats::jetstream::new(client))
            })
            .await?;
        Ok(ctx.clone())
    }

    /// 确保承载 stream 存在(只建一次),返回 JetStream 上下文。
    async fn ensure_stream(&self) -> Result<async_nats::jetstream::Context> {
        let js = self.context().await?;
        let js2 = js.clone();
        self.stream_ready
            .get_or_try_init(|| async {
                let mut cfg = async_nats::jetstream::stream::Config {
                    name: self.stream_name.clone(),
                    subjects: vec![format!("{}.>", sanitize(&self.namespace))],
                    ..Default::default()
                };
                if self.minid_time > 0 {
                    cfg.max_age = Duration::from_secs(self.minid_time * 3600);
                }
                js2.get_or_create_stream(cfg).await.map_err(nats_err)?;
                info!("NatsQueue stream `{}` ready", self.stream_name);
                Ok::<_, crate::errors::Error>(())
            })
            .await?;
        Ok(js)
    }

    fn subject(&self, topic: &str) -> String {
        format!("{}.{}", sanitize(&self.namespace), sanitize(topic))
    }
    fn dlq_subject(&self, topic: &str) -> String {
        format!("{}._dlq.{}", sanitize(&self.namespace), sanitize(topic))
    }
    fn durable(&self, topic: &str) -> String {
        format!("{}_{}_c", sanitize(&self.namespace), sanitize(topic)).replace('-', "_")
    }
}

#[async_trait]
impl MqBackend for NatsQueue {
    async fn publish_with_headers(
        &self,
        topic: &str,
        _key: Option<&str>,
        payload: &[u8],
        headers: &HashMap<String, String>,
    ) -> Result<()> {
        let js = self.ensure_stream().await?;
        let subject = self.subject(topic);
        let h = to_header_map(headers);
        let ack = js
            .publish_with_headers(subject, h, payload.to_vec().into())
            .await
            .map_err(nats_err)?;
        // 等 server 持久化确认(至少一次)。
        ack.await.map_err(nats_err)?;
        Ok(())
    }

    async fn subscribe(&self, topic: &str, sender: mpsc::Sender<Message>) -> Result<()> {
        let js = self.ensure_stream().await?;
        let stream_name = self.stream_name.clone();
        let subject = self.subject(topic);
        let dlq_subject = self.dlq_subject(topic);
        let durable = self.durable(topic);
        let nack_policy = self.nack_policy;
        let topic_log = topic.to_string();

        tokio::spawn(async move {
            let stream = match js.get_stream(&stream_name).await {
                Ok(s) => s,
                Err(e) => {
                    error!("NatsQueue get_stream failed: {e}");
                    return;
                }
            };
            let consumer = match stream
                .get_or_create_consumer(
                    &durable,
                    async_nats::jetstream::consumer::pull::Config {
                        durable_name: Some(durable.clone()),
                        filter_subject: subject.clone(),
                        ack_policy: async_nats::jetstream::consumer::AckPolicy::Explicit,
                        ..Default::default()
                    },
                )
                .await
            {
                Ok(c) => c,
                Err(e) => {
                    error!("NatsQueue create consumer failed for {topic_log}: {e}");
                    return;
                }
            };
            let mut messages = match consumer.messages().await {
                Ok(m) => m,
                Err(e) => {
                    error!("NatsQueue messages() failed for {topic_log}: {e}");
                    return;
                }
            };
            info!("NatsQueue listening topic {topic_log} (subject {subject})");

            // id(stream sequence)-> owned jetstream 消息,供 ack processor 取出确认。
            let inflight: Arc<DashMap<String, async_nats::jetstream::Message>> =
                Arc::new(DashMap::new());
            let (ack_tx, mut ack_rx) = mpsc::channel::<(String, AckAction)>(1000);

            // ack processor:Ack → 确认;Nack → 按策略重发原 subject 或投 DLQ,再确认原消息。
            let inflight_ack = inflight.clone();
            let js_ack = js.clone();
            let subj_retry = subject.clone();
            tokio::spawn(async move {
                while let Some((id, action)) = ack_rx.recv().await {
                    let Some((_, msg)) = inflight_ack.remove(&id) else {
                        continue;
                    };
                    match action {
                        AckAction::Ack => {
                            let _ = msg.ack().await;
                        }
                        AckAction::Nack(reason, payload, headers) => {
                            let attempt = parse_attempt(&headers);
                            match decide_nack(nack_policy, attempt) {
                                NackDisposition::Retry { next_attempt } => {
                                    if nack_policy.backoff_ms > 0 {
                                        tokio::time::sleep(Duration::from_millis(
                                            nack_policy.backoff_ms,
                                        ))
                                        .await;
                                    }
                                    let mut nh = (*headers).clone();
                                    nh.insert(HEADER_ATTEMPT.to_string(), next_attempt.to_string());
                                    nh.insert(HEADER_NACK_REASON.to_string(), reason);
                                    let hm = to_header_map(&nh);
                                    let _ = js_ack
                                        .publish_with_headers(
                                            subj_retry.clone(),
                                            hm,
                                            payload.to_vec().into(),
                                        )
                                        .await;
                                    let _ = msg.ack().await;
                                }
                                NackDisposition::Dlq => {
                                    let mut hm = async_nats::HeaderMap::new();
                                    hm.insert(HEADER_NACK_REASON, reason.as_str());
                                    let _ = js_ack
                                        .publish_with_headers(
                                            dlq_subject.clone(),
                                            hm,
                                            payload.to_vec().into(),
                                        )
                                        .await;
                                    let _ = msg.ack().await;
                                }
                            }
                        }
                    }
                }
            });

            // 主接收循环。
            while let Some(item) = messages.next().await {
                let msg = match item {
                    Ok(m) => m,
                    Err(e) => {
                        warn!("NatsQueue recv error on {topic_log}: {e}");
                        continue;
                    }
                };
                let seq = msg.info().map(|i| i.stream_sequence).unwrap_or(0);
                let id = format!("{seq}");
                let payload = msg.payload.to_vec();
                let mut hdrs = HashMap::new();
                if let Some(h) = &msg.headers {
                    for (name, values) in h.iter() {
                        if let Some(v) = values.first() {
                            hdrs.insert(name.to_string(), v.to_string());
                        }
                    }
                }
                inflight.insert(id.clone(), msg);
                let out = Message {
                    payload: Arc::new(payload),
                    id,
                    headers: Arc::new(hdrs),
                    ack_tx: ack_tx.clone(),
                };
                if sender.send(out).await.is_err() {
                    warn!("NatsQueue subscriber channel closed for {topic_log}");
                    return;
                }
            }
        });

        Ok(())
    }

    async fn clean_storage(&self) -> Result<()> {
        // JetStream 的保留由 stream `max_age` 控制(见 ensure_stream),无需主动清理。
        Ok(())
    }

    async fn send_to_dlq(
        &self,
        topic: &str,
        _id: &str,
        payload: &[u8],
        reason: &str,
    ) -> Result<()> {
        let js = self.ensure_stream().await?;
        let mut h = async_nats::HeaderMap::new();
        h.insert(HEADER_NACK_REASON, reason);
        js.publish_with_headers(self.dlq_subject(topic), h, payload.to_vec().into())
            .await
            .map_err(nats_err)?
            .await
            .map_err(nats_err)?;
        Ok(())
    }

    async fn read_dlq(
        &self,
        _topic: &str,
        _count: usize,
    ) -> Result<Vec<(String, Vec<u8>, String, String)>> {
        // DLQ 消息已写入 `{ns}._dlq.{topic}` subject(可用 nats CLI / 单独消费者读取);
        // 程序化读取待后续实现(与 KafkaQueue 一致)。
        warn!("NatsQueue DLQ inspection not implemented yet");
        Ok(Vec::new())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::model::config::NatsConfig;

    fn cfg() -> NatsConfig {
        NatsConfig {
            url: std::env::var("MOCRA_NATS_URL").unwrap_or_else(|_| "nats://127.0.0.1:4222".into()),
            username: None,
            password: None,
            token: None,
        }
    }

    /// 唯一 namespace(→ 唯一 stream),避免测试间残留冲突。
    fn unique_ns(tag: &str) -> String {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        format!("mt_{tag}_{nanos}")
    }

    #[tokio::test]
    #[ignore = "需要本地 nats-server: docker run -p 4222:4222 nats -js"]
    async fn nats_publish_subscribe_ack_roundtrip() {
        let ns = unique_ns("rt");
        let q = NatsQueue::new(&cfg(), 0, &ns, NackPolicy::default()).unwrap();

        let (tx, mut rx) = mpsc::channel::<Message>(16);
        q.subscribe("task-normal", tx).await.unwrap();
        // 等 durable consumer 就绪。
        tokio::time::sleep(Duration::from_millis(500)).await;

        let mut headers = HashMap::new();
        headers.insert(HEADER_ATTEMPT.to_string(), "0".to_string());
        headers.insert("custom".to_string(), "v1".to_string());
        q.publish_with_headers("task-normal", None, b"hello-nats", &headers)
            .await
            .unwrap();

        let msg = tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .expect("timed out waiting for message")
            .expect("channel closed");
        assert_eq!(msg.payload.as_slice(), b"hello-nats");
        assert_eq!(
            msg.headers.get(HEADER_ATTEMPT).map(String::as_str),
            Some("0")
        );
        assert_eq!(msg.headers.get("custom").map(String::as_str), Some("v1"));
        msg.ack().await.unwrap();
    }

    #[tokio::test]
    #[ignore = "需要本地 nats-server"]
    async fn nats_nack_retries_then_dlq() {
        // max_retries=1:attempt 0 nack → 重投(attempt 1);attempt 1 nack → DLQ(不再重投)。
        let ns = unique_ns("nack");
        let policy = NackPolicy {
            max_retries: 1,
            backoff_ms: 0,
        };
        let q = NatsQueue::new(&cfg(), 0, &ns, policy).unwrap();

        let (tx, mut rx) = mpsc::channel::<Message>(16);
        q.subscribe("task-normal", tx).await.unwrap();
        tokio::time::sleep(Duration::from_millis(500)).await;

        let mut headers = HashMap::new();
        headers.insert(HEADER_ATTEMPT.to_string(), "0".to_string());
        q.publish_with_headers("task-normal", None, b"boom", &headers)
            .await
            .unwrap();

        // 第一次:attempt 0 → nack → 重投为 attempt 1。
        let m1 = tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .expect("timeout m1")
            .expect("closed m1");
        assert_eq!(
            m1.headers.get(HEADER_ATTEMPT).map(String::as_str),
            Some("0")
        );
        m1.nack("boom-fail-1").await.unwrap();

        // 第二次:attempt 1 → nack → 投 DLQ。
        let m2 = tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .expect("timeout m2")
            .expect("closed m2");
        assert_eq!(
            m2.headers.get(HEADER_ATTEMPT).map(String::as_str),
            Some("1")
        );
        m2.nack("boom-fail-2").await.unwrap();

        // 第三次:不应再收到(已进 DLQ subject,不匹配 consumer filter)。
        let m3 = tokio::time::timeout(Duration::from_millis(1500), rx.recv()).await;
        assert!(m3.is_err(), "message should be in DLQ, not redelivered");
    }
}

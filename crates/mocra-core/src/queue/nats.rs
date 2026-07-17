//! NATS (JetStream) data-plane queue backend (the `queue-nats` feature).
//!
//! JetStream provides **persistence + explicit ack + nack redelivery/DLQ**, with semantics
//! matching the Kafka backend; it is far lighter than Kafka (a single binary, no ZooKeeper or
//! cmake), which suits the data plane of an embedded Raft cluster.
//!
//! - A single stream (`{ns}_stream`, subjects `{ns}.>`) carries every topic;
//! - one **durable pull consumer** per topic (a shared durable name = competing consumption /
//!   load balancing);
//! - nack is resolved by [`NackPolicy`]: republish to the original subject (`attempt+1`) or
//!   send to the DLQ subject.
//!
//! > Account affinity (`hash(account)`) is currently plain competing consumption under NATS
//! > (load balancing, not sticky per account); it can later be implemented by "subscribing to
//! > partition subjects via `owns_partition_key`" (see the data-plane affinity item on the
//! > roadmap).

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

/// Guards NATS subject / stream name tokens: illegal characters are replaced with `_`.
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

/// JetStream data-plane backend. The connection is established lazily (on the first
/// publish/subscribe), which accommodates synchronous construction.
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

    /// Lazily connects to NATS and returns the JetStream context (connects only once).
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

    /// Ensures the carrier stream exists (created only once) and returns the JetStream context.
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
        // Wait for the server's persistence acknowledgement (at-least-once).
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

            // id (stream sequence) -> owned jetstream message, for the ack processor to pick up
            // and acknowledge.
            let inflight: Arc<DashMap<String, async_nats::jetstream::Message>> =
                Arc::new(DashMap::new());
            let (ack_tx, mut ack_rx) = mpsc::channel::<(String, AckAction)>(1000);

            // ack processor: Ack → acknowledge; Nack → per policy, republish to the original
            // subject or send to the DLQ, then acknowledge the original message.
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

            // Main receive loop.
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
        // JetStream retention is governed by the stream's `max_age` (see ensure_stream), so no
        // active cleanup is needed.
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
        // DLQ messages are written to the `{ns}._dlq.{topic}` subject (readable via the nats CLI
        // or a separate consumer); programmatic reads are not implemented yet (same as
        // KafkaQueue).
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

    /// A unique namespace (→ a unique stream), avoiding leftover conflicts between tests.
    fn unique_ns(tag: &str) -> String {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        format!("mt_{tag}_{nanos}")
    }

    #[tokio::test]
    #[ignore = "requires a local nats-server: docker run -p 4222:4222 nats -js"]
    async fn nats_publish_subscribe_ack_roundtrip() {
        let ns = unique_ns("rt");
        let q = NatsQueue::new(&cfg(), 0, &ns, NackPolicy::default()).unwrap();

        let (tx, mut rx) = mpsc::channel::<Message>(16);
        q.subscribe("task-normal", tx).await.unwrap();
        // Wait for the durable consumer to be ready.
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
    #[ignore = "requires a local nats-server"]
    async fn nats_nack_retries_then_dlq() {
        // max_retries=1: nack on attempt 0 → redeliver (attempt 1); nack on attempt 1 → DLQ (no
        // further redelivery).
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

        // First delivery: attempt 0 → nack → redelivered as attempt 1.
        let m1 = tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .expect("timeout m1")
            .expect("closed m1");
        assert_eq!(
            m1.headers.get(HEADER_ATTEMPT).map(String::as_str),
            Some("0")
        );
        m1.nack("boom-fail-1").await.unwrap();

        // Second delivery: attempt 1 → nack → sent to the DLQ.
        let m2 = tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .expect("timeout m2")
            .expect("closed m2");
        assert_eq!(
            m2.headers.get(HEADER_ATTEMPT).map(String::as_str),
            Some("1")
        );
        m2.nack("boom-fail-2").await.unwrap();

        // Third delivery: should not arrive (it is on the DLQ subject, which does not match the
        // consumer filter).
        let m3 = tokio::time::timeout(Duration::from_millis(1500), rx.recv()).await;
        assert!(m3.is_err(), "message should be in DLQ, not redelivered");
    }
}

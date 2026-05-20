use crate::common::model::Priority;

pub(crate) const LARGE_PAYLOAD_BYTES: usize = 64 * 1024;
pub(crate) const EXPLICIT_TOPIC_NAMESPACE_DELIMITER: &str = "::";

pub(crate) fn qualify_topic_namespace(namespace: &str, topic: &str) -> String {
    format!(
        "{namespace}{EXPLICIT_TOPIC_NAMESPACE_DELIMITER}{topic}"
    )
}

pub(crate) fn split_explicit_topic_namespace(topic: &str) -> Option<(&str, &str)> {
    let (namespace, route_topic) = topic.split_once(EXPLICIT_TOPIC_NAMESPACE_DELIMITER)?;
    if namespace.is_empty() || route_topic.is_empty() {
        return None;
    }
    Some((namespace, route_topic))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum QueueRoute {
    Task,
    Request,
    Response,
    ParserTask,
    ErrorTask,
    Log,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct QueueBatchPolicy {
    pub max_items: usize,
    pub max_wait_ms: u64,
    pub blocking_item_threshold: usize,
    pub blocking_payload_bytes: Option<usize>,
}

impl QueueBatchPolicy {
    pub(crate) fn should_use_blocking(self, item_count: usize, total_payload_bytes: usize) -> bool {
        item_count >= self.blocking_item_threshold
            || self
                .blocking_payload_bytes
                .is_some_and(|threshold| total_payload_bytes >= threshold)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct QueueRouteContract {
    route: QueueRoute,
    base_topic: String,
    inbound_batch: QueueBatchPolicy,
    outbound_batch: QueueBatchPolicy,
    subscribe_parallelism_divisor: usize,
    min_subscribe_parallelism: usize,
    priority_routed: bool,
    backpressure_scope: &'static str,
}

impl QueueRouteContract {
    pub(crate) fn new(route: QueueRoute, log_topic: &str) -> Self {
        let base_topic = match route {
            QueueRoute::Task => "task",
            QueueRoute::Request => "request",
            QueueRoute::Response => "response",
            QueueRoute::ParserTask => "parser_task",
            QueueRoute::ErrorTask => "error_task",
            QueueRoute::Log => log_topic,
        }
        .to_string();

        Self {
            route,
            base_topic,
            inbound_batch: QueueBatchPolicy {
                max_items: 50,
                max_wait_ms: 5,
                blocking_item_threshold: 32,
                blocking_payload_bytes: Some(LARGE_PAYLOAD_BYTES),
            },
            outbound_batch: QueueBatchPolicy {
                max_items: 500,
                max_wait_ms: 5,
                blocking_item_threshold: 32,
                blocking_payload_bytes: None,
            },
            subscribe_parallelism_divisor: 50,
            min_subscribe_parallelism: 1,
            priority_routed: true,
            backpressure_scope: match route {
                QueueRoute::Task => "task",
                QueueRoute::Request => "request",
                QueueRoute::Response => "response",
                QueueRoute::ParserTask => "parser",
                QueueRoute::ErrorTask => "error",
                QueueRoute::Log => "log",
            },
        }
    }

    #[cfg(test)]
    pub(crate) fn route(&self) -> QueueRoute {
        self.route
    }

    pub(crate) fn route_name(&self) -> &'static str {
        match self.route {
            QueueRoute::Task => "task",
            QueueRoute::Request => "request",
            QueueRoute::Response => "response",
            QueueRoute::ParserTask => "parser_task",
            QueueRoute::ErrorTask => "error_task",
            QueueRoute::Log => "log",
        }
    }

    #[cfg(test)]
    pub(crate) fn base_topic(&self) -> &str {
        &self.base_topic
    }

    pub(crate) fn inbound_batch(&self) -> QueueBatchPolicy {
        self.inbound_batch
    }

    pub(crate) fn outbound_batch(&self) -> QueueBatchPolicy {
        self.outbound_batch
    }

    pub(crate) fn backpressure_scope(&self) -> &'static str {
        self.backpressure_scope
    }

    pub(crate) fn subscribe_parallelism(&self, concurrency: usize) -> usize {
        (concurrency / self.subscribe_parallelism_divisor).max(self.min_subscribe_parallelism)
    }

    pub(crate) fn topic_for_priority(&self, priority: Priority) -> String {
        if self.priority_routed {
            format!("{}-{}", self.base_topic, priority.suffix())
        } else {
            self.base_topic.clone()
        }
    }

    pub(crate) fn priority_topics(&self) -> Vec<String> {
        if self.priority_routed {
            [Priority::High, Priority::Normal, Priority::Low]
                .into_iter()
                .map(|priority| self.topic_for_priority(priority))
                .collect()
        } else {
            vec![self.base_topic.clone()]
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn queue_route_contract_builds_priority_topics_and_keeps_log_topic() {
        let request = QueueRouteContract::new(QueueRoute::Request, "cluster-log");
        assert_eq!(request.route(), QueueRoute::Request);
        assert_eq!(request.base_topic(), "request");
        assert_eq!(request.topic_for_priority(Priority::High), "request-high");
        assert_eq!(
            request.priority_topics(),
            vec![
                "request-high".to_string(),
                "request-normal".to_string(),
                "request-low".to_string()
            ]
        );

        let log = QueueRouteContract::new(QueueRoute::Log, "cluster-log");
        assert_eq!(log.base_topic(), "cluster-log");
        assert_eq!(log.topic_for_priority(Priority::Low), "cluster-log-low");
    }

    #[test]
    fn queue_batch_policy_applies_item_and_payload_thresholds() {
        let policy = QueueBatchPolicy {
            max_items: 50,
            max_wait_ms: 5,
            blocking_item_threshold: 32,
            blocking_payload_bytes: Some(1024),
        };

        assert!(policy.should_use_blocking(32, 0));
        assert!(policy.should_use_blocking(1, 1024));
        assert!(!policy.should_use_blocking(1, 1023));
    }

    #[test]
    fn subscribe_parallelism_has_floor() {
        let contract = QueueRouteContract::new(QueueRoute::Task, "log");
        assert_eq!(contract.subscribe_parallelism(1), 1);
        assert_eq!(contract.subscribe_parallelism(120), 2);
    }

    #[test]
    fn explicit_namespace_topic_helpers_preserve_route_topic() {
        let contract = QueueRouteContract::new(QueueRoute::Response, "log");
        let topic = qualify_topic_namespace("origin", &contract.topic_for_priority(Priority::Normal));

        assert_eq!(topic, "origin::response-normal");
        assert_eq!(
            split_explicit_topic_namespace(&topic),
            Some(("origin", "response-normal"))
        );
        assert_eq!(split_explicit_topic_namespace("response-normal"), None);
        assert_eq!(split_explicit_topic_namespace("::response-normal"), None);
    }
}

use crate::errors::{Error, ErrorKind};
use serde::{Deserialize, Serialize};

/// Retry backoff strategy used by a resolved policy.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum BackoffPolicy {
    /// Do not delay between retries.
    None,
    /// Increase delay by a fixed amount each retry until `max_ms`.
    Linear { base_ms: u64, max_ms: u64 },
    /// Increase delay exponentially until `max_ms`.
    Exponential { base_ms: u64, max_ms: u64 },
}

/// Dead-letter queue strategy for failed events.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum DlqPolicy {
    /// Never route to DLQ.
    Never,
    /// Route to DLQ only after retries are exhausted.
    OnExhausted,
    /// Always route to DLQ immediately.
    Always,
}

/// Alert severity emitted for a handled error.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum AlertLevel {
    /// Informational signal.
    Info,
    /// Warning-level alert.
    Warn,
    /// Error-level alert.
    Error,
    /// Critical operational alert.
    Critical,
}

/// Fully materialized policy returned by the resolver.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Policy {
    /// Whether retry is allowed.
    pub retryable: bool,
    /// Backoff strategy for retries.
    pub backoff: BackoffPolicy,
    /// DLQ routing behavior.
    pub dlq: DlqPolicy,
    /// Alerting level.
    pub alert: AlertLevel,
    /// Maximum retry attempts.
    pub max_retries: u32,
    /// Initial backoff value in milliseconds.
    pub backoff_ms: u64,
}

impl Default for Policy {
    fn default() -> Self {
        Self {
            retryable: true,
            backoff: BackoffPolicy::Exponential {
                base_ms: 500,
                max_ms: 30_000,
            },
            dlq: DlqPolicy::OnExhausted,
            alert: AlertLevel::Warn,
            max_retries: 3,
            backoff_ms: 500,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct PolicyConfig {
    /// Override rules applied on top of built-in defaults.
    pub overrides: Vec<PolicyOverride>,
}

/// A conditional rule used to override the default policy.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PolicyOverride {
    /// Optional domain match, e.g. `engine`.
    pub domain: Option<String>,
    /// Optional event type match, e.g. `download`.
    pub event_type: Option<String>,
    /// Optional lifecycle phase match, e.g. `failed`.
    pub phase: Option<String>,
    /// Required error kind match.
    pub kind: ErrorKind,
    /// Optional override for retryability.
    pub retryable: Option<bool>,
    /// Optional override for backoff strategy.
    pub backoff: Option<BackoffPolicy>,
    /// Optional override for DLQ strategy.
    pub dlq: Option<DlqPolicy>,
    /// Optional override for alert level.
    pub alert: Option<AlertLevel>,
    /// Optional override for max retries.
    pub max_retries: Option<u32>,
    /// Optional override for initial backoff in milliseconds.
    pub backoff_ms: Option<u64>,
}

/// Resolver output with the selected policy and a normalized reason key.
#[derive(Debug, Clone)]
pub struct Decision {
    /// The resolved policy after defaults + overrides.
    pub policy: Policy,
    /// A normalized trace key in the format `domain/event/phase/kind`.
    pub reason: String,
}

/// Resolves runtime error handling policies using default rules and optional overrides.
#[derive(Debug, Clone)]
pub struct PolicyResolver {
    overrides: Vec<PolicyOverride>,
}

impl PolicyResolver {
    /// Creates a resolver from optional config.
    pub fn new(config: Option<&PolicyConfig>) -> Self {
        let overrides = config
            .map(|cfg| cfg.overrides.clone())
            .unwrap_or_default();
        Self { overrides }
    }

    /// Resolves a policy from a concrete [`Error`].
    pub fn resolve_with_error(
        &self,
        domain: &str,
        event_type: Option<&str>,
        phase: Option<&str>,
        err: &Error,
    ) -> Decision {
        self.resolve_with_kind(domain, event_type, phase, err.kind().clone())
    }

    /// Resolves a policy from a known [`ErrorKind`].
    ///
    /// # Examples
    ///
    /// ```
    /// use mocra::common::policy::{DlqPolicy, PolicyResolver};
    /// use mocra::errors::ErrorKind;
    ///
    /// let resolver = PolicyResolver::new(None);
    /// let decision = resolver.resolve_with_kind(
    ///     "engine",
    ///     Some("parser"),
    ///     Some("failed"),
    ///     ErrorKind::Parser,
    /// );
    ///
    /// assert!(!decision.policy.retryable);
    /// assert_eq!(decision.policy.dlq, DlqPolicy::Always);
    /// ```
    pub fn resolve_with_kind(
        &self,
        domain: &str,
        event_type: Option<&str>,
        phase: Option<&str>,
        kind: ErrorKind,
    ) -> Decision {
        let policy = default_policy(domain, event_type, phase, &kind);
        let policy = apply_overrides(
            policy,
            &self.overrides,
            domain,
            event_type,
            phase,
            &kind,
        );
        Decision {
            policy,
            reason: format!(
                "{}/{}/{}/{:?}",
                normalize(domain),
                normalize_opt(event_type).unwrap_or("-") ,
                normalize_opt(phase).unwrap_or("-") ,
                kind
            ),
        }
    }
}

fn normalize(value: &str) -> &str {
    value.trim()
}

fn normalize_opt(value: Option<&str>) -> Option<&str> {
    value.map(|v| v.trim()).filter(|v| !v.is_empty())
}

fn default_policy(
    domain: &str,
    event_type: Option<&str>,
    phase: Option<&str>,
    kind: &ErrorKind,
) -> Policy {
    let domain = normalize(domain).to_ascii_lowercase();
    let event_type = normalize_opt(event_type)
        .unwrap_or("")
        .to_ascii_lowercase();
    let phase = normalize_opt(phase).unwrap_or("").to_ascii_lowercase();

    let is_failed_or_retry = phase == "failed" || phase == "retry";

    match (domain.as_str(), event_type.as_str(), is_failed_or_retry, kind) {
        ("engine", "task_model", true, ErrorKind::Task | ErrorKind::Module) => Policy {
            backoff: BackoffPolicy::Exponential {
                base_ms: 500,
                max_ms: 30_000,
            },
            dlq: DlqPolicy::OnExhausted,
            alert: AlertLevel::Warn,
            max_retries: 3,
            backoff_ms: 500,
            retryable: true,
        },
        ("engine", "parser_task_model", true, ErrorKind::Parser | ErrorKind::ProcessorChain) => Policy {
            backoff: BackoffPolicy::Exponential {
                base_ms: 500,
                max_ms: 30_000,
            },
            dlq: DlqPolicy::OnExhausted,
            alert: AlertLevel::Warn,
            max_retries: 3,
            backoff_ms: 500,
            retryable: true,
        },
        ("engine", "request_publish", true, ErrorKind::Queue) => Policy {
            backoff: BackoffPolicy::Exponential {
                base_ms: 500,
                max_ms: 30_000,
            },
            dlq: DlqPolicy::OnExhausted,
            alert: AlertLevel::Warn,
            max_retries: 3,
            backoff_ms: 500,
            retryable: true,
        },
        ("engine", "request_middleware", _, ErrorKind::Request | ErrorKind::ProcessorChain) => Policy {
            retryable: false,
            backoff: BackoffPolicy::None,
            dlq: DlqPolicy::Always,
            alert: AlertLevel::Error,
            max_retries: 0,
            backoff_ms: 0,
        },
        ("engine", "download", true, ErrorKind::Download | ErrorKind::Proxy | ErrorKind::RateLimit) => Policy {
            backoff: BackoffPolicy::Exponential {
                base_ms: 500,
                max_ms: 60_000,
            },
            dlq: DlqPolicy::OnExhausted,
            alert: AlertLevel::Warn,
            max_retries: 5,
            backoff_ms: 500,
            retryable: true,
        },
        ("engine", "response_middleware", _, ErrorKind::Response | ErrorKind::ProcessorChain) => Policy {
            retryable: false,
            backoff: BackoffPolicy::None,
            dlq: DlqPolicy::Always,
            alert: AlertLevel::Error,
            max_retries: 0,
            backoff_ms: 0,
        },
        ("engine", "response_publish", true, ErrorKind::Queue) => Policy {
            backoff: BackoffPolicy::Exponential {
                base_ms: 500,
                max_ms: 30_000,
            },
            dlq: DlqPolicy::OnExhausted,
            alert: AlertLevel::Warn,
            max_retries: 3,
            backoff_ms: 500,
            retryable: true,
        },
        ("engine", "module_generate", true, ErrorKind::Module | ErrorKind::Task) => Policy {
            backoff: BackoffPolicy::Exponential {
                base_ms: 500,
                max_ms: 30_000,
            },
            dlq: DlqPolicy::OnExhausted,
            alert: AlertLevel::Error,
            max_retries: 3,
            backoff_ms: 500,
            retryable: true,
        },
        ("engine", "parser", true, ErrorKind::Parser) => Policy {
            retryable: false,
            backoff: BackoffPolicy::None,
            dlq: DlqPolicy::Always,
            alert: AlertLevel::Error,
            max_retries: 0,
            backoff_ms: 0,
        },
        ("engine", "middleware_before", _, ErrorKind::DataMiddleware) => Policy {
            backoff: BackoffPolicy::Linear {
                base_ms: 200,
                max_ms: 10_000,
            },
            dlq: DlqPolicy::OnExhausted,
            alert: AlertLevel::Warn,
            max_retries: 3,
            backoff_ms: 200,
            retryable: true,
        },
        ("engine", "data_store", _, ErrorKind::DataStore | ErrorKind::Orm) => Policy {
            backoff: BackoffPolicy::Linear {
                base_ms: 200,
                max_ms: 10_000,
            },
            dlq: DlqPolicy::OnExhausted,
            alert: AlertLevel::Error,
            max_retries: 3,
            backoff_ms: 200,
            retryable: true,
        },
        ("system", "system_error", true, _) => Policy {
            retryable: false,
            backoff: BackoffPolicy::None,
            dlq: DlqPolicy::Never,
            alert: AlertLevel::Error,
            max_retries: 0,
            backoff_ms: 0,
        },
        _ => Policy::default(),
    }
}

fn apply_overrides(
    mut policy: Policy,
    overrides: &[PolicyOverride],
    domain: &str,
    event_type: Option<&str>,
    phase: Option<&str>,
    kind: &ErrorKind,
) -> Policy {
    let domain = normalize(domain).to_ascii_lowercase();
    let event_type = normalize_opt(event_type)
        .unwrap_or("")
        .to_ascii_lowercase();
    let phase = normalize_opt(phase).unwrap_or("").to_ascii_lowercase();

    let mut best_score = -1_i32;
    let mut best_override: Option<&PolicyOverride> = None;

    for override_item in overrides {
        if &override_item.kind != kind {
            continue;
        }

        let domain_match = match &override_item.domain {
            Some(value) => value.trim().eq_ignore_ascii_case(&domain),
            None => true,
        };
        if !domain_match {
            continue;
        }

        let event_match = match &override_item.event_type {
            Some(value) => value.trim().eq_ignore_ascii_case(&event_type),
            None => true,
        };
        if !event_match {
            continue;
        }

        let phase_match = match &override_item.phase {
            Some(value) => value.trim().eq_ignore_ascii_case(&phase),
            None => true,
        };
        if !phase_match {
            continue;
        }

        let score = (override_item.domain.is_some() as i32)
            + (override_item.event_type.is_some() as i32)
            + (override_item.phase.is_some() as i32)
            + 1;

        if score > best_score || (score == best_score && best_override.is_some()) {
            best_score = score;
            best_override = Some(override_item);
        }
    }

    if let Some(override_item) = best_override {
        if let Some(value) = override_item.retryable {
            policy.retryable = value;
        }
        if let Some(value) = override_item.backoff {
            policy.backoff = value;
        }
        if let Some(value) = override_item.dlq {
            policy.dlq = value;
        }
        if let Some(value) = override_item.alert {
            policy.alert = value;
        }
        if let Some(value) = override_item.max_retries {
            policy.max_retries = value;
        }
        if let Some(value) = override_item.backoff_ms {
            policy.backoff_ms = value;
        }
    }

    policy
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_policy_parser_failed_is_not_retryable() {
        let resolver = PolicyResolver::new(None);
        let decision = resolver.resolve_with_kind(
            "engine",
            Some("parser"),
            Some("failed"),
            ErrorKind::Parser,
        );
        assert!(!decision.policy.retryable);
        assert_eq!(decision.policy.dlq, DlqPolicy::Always);
    }

    #[test]
    fn overrides_take_precedence() {
        let cfg = PolicyConfig {
            overrides: vec![PolicyOverride {
                domain: Some("engine".to_string()),
                event_type: Some("download".to_string()),
                phase: Some("failed".to_string()),
                kind: ErrorKind::Download,
                retryable: Some(false),
                backoff: Some(BackoffPolicy::None),
                dlq: Some(DlqPolicy::Always),
                alert: Some(AlertLevel::Error),
                max_retries: Some(0),
                backoff_ms: Some(0),
            }],
        };

        let resolver = PolicyResolver::new(Some(&cfg));
        let decision = resolver.resolve_with_kind(
            "engine",
            Some("download"),
            Some("failed"),
            ErrorKind::Download,
        );

        assert!(!decision.policy.retryable);
        assert_eq!(decision.policy.dlq, DlqPolicy::Always);
        assert_eq!(decision.policy.max_retries, 0);
    }
}

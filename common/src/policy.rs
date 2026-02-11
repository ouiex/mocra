use errors::{Error, ErrorKind};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum BackoffPolicy {
    None,
    Linear { base_ms: u64, max_ms: u64 },
    Exponential { base_ms: u64, max_ms: u64 },
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum DlqPolicy {
    Never,
    OnExhausted,
    Always,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum AlertLevel {
    Info,
    Warn,
    Error,
    Critical,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Policy {
    pub retryable: bool,
    pub backoff: BackoffPolicy,
    pub dlq: DlqPolicy,
    pub alert: AlertLevel,
    pub max_retries: u32,
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
    pub overrides: Vec<PolicyOverride>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PolicyOverride {
    pub domain: Option<String>,
    pub event_type: Option<String>,
    pub phase: Option<String>,
    pub kind: ErrorKind,
    pub retryable: Option<bool>,
    pub backoff: Option<BackoffPolicy>,
    pub dlq: Option<DlqPolicy>,
    pub alert: Option<AlertLevel>,
    pub max_retries: Option<u32>,
    pub backoff_ms: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct Decision {
    pub policy: Policy,
    pub reason: String,
}

#[derive(Debug, Clone)]
pub struct PolicyResolver {
    overrides: Vec<PolicyOverride>,
}

impl PolicyResolver {
    pub fn new(config: Option<&PolicyConfig>) -> Self {
        let overrides = config
            .map(|cfg| cfg.overrides.clone())
            .unwrap_or_default();
        Self { overrides }
    }

    pub fn resolve_with_error(
        &self,
        domain: &str,
        event_type: Option<&str>,
        phase: Option<&str>,
        err: &Error,
    ) -> Decision {
        self.resolve_with_kind(domain, event_type, phase, err.kind().clone())
    }

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

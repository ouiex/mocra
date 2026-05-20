use std::sync::Arc;

use async_trait::async_trait;
use futures::StreamExt;

use crate::common::interface::ModuleNodeTrait;
use crate::common::response_cache::apply_request_response_cache_policy;
use crate::engine::task::module_node_runtime_bridge::{
    decode_generate_runtime_input, decode_parser_runtime_input, encode_parser_output_payload,
    encode_request_batch_payload, is_generate_runtime_input, is_parser_runtime_input,
};
use crate::schedule::dag::{DagError, DagNodeTrait, NodeExecutionContext, TaskPayload};

pub(crate) const SCHEDULER_PARSER_NODE_ERROR_PREFIX: &str = "module parser returned error: ";

pub(crate) fn scheduler_parser_error_message(err: &DagError) -> Option<(&str, &str)> {
    match err {
        DagError::NodeExecutionFailed { node_id, reason } => reason
            .strip_prefix(SCHEDULER_PARSER_NODE_ERROR_PREFIX)
            .map(|message| (node_id.as_str(), message)),
        DagError::RetryExhausted {
            node_id,
            last_error,
            ..
        } => last_error
            .split_once(SCHEDULER_PARSER_NODE_ERROR_PREFIX)
            .map(|(_, message)| (node_id.as_str(), message)),
        _ => None,
    }
}

/// Adapter that bridges ModuleNodeTrait into DagNodeTrait.
///
/// Current phase keeps the old chain runtime as primary execution path.
/// This adapter can execute real `generate()` and `parser()` logic only when
/// `NodeExecutionContext` carries a scheduler runtime_input payload produced by
/// the engine-side bridge.
pub struct ModuleNodeDagAdapter {
    node: Arc<dyn ModuleNodeTrait>,
}

impl ModuleNodeDagAdapter {
    pub fn new(node: Arc<dyn ModuleNodeTrait>) -> Self {
        Self { node }
    }

    pub fn inner(&self) -> Arc<dyn ModuleNodeTrait> {
        self.node.clone()
    }
}

#[async_trait]
impl DagNodeTrait for ModuleNodeDagAdapter {
    async fn start(&self, context: NodeExecutionContext) -> Result<TaskPayload, DagError> {
        let runtime_input = context.runtime_input.as_ref().ok_or_else(|| DagError::NodeExecutionFailed {
            node_id: context.node_id.clone(),
            reason: "ModuleNodeDagAdapter requires runtime_input to execute node logic".to_string(),
        })?;

        if is_generate_runtime_input(runtime_input) {
            let generate_input = decode_generate_runtime_input(runtime_input).map_err(|e| {
                DagError::NodeExecutionFailed {
                    node_id: context.node_id.clone(),
                    reason: format!("decode runtime_input failed: {e}"),
                }
            })?;

            if generate_input.routing.node_key != context.node_id {
                let scheduler_node_id = context.node_id.clone();
                return Err(DagError::NodeExecutionFailed {
                    node_id: scheduler_node_id.clone(),
                    reason: format!(
                        "runtime_input node_key '{}' does not match scheduler node_id '{}'",
                        generate_input.routing.node_key, scheduler_node_id
                    ),
                });
            }

            let request_stream = self
                .node
                .generate(generate_input.borrowed())
                .await
                .map_err(|e| DagError::NodeExecutionFailed {
                    node_id: context.node_id.clone(),
                    reason: e.to_string(),
                })?;
            let response_cache_common = generate_input.config.common.clone();
            let requests = request_stream
                .collect::<Vec<_>>()
                .await
                .into_iter()
                .map(|request| apply_request_response_cache_policy(request, &response_cache_common))
                .collect::<Vec<_>>();

            return encode_request_batch_payload(&requests).map_err(|e| DagError::NodeExecutionFailed {
                node_id: context.node_id,
                reason: format!("encode request batch failed: {e}"),
            });
        }

        if is_parser_runtime_input(runtime_input) {
            let parser_input = decode_parser_runtime_input(runtime_input).map_err(|e| {
                DagError::NodeExecutionFailed {
                    node_id: context.node_id.clone(),
                    reason: format!("decode runtime_input failed: {e}"),
                }
            })?;

            if parser_input.routing.node_key != context.node_id {
                let scheduler_node_id = context.node_id.clone();
                return Err(DagError::NodeExecutionFailed {
                    node_id: scheduler_node_id.clone(),
                    reason: format!(
                        "runtime_input node_key '{}' does not match scheduler node_id '{}'",
                        parser_input.routing.node_key, scheduler_node_id
                    ),
                });
            }

            let output = self
                .node
                .parser(parser_input.response.clone(), parser_input.borrowed())
                .await
                .map_err(|e| DagError::NodeExecutionFailed {
                    node_id: context.node_id.clone(),
                    reason: format!("{SCHEDULER_PARSER_NODE_ERROR_PREFIX}{e}"),
                })?;

            return encode_parser_output_payload(output).map_err(|e| DagError::NodeExecutionFailed {
                node_id: context.node_id,
                reason: format!("encode parser output failed: {e}"),
            });
        }

        Err(DagError::NodeExecutionFailed {
            node_id: context.node_id,
            reason: format!(
                "unsupported runtime_input content_type: {:?}",
                runtime_input.content_type
            ),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use async_trait::async_trait;

    use super::ModuleNodeDagAdapter;
    use crate::common::interface::{ModuleNodeTrait, NodeGenerateContext, NodeParseContext, SyncBoxStream, ToSyncBoxStream};
    use crate::common::model::login_info::LoginInfo;
    use crate::common::model::{
        ExecutionMark, ExecutionMeta, NodeDispatch, NodeInput, NodeParseOutput, PayloadCodec,
        Priority, Request, ResolvedCommonConfig, ResolvedNodeConfig, Response, RoutingMeta,
        TypedEnvelope,
    };
    use crate::common::response_cache::{RESPONSE_CACHE_EXPIRES_AT_KEY, current_time_ms};
    use crate::engine::task::module_node_runtime_bridge::{
        SchedulerNodeGenerateRuntimeInput, SchedulerNodeParserRuntimeInput,
        decode_parser_output_payload, decode_request_batch_payload, encode_generate_runtime_input,
        encode_parser_runtime_input,
    };
    use crate::schedule::dag::{DagNodeTrait, NodeExecutionContext};
    use crate::errors::Result as CResult;
    use uuid::Uuid;

    struct CapturingGenerateNode;

    #[async_trait]
    impl ModuleNodeTrait for CapturingGenerateNode {
        async fn generate(&self, ctx: NodeGenerateContext<'_>) -> CResult<SyncBoxStream<'static, Request>> {
            let mut request = Request::new("https://example.com/generated", "GET");
            request.account = ctx.routing.account.clone();
            request.platform = ctx.routing.platform.clone();
            request.module = ctx.routing.module.clone();
            request.context = request.context.with_node_id(ctx.routing.node_key.clone());
            request.priority = ctx.routing.priority;
            Ok(vec![request].to_stream())
        }

        async fn parser(
            &self,
            response: Response,
            ctx: NodeParseContext<'_>,
        ) -> CResult<NodeParseOutput> {
            Ok(NodeParseOutput::default()
                .with_next(NodeDispatch::new(
                    "detail",
                    NodeInput::new(
                        "detail",
                        TypedEnvelope::new(
                            "node.input",
                            1,
                            PayloadCodec::Json,
                            format!(
                                "{{\"status\":{},\"node\":\"{}\"}}",
                                response.status_code, ctx.routing.node_key
                            )
                            .into_bytes(),
                        ),
                    ),
                ))
                .finish())
        }

        fn stable_node_key(&self) -> &'static str {
            "page"
        }
    }

    fn sample_runtime_input() -> SchedulerNodeGenerateRuntimeInput {
        SchedulerNodeGenerateRuntimeInput {
            routing: RoutingMeta {
                namespace: "demo".to_string(),
                account: "account-a".to_string(),
                platform: "platform-x".to_string(),
                module: "catalog".to_string(),
                node_key: "page".to_string(),
                run_id: Uuid::now_v7(),
                request_id: Uuid::now_v7(),
                parent_request_id: None,
                priority: Priority::High,
            },
            exec: ExecutionMeta::default(),
            config: ResolvedNodeConfig {
                profile_key: "demo:profile:account-a:platform-x:catalog".to_string(),
                profile_version: 1,
                common: ResolvedCommonConfig::default(),
                node_config: TypedEnvelope::new(
                    "node.config",
                    1,
                    PayloadCodec::Json,
                    br#"{"limit":10}"#.to_vec(),
                ),
            },
            input: NodeInput::new(
                "page",
                TypedEnvelope::new(
                    "node.input",
                    1,
                    PayloadCodec::Json,
                    br#"{"cursor":"abc"}"#.to_vec(),
                ),
            ),
            login_info: Some(LoginInfo::default()),
        }
    }

    fn sample_parser_runtime_input() -> SchedulerNodeParserRuntimeInput {
        SchedulerNodeParserRuntimeInput {
            routing: RoutingMeta {
                namespace: "demo".to_string(),
                account: "account-a".to_string(),
                platform: "platform-x".to_string(),
                module: "catalog".to_string(),
                node_key: "page".to_string(),
                run_id: Uuid::now_v7(),
                request_id: Uuid::now_v7(),
                parent_request_id: Some(Uuid::now_v7()),
                priority: Priority::High,
            },
            exec: ExecutionMeta::default(),
            config: ResolvedNodeConfig {
                profile_key: "demo:profile:account-a:platform-x:catalog".to_string(),
                profile_version: 1,
                common: ResolvedCommonConfig::default(),
                node_config: TypedEnvelope::new(
                    "node.config",
                    1,
                    PayloadCodec::Json,
                    br#"{\"limit\":10}"#.to_vec(),
                ),
            },
            response: Response {
                id: Uuid::now_v7(),
                platform: "platform-x".to_string(),
                account: "account-a".to_string(),
                module: "catalog".to_string(),
                status_code: 200,
                cookies: Default::default(),
                content: br#"{\"ok\":true}"#.to_vec(),
                storage_path: None,
                headers: vec![("content-type".to_string(), "application/json".to_string())],
                task_retry_times: 1,
                metadata: Default::default(),
                download_middleware: Vec::new(),
                data_middleware: Vec::new(),
                task_finished: false,
                context: ExecutionMark::default().with_node_id("page"),
                run_id: Uuid::now_v7(),
                prefix_request: Uuid::now_v7(),
                request_hash: None,
                priority: Priority::High,
            },
            login_info: Some(LoginInfo::default()),
        }
    }

    #[tokio::test]
    async fn adapter_executes_generate_when_runtime_input_is_present() {
        let adapter = ModuleNodeDagAdapter::new(Arc::new(CapturingGenerateNode));
        let mut runtime_input = sample_runtime_input();
        runtime_input.config.common.response_cache_enabled = true;
        runtime_input.config.common.response_cache_ttl_secs = Some(60);
        let payload = encode_generate_runtime_input(&runtime_input).expect("encode runtime input");
        let output = adapter
            .start(NodeExecutionContext {
                run_id: runtime_input.routing.run_id.to_string(),
                run_fencing_token: None,
                node_id: "page".to_string(),
                attempt: 1,
                upstream_nodes: vec![],
                upstream_outputs: BTreeMap::new().into_iter().collect(),
                runtime_input: Some(payload),
                layer_index: 0,
            })
            .await
            .expect("generate should succeed");

        let requests = decode_request_batch_payload(&output).expect("decode request batch");
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].url, "https://example.com/generated");
        assert_eq!(requests[0].account, "account-a");
        assert_eq!(requests[0].priority, Priority::High);
        assert!(requests[0].enable_response_cache);
        let expires_at = requests[0]
            .meta
            .get_trait_config::<i64>(RESPONSE_CACHE_EXPIRES_AT_KEY)
            .expect("generated request should carry explicit cache expiry");
        assert!(expires_at > current_time_ms());
    }

    #[tokio::test]
    async fn adapter_rejects_missing_runtime_input() {
        let adapter = ModuleNodeDagAdapter::new(Arc::new(CapturingGenerateNode));
        let err = adapter
            .start(NodeExecutionContext {
                run_id: "run-1".to_string(),
                run_fencing_token: None,
                node_id: "page".to_string(),
                attempt: 1,
                upstream_nodes: vec![],
                upstream_outputs: Default::default(),
                runtime_input: None,
                layer_index: 0,
            })
            .await
            .expect_err("missing runtime_input should fail");

        assert!(err.to_string().contains("requires runtime_input"));
    }

    #[tokio::test]
    async fn adapter_executes_parser_when_runtime_input_is_present() {
        let adapter = ModuleNodeDagAdapter::new(Arc::new(CapturingGenerateNode));
        let runtime_input = sample_parser_runtime_input();
        let payload = encode_parser_runtime_input(&runtime_input).expect("encode parser input");
        let output = adapter
            .start(NodeExecutionContext {
                run_id: runtime_input.routing.run_id.to_string(),
                run_fencing_token: None,
                node_id: "page".to_string(),
                attempt: 1,
                upstream_nodes: vec![],
                upstream_outputs: BTreeMap::new().into_iter().collect(),
                runtime_input: Some(payload),
                layer_index: 0,
            })
            .await
            .expect("parser should succeed");

        let parsed = decode_parser_output_payload(&output).expect("decode parser output");
        assert_eq!(parsed.next.len(), 1);
        assert_eq!(parsed.next[0].target_node, "detail");
        assert!(parsed.finished);
    }
}

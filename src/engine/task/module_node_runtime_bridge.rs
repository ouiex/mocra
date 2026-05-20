use crate::common::interface::module::NodeGenerateContext;
use crate::common::interface::module::NodeParseContext;
use crate::common::model::login_info::LoginInfo;
#[cfg(test)]
use crate::common::model::ModuleConfig;
use crate::common::model::{
    data::{DataEvent, DataFrameStore, DataType, DataframeStoreData, FileStore, StoreContext},
    ExecutionMeta, NodeDispatch, NodeInput, NodeParseOutput, ParsedData, Request,
    ResolvedNodeConfig, Response, RoutingMeta,
};
#[cfg(test)]
use crate::engine::task::node_context_adapter::{
    build_module_config_generate_context, build_module_config_parse_context,
};
use crate::schedule::dag::{DagError, TaskPayload};
use serde::{Deserialize, Serialize};
#[cfg(test)]
use serde_json::{Map, Value};
use uuid::Uuid;

const GENERATE_INPUT_CONTENT_TYPE: &str = "application/x-mocra-node-generate-input";
const PARSER_INPUT_CONTENT_TYPE: &str = "application/x-mocra-node-parser-input";
const PARSER_OUTPUT_CONTENT_TYPE: &str = "application/x-mocra-node-parser-output";
const REQUEST_BATCH_CONTENT_TYPE: &str = "application/x-mocra-request-batch";
const GENERATE_INPUT_SCHEMA_ID: &str = "mocra.scheduler.generate_input";
const PARSER_INPUT_SCHEMA_ID: &str = "mocra.scheduler.parser_input";
const PARSER_OUTPUT_SCHEMA_ID: &str = "mocra.scheduler.parser_output";
const REQUEST_BATCH_SCHEMA_ID: &str = "mocra.scheduler.request_batch";
const RUNTIME_BRIDGE_SCHEMA_VERSION: &str = "1";

fn has_runtime_bridge_schema(payload: &TaskPayload, expected_schema: &str) -> bool {
    payload
        .metadata
        .get("schema")
        .map(String::as_str)
        == Some(expected_schema)
        && payload
            .metadata
            .get("version")
            .map(String::as_str)
            == Some(RUNTIME_BRIDGE_SCHEMA_VERSION)
}

fn validate_runtime_bridge_payload(
    payload: &TaskPayload,
    expected_content_type: &str,
    expected_schema: &str,
) -> Result<(), DagError> {
    if payload.content_type.as_deref() != Some(expected_content_type) {
        return Err(DagError::InvalidPayloadEnvelope(format!(
            "unexpected runtime bridge content_type: expected {:?}, got {:?}",
            expected_content_type, payload.content_type
        )));
    }

    if !has_runtime_bridge_schema(payload, expected_schema) {
        return Err(DagError::InvalidPayloadEnvelope(format!(
            "unexpected runtime bridge schema: expected {}@{}, got schema={:?} version={:?}",
            expected_schema,
            RUNTIME_BRIDGE_SCHEMA_VERSION,
            payload.metadata.get("schema"),
            payload.metadata.get("version")
        )));
    }

    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchedulerNodeGenerateRuntimeInput {
    pub routing: RoutingMeta,
    pub exec: ExecutionMeta,
    pub config: ResolvedNodeConfig,
    pub input: NodeInput,
    pub login_info: Option<LoginInfo>,
}

impl SchedulerNodeGenerateRuntimeInput {
    pub fn borrowed(&self) -> NodeGenerateContext<'_> {
        NodeGenerateContext {
            routing: &self.routing,
            exec: &self.exec,
            config: &self.config,
            input: &self.input,
            login_info: self.login_info.as_ref(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchedulerNodeParserRuntimeInput {
    pub routing: RoutingMeta,
    pub exec: ExecutionMeta,
    pub config: ResolvedNodeConfig,
    pub response: Response,
    pub login_info: Option<LoginInfo>,
}

impl SchedulerNodeParserRuntimeInput {
    pub fn borrowed(&self) -> NodeParseContext<'_> {
        NodeParseContext {
            routing: &self.routing,
            exec: &self.exec,
            config: &self.config,
            login_info: self.login_info.as_ref(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchedulerNodeParserRuntimeOutput {
    pub next: Vec<NodeDispatch>,
    pub data: Vec<SchedulerParsedData>,
    pub finished: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchedulerParsedData {
    pub request_id: Uuid,
    pub platform: String,
    pub account: String,
    pub module: String,
    pub meta: crate::common::model::meta::MetaData,
    pub data: SchedulerParsedDataPayload,
    pub data_middleware: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SchedulerParsedDataPayload {
    DataFrame {
        ipc_bytes: Vec<u8>,
        schema: String,
        table: String,
    },
    File {
        file_name: String,
        file_path: String,
        content: Vec<u8>,
    },
}

impl SchedulerParsedData {
    fn from_parsed_data(data: ParsedData) -> Result<Self, DagError> {
        let payload = match data.data {
            DataType::DataFrame(store) => SchedulerParsedDataPayload::DataFrame {
                ipc_bytes: match &store.data {
                    DataframeStoreData::Bytes(bytes) => bytes.clone(),
                    DataframeStoreData::DataFrame(df) => {
                        match DataFrameStore::default().with_data(df.clone()).data {
                            DataframeStoreData::Bytes(bytes) => bytes,
                            DataframeStoreData::DataFrame(_) => unreachable!(),
                        }
                    }
                },
                schema: store.schema,
                table: store.table,
            },
            DataType::File(store) => SchedulerParsedDataPayload::File {
                file_name: store.file_name,
                file_path: store.file_path,
                content: store.content,
            },
        };

        Ok(Self {
            request_id: data.request_id,
            platform: data.platform,
            account: data.account,
            module: data.module,
            meta: data.meta,
            data: payload,
            data_middleware: data.data_middleware,
        })
    }

    fn into_parsed_data(self) -> ParsedData {
        let ctx = StoreContext {
            request_id: self.request_id,
            platform: self.platform.clone(),
            account: self.account.clone(),
            module: self.module.clone(),
            meta: self.meta.clone(),
            data_middleware: self.data_middleware.clone(),
        };

        let data = match self.data {
            SchedulerParsedDataPayload::DataFrame {
                ipc_bytes,
                schema,
                table,
            } => DataType::DataFrame(
                DataFrameStore::default()
                    .with_ctx(ctx)
                    .with_ipc_bytes(ipc_bytes)
                    .with_schema(schema)
                    .with_table(table),
            ),
            SchedulerParsedDataPayload::File {
                file_name,
                file_path,
                content,
            } => DataType::File(
                FileStore::default()
                    .with_ctx(ctx)
                    .with_name(file_name)
                    .with_path(file_path)
                    .with_content(content),
            ),
        };

        DataEvent {
            request_id: self.request_id,
            platform: self.platform,
            account: self.account,
            module: self.module,
            meta: self.meta,
            data,
            data_middleware: self.data_middleware,
        }
    }
}

impl SchedulerNodeParserRuntimeOutput {
    fn from_node_parse_output(output: NodeParseOutput) -> Result<Self, DagError> {
        Ok(Self {
            next: output.next,
            data: output
                .data
                .into_iter()
                .map(SchedulerParsedData::from_parsed_data)
                .collect::<Result<Vec<_>, _>>()?,
            finished: output.finished,
        })
    }

    pub fn into_node_parse_output(self) -> NodeParseOutput {
        NodeParseOutput {
            next: self.next,
            data: self
                .data
                .into_iter()
                .map(SchedulerParsedData::into_parsed_data)
                .collect(),
            finished: self.finished,
        }
    }
}

#[cfg(test)]
pub(crate) fn build_module_config_generate_runtime_input(
    module_id: &str,
    run_id: Uuid,
    node_key: &str,
    base_common: crate::common::model::ResolvedCommonConfig,
    config: &ModuleConfig,
    params: Map<String, Value>,
    login_info: Option<LoginInfo>,
    parent_request_id: Option<Uuid>,
) -> SchedulerNodeGenerateRuntimeInput {
    let context = build_module_config_generate_context(
        module_id,
        run_id,
        node_key,
        base_common,
        config,
        params,
        login_info,
        parent_request_id,
    );

    SchedulerNodeGenerateRuntimeInput {
        routing: context.routing,
        exec: context.exec,
        config: context.config,
        input: context.input,
        login_info: context.login_info,
    }
}

#[cfg(test)]
pub(crate) fn build_module_config_parse_runtime_input(
    module_id: &str,
    node_key: &str,
    base_common: crate::common::model::ResolvedCommonConfig,
    config: Option<&ModuleConfig>,
    response: &Response,
) -> SchedulerNodeParserRuntimeInput {
    let context = build_module_config_parse_context(
        module_id,
        node_key,
        base_common,
        config,
        response,
    );

    SchedulerNodeParserRuntimeInput {
        routing: context.routing,
        exec: context.exec,
        config: context.config,
        response: response.clone(),
        login_info: context.login_info,
    }
}

pub(crate) fn encode_generate_runtime_input(
    input: &SchedulerNodeGenerateRuntimeInput,
) -> Result<TaskPayload, DagError> {
    let bytes = serde_json::to_vec(input)
        .map_err(|e| DagError::InvalidPayloadEnvelope(format!("encode generate runtime input: {e}")))?;
    Ok(TaskPayload::from_bytes(bytes)
        .with_content_type(GENERATE_INPUT_CONTENT_TYPE)
        .with_meta("schema", GENERATE_INPUT_SCHEMA_ID)
        .with_meta("version", RUNTIME_BRIDGE_SCHEMA_VERSION))
}

pub(crate) fn decode_generate_runtime_input(
    payload: &TaskPayload,
) -> Result<SchedulerNodeGenerateRuntimeInput, DagError> {
    validate_runtime_bridge_payload(payload, GENERATE_INPUT_CONTENT_TYPE, GENERATE_INPUT_SCHEMA_ID)?;

    serde_json::from_slice(&payload.bytes)
        .map_err(|e| DagError::InvalidPayloadEnvelope(format!("decode generate runtime input: {e}")))
}

pub(crate) fn encode_parser_runtime_input(
    input: &SchedulerNodeParserRuntimeInput,
) -> Result<TaskPayload, DagError> {
    let bytes = serde_json::to_vec(input)
        .map_err(|e| DagError::InvalidPayloadEnvelope(format!("encode parser runtime input: {e}")))?;
    Ok(TaskPayload::from_bytes(bytes)
        .with_content_type(PARSER_INPUT_CONTENT_TYPE)
        .with_meta("schema", PARSER_INPUT_SCHEMA_ID)
        .with_meta("version", RUNTIME_BRIDGE_SCHEMA_VERSION))
}

pub(crate) fn decode_parser_runtime_input(
    payload: &TaskPayload,
) -> Result<SchedulerNodeParserRuntimeInput, DagError> {
    validate_runtime_bridge_payload(payload, PARSER_INPUT_CONTENT_TYPE, PARSER_INPUT_SCHEMA_ID)?;

    serde_json::from_slice(&payload.bytes)
        .map_err(|e| DagError::InvalidPayloadEnvelope(format!("decode parser runtime input: {e}")))
}

pub(crate) fn encode_parser_output_payload(
    output: NodeParseOutput,
) -> Result<TaskPayload, DagError> {
    let encoded = SchedulerNodeParserRuntimeOutput::from_node_parse_output(output)?;
    let bytes = serde_json::to_vec(&encoded)
        .map_err(|e| DagError::InvalidPayloadEnvelope(format!("encode parser output: {e}")))?;
    Ok(TaskPayload::from_bytes(bytes)
        .with_content_type(PARSER_OUTPUT_CONTENT_TYPE)
        .with_meta("schema", PARSER_OUTPUT_SCHEMA_ID)
        .with_meta("version", RUNTIME_BRIDGE_SCHEMA_VERSION))
}

pub(crate) fn decode_parser_output_payload(
    payload: &TaskPayload,
) -> Result<SchedulerNodeParserRuntimeOutput, DagError> {
    validate_runtime_bridge_payload(payload, PARSER_OUTPUT_CONTENT_TYPE, PARSER_OUTPUT_SCHEMA_ID)?;

    serde_json::from_slice(&payload.bytes)
        .map_err(|e| DagError::InvalidPayloadEnvelope(format!("decode parser output: {e}")))
}

pub(crate) fn is_generate_runtime_input(payload: &TaskPayload) -> bool {
    payload.content_type.as_deref() == Some(GENERATE_INPUT_CONTENT_TYPE)
        && has_runtime_bridge_schema(payload, GENERATE_INPUT_SCHEMA_ID)
}

pub(crate) fn is_parser_runtime_input(payload: &TaskPayload) -> bool {
    payload.content_type.as_deref() == Some(PARSER_INPUT_CONTENT_TYPE)
        && has_runtime_bridge_schema(payload, PARSER_INPUT_SCHEMA_ID)
}

pub(crate) fn encode_request_batch_payload(requests: &[Request]) -> Result<TaskPayload, DagError> {
    let bytes = serde_json::to_vec(requests)
        .map_err(|e| DagError::InvalidPayloadEnvelope(format!("encode request batch: {e}")))?;
    Ok(TaskPayload::from_bytes(bytes)
        .with_content_type(REQUEST_BATCH_CONTENT_TYPE)
        .with_meta("schema", REQUEST_BATCH_SCHEMA_ID)
        .with_meta("version", RUNTIME_BRIDGE_SCHEMA_VERSION))
}

#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn decode_request_batch_payload(payload: &TaskPayload) -> Result<Vec<Request>, DagError> {
    validate_runtime_bridge_payload(payload, REQUEST_BATCH_CONTENT_TYPE, REQUEST_BATCH_SCHEMA_ID)?;

    serde_json::from_slice(&payload.bytes)
        .map_err(|e| DagError::InvalidPayloadEnvelope(format!("decode request batch: {e}")))
}

#[cfg(test)]
mod tests {
    use super::{
        SchedulerNodeGenerateRuntimeInput, SchedulerNodeParserRuntimeInput,
        build_module_config_generate_runtime_input,
        build_module_config_parse_runtime_input,
        decode_generate_runtime_input, decode_parser_output_payload, decode_parser_runtime_input,
        decode_request_batch_payload, encode_generate_runtime_input,
        encode_parser_output_payload, encode_parser_runtime_input, encode_request_batch_payload,
        is_generate_runtime_input, is_parser_runtime_input,
    };
    use crate::common::model::ModuleConfig;
    use crate::common::model::{
        data::{DataType, FileStore, StoreContext},
        ExecutionMark, ExecutionMeta, NodeDispatch, NodeInput, NodeParseOutput, PayloadCodec,
        Priority, Request, ResolvedCommonConfig, ResolvedNodeConfig, Response, RoutingMeta,
        TypedEnvelope,
    };
    use serde_json::Map;
    use uuid::Uuid;

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
                parent_request_id: Some(Uuid::now_v7()),
                priority: Priority::High,
            },
            exec: ExecutionMeta {
                retry_count: 1,
                task_retry_count: 2,
                profile_version: 3,
                trace_id: Some("trace-1".to_string()),
                fence_token: Some(9),
                created_at_ms: 10,
                updated_at_ms: 11,
            },
            config: ResolvedNodeConfig {
                profile_key: "demo:profile:account-a:platform-x:catalog".to_string(),
                profile_version: 3,
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
            login_info: None,
        }
    }

    fn sample_response() -> Response {
        Response {
            id: Uuid::now_v7(),
            platform: "platform-x".to_string(),
            account: "account-a".to_string(),
            module: "catalog".to_string(),
            status_code: 200,
            cookies: Default::default(),
            content: br#"{\"page\":1}"#.to_vec(),
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
            priority: Priority::Normal,
        }
    }

    fn sample_parser_runtime_input() -> SchedulerNodeParserRuntimeInput {
        let response = sample_response();
        SchedulerNodeParserRuntimeInput {
            routing: RoutingMeta {
                namespace: "demo".to_string(),
                account: response.account.clone(),
                platform: response.platform.clone(),
                module: response.module.clone(),
                node_key: "page".to_string(),
                run_id: response.run_id,
                request_id: response.id,
                parent_request_id: Some(response.prefix_request),
                priority: response.priority,
            },
            exec: ExecutionMeta {
                task_retry_count: response.task_retry_times as u32,
                created_at_ms: 10,
                updated_at_ms: 11,
                ..ExecutionMeta::default()
            },
            config: ResolvedNodeConfig {
                profile_key: "demo:profile:account-a:platform-x:catalog".to_string(),
                profile_version: 3,
                common: ResolvedCommonConfig::default(),
                node_config: TypedEnvelope::new(
                    "node.config",
                    1,
                    PayloadCodec::Json,
                    br#"{\"limit\":10}"#.to_vec(),
                ),
            },
            response,
            login_info: None,
        }
    }

    #[test]
    fn generate_runtime_input_round_trips_through_task_payload() {
        let input = sample_runtime_input();
        let payload = encode_generate_runtime_input(&input).expect("encode generate input");
        let decoded = decode_generate_runtime_input(&payload).expect("decode generate input");

        assert_eq!(decoded.routing, input.routing);
        assert_eq!(decoded.exec, input.exec);
        assert_eq!(decoded.config, input.config);
        assert_eq!(decoded.input, input.input);
        assert_eq!(decoded.login_info.is_some(), input.login_info.is_some());
    }

    #[test]
    fn parser_runtime_input_round_trips_through_task_payload() {
        let input = sample_parser_runtime_input();
        let payload = encode_parser_runtime_input(&input).expect("encode parser input");
        let decoded = decode_parser_runtime_input(&payload).expect("decode parser input");

        assert_eq!(decoded.routing, input.routing);
        assert_eq!(decoded.exec, input.exec);
        assert_eq!(decoded.config, input.config);
        assert_eq!(decoded.response.id, input.response.id);
        assert_eq!(decoded.response.content, input.response.content);
    }

    #[test]
    fn request_batch_round_trips_through_task_payload() {
        let requests = vec![
            Request::new("https://example.com/a", "GET"),
            Request::new("https://example.com/b", "POST"),
        ];
        let payload = encode_request_batch_payload(&requests).expect("encode request batch");
        let decoded = decode_request_batch_payload(&payload).expect("decode request batch");

        assert_eq!(decoded.len(), 2);
        assert_eq!(decoded[0].url, requests[0].url);
        assert_eq!(decoded[1].method, requests[1].method);
    }

    #[test]
    fn parser_output_round_trips_through_task_payload() {
        let parsed_data = crate::common::model::ParsedData {
            request_id: Uuid::now_v7(),
            platform: "platform-x".to_string(),
            account: "account-a".to_string(),
            module: "catalog".to_string(),
            meta: Default::default(),
            data: DataType::File(
                FileStore::default()
                    .with_ctx(StoreContext {
                        request_id: Uuid::nil(),
                        platform: "ignored".to_string(),
                        account: "ignored".to_string(),
                        module: "ignored".to_string(),
                        meta: Default::default(),
                        data_middleware: vec![],
                    })
                    .with_name("detail.json")
                    .with_path("/tmp/detail.json")
                    .with_content(br#"{\"ok\":true}"#.to_vec()),
            ),
            data_middleware: vec!["object_store".to_string()],
        };
        let output = NodeParseOutput::default()
            .with_next(NodeDispatch::new(
                "detail",
                NodeInput::new(
                    "detail",
                    TypedEnvelope::new(
                        "node.input",
                        1,
                        PayloadCodec::Json,
                        br#"{\"cursor\":\"next\"}"#.to_vec(),
                    ),
                ),
            ))
            .with_data(parsed_data)
            .finish();
        let payload = encode_parser_output_payload(output).expect("encode parser output");
        let decoded = decode_parser_output_payload(&payload)
            .expect("decode parser output")
            .into_node_parse_output();

        assert_eq!(decoded.next.len(), 1);
        assert_eq!(decoded.next[0].target_node, "detail");
        assert_eq!(decoded.data.len(), 1);
        assert_eq!(decoded.data[0].module, "catalog");
        assert_eq!(decoded.data[0].data_middleware, vec!["object_store".to_string()]);
        match &decoded.data[0].data {
            DataType::File(store) => {
                assert_eq!(store.file_name, "detail.json");
                assert_eq!(store.file_path, "/tmp/detail.json");
                assert_eq!(store.content, br#"{\"ok\":true}"#.to_vec());
            }
            other => panic!("expected file payload, got {other:?}"),
        }
        assert!(decoded.finished);
    }

    #[test]
    fn generate_runtime_input_requires_schema_match() {
        let input = sample_runtime_input();
        let mut payload = encode_generate_runtime_input(&input).expect("encode generate input");
        payload
            .metadata
            .insert("schema".to_string(), "transport.generate_input".to_string());

        assert!(!is_generate_runtime_input(&payload));
        assert!(decode_generate_runtime_input(&payload).is_err());
    }

    #[test]
    fn parser_runtime_input_requires_schema_match() {
        let input = sample_parser_runtime_input();
        let mut payload = encode_parser_runtime_input(&input).expect("encode parser input");
        payload.metadata.remove("version");

        assert!(!is_parser_runtime_input(&payload));
        assert!(decode_parser_runtime_input(&payload).is_err());
    }

    #[test]
    fn build_module_config_generate_runtime_input_matches_context_shape() {
        let input = build_module_config_generate_runtime_input(
            "account-a-platform-x-catalog",
            Uuid::now_v7(),
            "detail",
            crate::common::model::ResolvedCommonConfig::default(),
            &ModuleConfig::default(),
            Map::new(),
            None,
            None,
        );

        assert_eq!(input.routing.account, "account");
        assert_eq!(input.routing.platform, "a");
        assert_eq!(input.routing.module, "platform-x-catalog");
        assert_eq!(input.routing.node_key, "detail");
        assert_eq!(input.input.target_node, "detail");
    }

    #[test]
    fn build_module_config_parse_runtime_input_matches_response_shape() {
        let response = sample_response();
        let input = build_module_config_parse_runtime_input(
            "account-a-platform-x-catalog",
            "page",
            crate::common::model::ResolvedCommonConfig::default(),
            Some(&ModuleConfig::default()),
            &response,
        );

        assert_eq!(input.routing.account, response.account);
        assert_eq!(input.routing.platform, response.platform);
        assert_eq!(input.routing.module, response.module);
        assert_eq!(input.routing.node_key, "page");
        assert_eq!(input.response.id, response.id);
    }
}

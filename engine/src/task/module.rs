use errors::Result;
use log::warn;
use serde::ser::SerializeStruct;
use serde::{Serialize, Serializer};
use serde_json::Map;
use std::sync::Arc;
use uuid::Uuid;
use futures::StreamExt;
use common::interface::{DataMiddleware, DataStoreMiddleware, ModuleTrait, SyncBoxStream};
use common::model::entity::{AccountModel, PlatformModel};
use common::model::login_info::LoginInfo;
use common::model::{Cookies, Headers, ModuleConfig, Response,Request};
use common::model::message::ParserData;
use errors::RequestError;
use crate::task::module_processor_with_chain::ModuleProcessorWithChain;

#[derive(Clone)]
pub struct Module {
    pub config: ModuleConfig,
    pub account: AccountModel,
    pub platform: PlatformModel,
    pub error_times: u32,
    pub finished: bool,
    pub data_middleware: Vec<String>,
    pub download_middleware: Vec<String>,
    pub module: Arc<dyn ModuleTrait>,
    pub locker: bool,
    pub locker_ttl: u64,
    pub processor: ModuleProcessorWithChain,
    pub run_id: Uuid,
    pub prefix_request: Uuid,
    // Optional execution context to drive precise step selection (e.g., retry current step)
    pub pending_ctx: Option<common::model::ExecutionMark>,
}
impl Module {
    pub async fn generate(
        &self,
        task_meta: Map<String, serde_json::Value>,
        login_info: Option<LoginInfo>,
    ) -> Result<SyncBoxStream<'static, Request>> {
        if self.module.should_login() && login_info.is_none() {
            return Err(RequestError::NotLogin("module need login".into()).into());
        }
        let request_stream = self
            .processor
            .execute_request(
                self.config.clone(),
                task_meta.clone(),
                login_info.clone(),
                self.pending_ctx.clone(),
                Some(self.prefix_request),
            )
            .await?;

        // let request_list = self
        //     .module
        //     .generate(
        //         self.config.clone(),
        //         task_meta.clone(),
        //         login_info.clone(),
        //         self.state_handle.clone(),
        //     )
        //     .await?;

        // Capture needed values for the mapping closure.
        let module_name = self.module.name().clone();
        let platform_name = self.platform.name.clone();
        let download_middleware = self.download_middleware.clone();
        let data_middleware = self.data_middleware.clone();
        let account_name = self.account.name.clone();
        let finished = self.finished;
        let limit_id = self
            .config
            .get_config_value("limit_id")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let headers = self.module.headers().await;
        let cookies = self.module.cookies().await;
        let run_id = self.run_id;
        let prefix_request = self.prefix_request;
        let config = self.config.clone();
        let stream = request_stream
            .map(move |mut request| {
                if request.id.is_nil() {
                    request.id = Uuid::now_v7();
                }
                request.module = module_name.clone();
                request.platform = platform_name.clone();
                request.download_middleware = download_middleware.clone();
                request.data_middleware = data_middleware.clone();
                request.account = account_name.clone();
                request.task_finished = finished;
                // inherit run_id from Module/Task so ModuleProcessor and downstream can isolate state per run
                request.run_id = run_id;
                // Set chain prefix from Task/Module level; used to recover previous request on failures
                request.prefix_request = prefix_request;

                if request.headers.is_empty() && !headers.is_empty() {
                    request = request.with_headers(headers.clone());
                }
                if !cookies.is_empty() {
                    request = request.with_cookies(cookies.clone());
                }

                // 添加来自于登录信息的cookies和headers
                // 自动添加UA
                // 如果有登录信息则自动添加UA，没有登录信息需要手动在trait中添加UA
                if let Some(ref info) = login_info {
                    let cookies = Cookies::from(info);
                    let headers = Headers::from(info);
                    request.headers.merge(&headers);
                    request.cookies.merge(&cookies);
                    request = request.with_login_info(info);
                }
                request.limit_id = limit_id.clone().unwrap_or(request.module_id());
                // request.meta = request.meta.add_module_config(&self.config);
                request = request
                    .with_module_config(&config)
                    .with_task_config(task_meta.clone());
                // add downloader from config
                if let Some(downloader) = config.get_config::<String>("downloader") {
                    request.downloader = downloader;
                }
                else {
                    request.downloader = "request_downloader".to_string();
                }
                log::info!("[Module] request prepared: request_id={}", request.id);
                request
            });
        Ok(Box::pin(stream))
    }

    pub async fn add_step(&self) {
        // Run module-level pre_process once before registering steps
        if let Err(e) = self
            .module
            .pre_process(Some(self.config.clone()))
            .await
        {
            // Keep non-breaking: log and continue to allow default/no-op usage
            warn!(
                "module pre_process failed for {}: {}",
                self.module.name(),
                e
            );
        }
        let node = self.module.add_step().await;
        for n in node {
            self.processor.add_step_node(n).await;
        }
    }
    pub async fn parser(
        &self,
        response: Response,
        config: Option<ModuleConfig>,
    ) -> Result<ParserData> {
        // Capture current step to determine if this is the last step
        let current_step = response.context.step_idx.unwrap_or(0) as usize;
        // Clone config for potential post_process
        let cfg_for_post = config.clone();

        let mut data = self.processor.execute_parser(response, config).await?;
        // Enrich returned Data with module/account/platform identifiers
        for d in data.data.iter_mut() {
            d.module = self.module.name();
            d.account = self.account.name.clone();
            d.platform = self.platform.name.clone();
        }


        // If we've reached the last step and there's no further task to advance,
        // trigger the module-level post_process hook.
        let total_steps = self.processor.get_total_steps().await;
        let is_last_step = current_step + 1 >= total_steps && total_steps > 0;
        let no_next_task = data.parser_task.is_none();
        if is_last_step && no_next_task {
            self.module.post_process(cfg_for_post).await?;
            
        }
        Ok(data)
    }

    pub fn id(&self) -> String {
        format!(
            "{}-{}-{}",
            self.account.name,
            self.platform.name,
            self.module.name()
        )
    }

}

impl Serialize for Module {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("Module", 8)?;
        state.serialize_field("config", &self.config)?;
        state.serialize_field("account", &self.account)?;
        state.serialize_field("platform", &self.platform)?;
        state.serialize_field("error_times", &self.error_times)?;
        state.serialize_field("data_middleware", &self.data_middleware)?;
        state.serialize_field("download_middleware", &self.download_middleware)?;
        state.serialize_field("module", &self.module.name())?;
        state.end()
    }
}


pub struct ModuleEntity{
    pub module_work:Arc<dyn ModuleTrait>,
    pub download_middleware:Vec<Arc<dyn ModuleTrait>>,
    pub data_middleware:Vec<Arc<dyn DataMiddleware>>,
    pub store_middleware:Vec<Arc<dyn DataStoreMiddleware>>,
}

impl From<Arc<dyn ModuleTrait>> for ModuleEntity {
    fn from(module: Arc<dyn ModuleTrait>) -> Self {
        ModuleEntity {
            module_work: module,
            download_middleware: vec![],
            data_middleware: vec![],
            store_middleware: vec![],
        }
    }
}
impl ModuleEntity {
    pub fn add_download_middleware(mut self, middleware: Arc<dyn ModuleTrait>)->Self {
        self.download_middleware.push(middleware);
        self
    }

    pub fn add_data_middleware(mut self, middleware: Arc<dyn DataMiddleware>) ->Self{
        self.data_middleware.push(middleware);
        self
    }

    pub fn add_store_middleware(mut self, middleware: Arc<dyn DataStoreMiddleware>)->Self {
        self.store_middleware.push(middleware);
        self
    }
}

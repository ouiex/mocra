use super::{
    assembler::{ConfigAssembler, ModuleAssembler},
    repository::TaskRepository,
    task::Task,
};
use errors::{ModuleError::ModuleNotFound, Result};

use common::model::login_info::LoginInfo;
use common::model::message::{ErrorTaskModel, ParserTaskModel, TaskModel};
use common::model::{ModuleConfig, Response};
use common::state::State;
use cacheable::{CacheAble, CacheService};
use crate::task::module::Module;
use crate::task::module_processor_with_chain::ModuleProcessorWithChain;
use dashmap::DashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use uuid::Uuid;

/// Task factory that materializes Task instances from runtime inputs.
pub struct TaskFactory {
    repository: TaskRepository,
    cache_service: Arc<CacheService>,
    cookie_service: Option<Arc<CacheService>>,
    module_assembler: Arc<tokio::sync::RwLock<ModuleAssembler>>,
    // In-memory cache keyed by task id (account-platform).
    cache: Arc<DashMap<String, CacheEntry>>,
    state: Arc<State>,
}

const CACHE_TTL: Duration = Duration::from_secs(30);

struct CacheEntry {
    task: Arc<Task>,
    expires_at: Instant,
}

impl TaskFactory {
    /// Creates a task factory with repository/cache/assembler dependencies.
    pub fn new(
        repository: TaskRepository,
        sync_service: Arc<CacheService>,
        cookie_sync_service: Option<Arc<CacheService>>,
        module_assembler: Arc<tokio::sync::RwLock<ModuleAssembler>>,
        state: Arc<State>,
    ) -> Self {
        Self {
            repository,
            cache_service: sync_service,
            cookie_service: cookie_sync_service,
            module_assembler,
            cache: Arc::new(DashMap::new()),
            state,
        }
    }

    /// Loads login info from cookie cache service if configured.
    pub async fn login_info(&self, id: &str) -> Option<LoginInfo> {
        if let Some(sync) = self.cookie_service.as_ref(){
            let result = LoginInfo::sync(id, sync).await;
            match result {
                Ok(None) => {
                    let key = <LoginInfo as CacheAble>::cache_id(id, sync);
                    log::warn!("cookie not found in redis: key={}", key);
                }
                Ok(Some(info))=>{
                    return Some(info);
                }
                Err(err) => {
                    log::warn!("cookies load error {}", err);
                }
            }
        }
        log::warn!("cookie service not configured; skip login_info lookup for id={}", id);
        None

    }
    /// Creates task from TaskModel and optionally filters requested modules.
    pub async fn create_task_from_model(&self, task_model: &TaskModel) -> Result<Task> {
        let mut task = (*self
            .create_task_with_modules(&task_model.platform, &task_model.account, task_model.run_id)
            .await?).clone();
        task.run_id = task_model.run_id;
        task.modules.iter_mut().for_each(|m| {
            m.run_id = task_model.run_id;
        });
        if let Some(names) = &task_model.module
            && !names.is_empty() {
                let want: std::collections::HashSet<&str> =
                    names.iter().map(|s| s.as_str()).collect();
                task.modules
                    .retain(|m| want.contains(m.module.name().as_str()));
            }
        Ok(task)
    }

    // Cache key uses Task::id() => account-platform; value stores full task module set.

    /// Builds full task with all enabled modules for one account-platform pair.
    async fn create_task_with_modules(
        &self,
        platform_name: &str,
        account_name: &str,
        run_id: Uuid,
    ) -> Result<Arc<Task>> {
        let start = Instant::now();
        // Fast path: in-memory cache lookup.
        let cache_key = format!("{account_name}-{platform_name}");
        if let Some(cached) = self.get_from_cache(&cache_key).await {
            return Ok(cached);
        }
        log::warn!("create_task_with_modules: cache miss for {}, loading from DB", cache_key);

        // Load all modules available under account-platform relation.
        let modules = self
            .repository
            .load_modules_by_account_platform(platform_name, account_name)
            .await?;

        // Load base account/platform entities.
        let account = self.repository.load_account(account_name).await?;
        let platform = self.repository.load_platform(platform_name).await?;

        // Validate account-platform relation.
        let rel_account_platform = self
            .repository
            .load_account_platform_relation(account.id, platform.id)
            .await?;

        if modules.is_empty() {
            let mut task = Task {
                account,
                platform,
                // error_times: 0,
                login_info: None,
                modules: vec![],
                metadata: Default::default(),
                run_id,
                prefix_request: Default::default(),
            };
            task.login_info = self.login_info(&task.id()).await;
            let task = Arc::new(task);
            // Cache empty-module task as well.
            self.put_task_aliases(task.clone()).await;
            return Ok(task)
        }

        // Batch-load middleware relations.
        let module_ids: Vec<i32> = modules.iter().map(|m| m.id).collect();
        let module_data_middleware_map = self
            .repository
            .load_module_data_middleware_relations(&module_ids)
            .await?;
        let module_download_middleware_map = self
            .repository
            .load_module_download_middleware_relations(&module_ids)
            .await?;

        // Collect middleware ids for bulk entity loading.
        let mut all_data_middleware_ids = std::collections::HashSet::new();
        let mut all_download_middleware_ids = std::collections::HashSet::new();

        for relations in module_data_middleware_map.values() {
            for rel in relations {
                all_data_middleware_ids.insert(rel.data_middleware_id);
            }
        }

        for relations in module_download_middleware_map.values() {
            for rel in relations {
                all_download_middleware_ids.insert(rel.download_middleware_id);
            }
        }

        // Bulk-load middleware entities.
        let all_data_middleware = if !all_data_middleware_ids.is_empty() {
            self.repository
                .load_data_middlewares(&all_data_middleware_ids.into_iter().collect::<Vec<_>>())
                .await?
        } else {
            vec![]
        };

        let all_download_middleware = if !all_download_middleware_ids.is_empty() {
            self.repository
                .load_download_middlewares(
                    &all_download_middleware_ids.into_iter().collect::<Vec<_>>(),
                )
                .await?
        } else {
            vec![]
        };

        // Batch-load module relation maps.
        let module_ids_list: Vec<i32> = modules.iter().map(|m| m.id).collect();
        let rel_module_platform_map = self
            .repository
            .load_module_platform_relations(&module_ids_list, platform.id)
            .await?;
        let rel_module_account_map = self
            .repository
            .load_module_account_relations(&module_ids_list, account.id)
            .await?;

        // Materialize runtime module instances.
        let mut module_instances = Vec::new();
        for module in modules {
            // Resolve preloaded relation data for current module.
            let rel_module_platform = match rel_module_platform_map.get(&module.id) {
                Some(r) => r.clone(),
                None => {
                    log::warn!("Missing platform relation for module {}", module.id);
                    continue;
                }
            };
            let rel_module_account = match rel_module_account_map.get(&module.id) {
                Some(r) => r.clone(),
                None => {
                     log::warn!("Missing account relation for module {}", module.id);
                     continue;
                }
            };

            let rel_module_data_middleware = module_data_middleware_map
                .get(&module.id)
                .cloned()
                .unwrap_or_default();
            let rel_module_download_middleware = module_download_middleware_map
                .get(&module.id)
                .cloned()
                .unwrap_or_default();

            // Filter middlewares linked to this module.
            let data_middleware: Vec<_> = all_data_middleware
                .iter()
                .filter(|m| {
                    rel_module_data_middleware
                        .iter()
                        .any(|rel| rel.data_middleware_id == m.id)
                })
                .cloned()
                .collect();

            let download_middleware: Vec<_> = all_download_middleware
                .iter()
                .filter(|m| {
                    rel_module_download_middleware
                        .iter()
                        .any(|rel| rel.download_middleware_id == m.id)
                })
                .cloned()
                .collect();

            // Assemble effective module config.
            let module_config = ConfigAssembler::assemble_module_config(
                &account,
                &platform,
                &module,
                &rel_account_platform,
                &rel_module_platform,
                &rel_module_account,
                &data_middleware,
                &download_middleware,
                &rel_module_data_middleware,
                &rel_module_download_middleware,
            );

            // Build runtime module instance.
            let assembler = self.module_assembler.read().await;
            let module_assembler = match assembler.get_module(&module.name) {
                Some(module) => module,
                None => continue,
            };
            if module_assembler.version() != module.version {
                continue;
            }

            let locker = module_config
                .get_config_value("module_locker")
                .and_then(|v| v.as_bool())
                .unwrap_or(false);
            let cache_ttl = self.state.config.read().await.cache.ttl;
            let module_instance = Module {
                config: Arc::new(module_config),
                account: account.clone(),
                platform: platform.clone(),
                error_times: 0,
                finished: false,
                data_middleware: data_middleware.iter().map(|x| x.name.clone()).collect(),
                download_middleware: download_middleware.iter().map(|x| x.name.clone()).collect(),
                module: module_assembler,
                locker,
                locker_ttl: 0,
                processor: ModuleProcessorWithChain::new(
                    format!("{}-{}-{}", account.name, platform.name, module.name),
                    self.state.cache_service.clone(),
                    run_id,
                    cache_ttl
                ),
                run_id,
                prefix_request: Default::default(),
                pending_ctx: None,
                bound_task_meta: None,
                bound_login_info: None,
            };
            module_instance.add_step().await;

            // StateTrait capability checks are no longer required at this layer.

            module_instances.push(module_instance);
        }
        let mut task = Task {
            account,
            platform,
            // error_times: 0,
            login_info: None,
            modules: module_instances,
            metadata: Default::default(),
            run_id,
            prefix_request: Default::default(),
        };
        task.login_info = self.login_info(&task.id()).await;
        let task = Arc::new(task);
        // Cache task by account-platform key.
        self.put_task_aliases(task.clone()).await;
        log::warn!("create_task_with_modules: loaded from DB for {}, took {:?}", cache_key, start.elapsed());
        Ok(task)
    }

    /// Loads task from TaskModel and synchronizes initial runtime state.
    pub async fn load_with_model(&self, task_model: &TaskModel) -> Result<Task> {
        let task = self.create_task_from_model(task_model).await;
        match task {
            Ok(mut task) => {
                // Task status synchronization placeholder.
                // task.error_times = self.sync_service.load_task_status(&task.id()).await;
                // self.sync_service
                //     .sync_task_status(&task.id(), task.error_times)
                //     .await?;

                // Sync module runtime markers and config.
                task.prefix_request = Uuid::nil();
                for module in task.modules.iter_mut() {
                    // let (error_times, finished) =
                    //     self.cache_service.load_module_status(&module.id()).await;
                    // module.error_times = error_times;
                    // module.finished = finished;
                    module.prefix_request = Uuid::nil();
                    // Sync module config to cache.
                    module
                        .config
                        .send(&module.id(), &self.cache_service)
                        .await
                        .ok();

                    // Module state synchronization placeholder.
                    // self.cache_service
                    //     .sync_module_status(&module.id(), module.error_times, module.finished)
                    //     .await
                    //     .ok();
                }
                Ok(task)
            }
            Err(e) => Err(ModuleNotFound(
                format!(
                    "{}-{}-{:?} not found with error: {}",
                    task_model.platform, task_model.account, task_model.module, e
                )
                    .into(),
            ))?,
        }
    }

    /// Loads task from ParserTaskModel and restores parser-driven context.
    pub async fn load_parser_model(&self, parser_model: &ParserTaskModel) -> Result<Task> {
        let mut task = self
            .create_task_from_model(&parser_model.account_task)
            .await?;
        task.prefix_request = parser_model.prefix_request;
        // Ensure run_id strictly inherits from the incoming ParserTaskModel (source of truth)
        task.run_id = parser_model.run_id;
        task.modules
            .iter_mut()
            .for_each(|m| m.run_id = parser_model.run_id);

        // Restore historical metadata and parser progression context.
        // task.error_times = self.sync_service.load_task_status(&task.id()).await;
        task.metadata = parser_model
            .metadata
            .as_object()
            .cloned()
            .unwrap_or_default();
        for module in task.modules.iter_mut() {
            // let (error_times, _) = self.cache_service.load_module_status(&module.id()).await;
            // module.error_times = error_times;
            module.prefix_request = parser_model.prefix_request;
            // Prefer explicit context from parser_model if provided
            module.pending_ctx = Some(parser_model.context.clone());
            // Try to refresh module config from cache storage.
            if let Ok(Some(config)) =
                ModuleConfig::sync(&module.id(), &self.cache_service).await
            {

                module.config = Arc::new(config);
            }
        }
        Ok(task)
    }

    /// Loads task from ErrorTaskModel and propagates retry context.
    pub async fn load_error_model(&self, error_model: &ErrorTaskModel) -> Result<Task> {
        let mut task = self
            .create_task_from_model(&error_model.account_task)
            .await?;
        task.prefix_request = error_model.prefix_request;
        // Ensure run_id strictly inherits from the incoming ErrorTaskModel
        task.run_id = error_model.run_id;
        task.modules.iter_mut().for_each(|m| {
            m.run_id = error_model.run_id;
            m.prefix_request = error_model.prefix_request;
            // Drive precise retry via ExecutionMark from error context
            m.pending_ctx = Some(error_model.context.clone());
        });

        // Task error accounting placeholder.
        // task.error_times = self.sync_service.load_task_status(&task.id()).await;
        // task.error_times += 1;
        // self.sync_service
        //     .sync_task_status(&task.id(), task.error_times)
        //     .await?;

        // Task threshold check placeholder.
        // if task.error_times > self.state.config.read().await.crawler.task_max_errors {
        //     return Err(ModuleError::TaskMaxError(
        //         format!(
        //             "Task {}-{} error times exceed limit",
        //             task.account.name, task.platform.name
        //         )
        //         .into(),
        //     )
        //     .into());
        // }

        // Module error accounting placeholder.
        // for module in task.crawler.iter_mut() {
        //     let (error_times, finished) = self.sync_service.load_module_status(&module.id()).await;
        //     module.error_times = error_times + 1;
        //     module.finished = finished;
        //
        //     self.sync_service
        //         .sync_module_status(&module.id(), module.error_times, module.finished)
        //         .await?;
        // }

        // Module filtering by error threshold placeholder.
        // let max_errors = self.state.config.read().await.crawler.task_max_errors;
        // task.crawler.retain(|m| m.error_times < max_errors);
        task.metadata = error_model
            .metadata
            .as_object()
            .cloned()
            .unwrap_or_default();
        Ok(task)
    }

    pub async fn load_with_response(&self, response: &Response) -> Result<Task> {
        // Load full task (cache-aware) and keep only target module.
        self.create_task_with_modules(&response.platform, &response.account, response.run_id)
            .await
            .map(|t| {
                let mut t = (*t).clone();
                t.modules.retain(|m| m.module.name() == response.module);
                t
            })
    }

    pub async fn load_module_with_response(&self, response: &Response) -> Result<(Arc<Module>, Option<LoginInfo>)> {
        let task = self.create_task_with_modules(&response.platform, &response.account, response.run_id).await?;
        if let Some(module) = task.modules.iter().find(|m| m.module.name() == response.module) {
            Ok((Arc::new(module.clone()), task.login_info.clone()))
        } else {
            Err(ModuleNotFound(format!("Module {} not found in task", response.module).into()).into())
        }
    }

    // Returns cached task if entry is fresh; expired entries are eagerly removed.
    async fn get_from_cache(&self, key: &str) -> Option<Arc<Task>> {
        // Fast read-path check.
        if let Some(entry) = self.cache.get(key) {
            if Instant::now() < entry.expires_at {
                // OPTIMIZATION: Do not refresh login_info on every hit. Respect the cache TTL.
                // task.login_info = self.login_info(&task.id()).await;
                return Some(entry.task.clone());
            } else {
                // Expired
                drop(entry); // Drop read lock before removing
                self.cache.remove(key);
                return None;
            }
        }
        None
    }
    pub async fn clear_cache(&self) {
        self.cache.clear();
    }

    // Inserts task aliases into cache (currently a single task.id key).
    async fn put_task_aliases(&self, task: Arc<Task>) {
        let entry = CacheEntry {
            task: task.clone(),
            expires_at: Instant::now() + CACHE_TTL,
        };
        self.cache.insert(task.id(), entry);
    }
}

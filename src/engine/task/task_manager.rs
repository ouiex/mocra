#![allow(unused)]
use super::{
    assembler::ModuleAssembler,
    factory::{TaskFactory, TaskFactoryConfig},
    module::Module,
    repository::TaskRepository,
    task::Task,
};
use crate::cacheable::{CacheAble, CacheService};
use crate::common::interface::ModuleTrait;
use crate::common::model::login_info::LoginInfo;
use crate::common::model::message::TaskEvent;
use crate::common::model::{NodeDispatchEnvelope, NodeErrorEnvelope, Request, Response};
use crate::common::state::State;
use crate::engine::task::module_dag_orchestrator::{
    ModuleDagOrchestrator, ModuleDagOrchestratorOptions,
};
use crate::errors::Result;
use crate::schedule::dag::{Dag, DagError};
use dashmap::DashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModuleDagCutoverGateState {
    pub module_name: String,
    pub failure_streak: usize,
    pub last_failure_ms: Option<u64>,
    pub blocked: bool,
}

pub struct TaskManager {
    factory: TaskFactory,
    pub cache_service: Arc<CacheService>,
    module_assembler: Arc<RwLock<ModuleAssembler>>,
    compiled_dags: Arc<DashMap<String, Dag>>,
    dag_cutover_tracker: DagCutoverStateTracker,
}

#[derive(Debug, Clone, Copy)]
struct DagCutoverFailureState {
    streak: usize,
    last_failure_ms: u64,
}

#[derive(Debug, Clone, Copy)]
struct DagCutoverWarmupState {
    matched_count: usize,
    first_match_ms: u64,
    last_match_ms: u64,
}

#[derive(Clone)]
struct DagCutoverStateTracker {
    failures: Arc<DashMap<String, DagCutoverFailureState>>,
    warmup: Arc<DashMap<String, DagCutoverWarmupState>>,
    now_ms_provider: Arc<dyn Fn() -> u64 + Send + Sync>,
}

impl DagCutoverStateTracker {
    fn new(now_ms_provider: Arc<dyn Fn() -> u64 + Send + Sync>) -> Self {
        Self {
            failures: Arc::new(DashMap::new()),
            warmup: Arc::new(DashMap::new()),
            now_ms_provider,
        }
    }

    fn now_ms() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64
    }

    fn current_ms(&self) -> u64 {
        (self.now_ms_provider)()
    }

    fn should_allow_cutover(
        &self,
        scope_key: &str,
        failure_threshold: usize,
        recovery_window_secs: u64,
    ) -> bool {
        let threshold = failure_threshold.max(1);
        let Some(state) = self.failures.get(scope_key).map(|v| *v) else {
            return true;
        };

        if state.streak < threshold {
            return true;
        }

        let recovery_window_ms = recovery_window_secs.saturating_mul(1000);
        if recovery_window_ms == 0 {
            return false;
        }

        let elapsed_ms = self.current_ms().saturating_sub(state.last_failure_ms);
        if elapsed_ms >= recovery_window_ms {
            // Cooldown elapsed: clear streak and allow one probing cutover attempt.
            self.failures.remove(scope_key);
            return true;
        }

        false
    }

    fn record_cutover_failure(&self, scope_key: &str) {
        self.failures
            .entry(scope_key.to_string())
            .and_modify(|v| {
                v.streak += 1;
                v.last_failure_ms = self.current_ms();
            })
            .or_insert(DagCutoverFailureState {
                streak: 1,
                last_failure_ms: self.current_ms(),
            });
    }

    fn record_shadow_compare_result(&self, scope_key: &str, compare_result: &str) {
        if compare_result == "match" {
            self.warmup
                .entry(scope_key.to_string())
                .and_modify(|v| {
                    v.matched_count += 1;
                    v.last_match_ms = self.current_ms();
                })
                .or_insert_with(|| {
                    let now = self.current_ms();
                    DagCutoverWarmupState {
                        matched_count: 1,
                        first_match_ms: now,
                        last_match_ms: now,
                    }
                });
            return;
        }

        // Any mismatch or shadow error resets warmup accumulation.
        self.warmup.remove(scope_key);
    }

    fn should_allow_cutover_warmup(
        &self,
        scope_key: &str,
        min_shadow_matches: usize,
        min_observation_window_secs: u64,
    ) -> bool {
        let required_matches = min_shadow_matches.max(1);
        let Some(state) = self.warmup.get(scope_key).map(|v| *v) else {
            return false;
        };

        if state.matched_count < required_matches {
            return false;
        }

        let window_ms = min_observation_window_secs.saturating_mul(1000);
        if window_ms == 0 {
            return true;
        }

        let elapsed_ms = self.current_ms().saturating_sub(state.first_match_ms);
        elapsed_ms >= window_ms
    }

    fn record_cutover_success(&self, scope_key: &str) {
        self.failures.remove(scope_key);
    }

    fn failure_state(&self, scope_key: &str) -> Option<DagCutoverFailureState> {
        self.failures.get(scope_key).map(|entry| *entry)
    }
}

impl Default for DagCutoverStateTracker {
    fn default() -> Self {
        Self::new(Arc::new(Self::now_ms))
    }
}

impl TaskManager {
    /// Creates a task manager with repository, factory, and module assembler wiring.
    pub fn new(state: Arc<State>) -> Self {
        let repository = TaskRepository::new((*state.db).clone());

        let module_assembler = Arc::new(RwLock::new(ModuleAssembler::new()));
        let factory = TaskFactory::new(TaskFactoryConfig {
            repository,
            cache_service: Arc::clone(&state.cache_service),
            cookie_service: state.cookie_service.clone(),
            module_assembler: Arc::clone(&module_assembler),
            state: Arc::clone(&state),
        });

        Self {
            factory,
            cache_service: Arc::clone(&state.cache_service),
            module_assembler,
            compiled_dags: Arc::new(DashMap::new()),
            dag_cutover_tracker: DagCutoverStateTracker::default(),
        }
    }

    /// Registers one module implementation and pre-compiles its DAG.
    pub async fn add_module(&self, work: Arc<dyn ModuleTrait>) {
        let name = work.name();
        {
            let mut assembler = self.module_assembler.write().await;
            assembler.register_module(work.clone());
        }
        self.precompile_module_dag(name, work).await;
    }

    /// Registers multiple module implementations and pre-compiles their DAGs.
    pub async fn add_modules(&self, works: Vec<Arc<dyn ModuleTrait>>) {
        {
            let mut assembler = self.module_assembler.write().await;
            for work in &works {
                assembler.register_module(work.clone());
            }
        }
        for work in works {
            let name = work.name();
            self.precompile_module_dag(name, work).await;
        }
    }

    /// Pre-compiles and caches a module DAG via `compile_module`.
    async fn precompile_module_dag(&self, name: &str, module: Arc<dyn ModuleTrait>) {
        match self.dag_orchestrator().compile_module(module).await {
            Ok(dag) => {
                self.compiled_dags.insert(name.to_string(), dag);
            }
            Err(err) => {
                log::warn!(
                    "[TaskManager] module DAG pre-compile failed: module={} err={}",
                    name,
                    err
                );
            }
        }
    }

    /// Returns the pre-compiled DAG for a registered module, if available.
    pub fn get_module_dag(&self, module_name: &str) -> Option<Dag> {
        self.compiled_dags.get(module_name).map(|v| v.clone())
    }
    /// Returns a registered module trait by name, if present.
    pub async fn get_module(&self, name: &str) -> Option<Arc<dyn ModuleTrait>> {
        let assembler = self.module_assembler.read().await;
        assembler.get_module(name)
    }
    /// Returns true when module name is registered.
    pub async fn exists_module(&self, name: &str) -> bool {
        let assembler = self.module_assembler.read().await;
        assembler.get_module(name).is_some()
    }
    /// Unregisters module by name and removes its cached DAG.
    pub async fn remove_work(&self, name: &str) {
        let mut assembler = self.module_assembler.write().await;
        assembler.remove_module(name);
        drop(assembler);
        self.compiled_dags.remove(name);
    }
    /// Removes all modules loaded from a given origin path.
    pub async fn remove_by_origin(&self, origin: &std::path::Path) {
        let mut assembler = self.module_assembler.write().await;
        assembler.remove_by_origin(origin);
    }
    /// Returns all registered module names.
    pub async fn module_names(&self) -> Vec<String> {
        let assembler = self.module_assembler.read().await;
        assembler.module_names()
    }
    /// Tags module names with origin path for hot-reload bookkeeping.
    pub async fn set_origin(&self, names: &[String], origin: &std::path::Path) {
        let mut assembler = self.module_assembler.write().await;
        assembler.set_origin(names, origin);
    }
    /// Loads task from TaskModel and synchronizes initial state.
    pub async fn load_with_model(&self, task_model: &TaskEvent) -> Result<Task> {
        self.factory.load_with_model(task_model).await
    }

    /// Loads task from parser dispatch envelope with historical state restoration.
    pub async fn load_parser_dispatch(&self, dispatch: &NodeDispatchEnvelope) -> Result<Task> {
        self.factory.load_parser_dispatch(dispatch).await
    }

    /// Loads task from error envelope and applies error accounting.
    pub async fn load_error_envelope(&self, envelope: &NodeErrorEnvelope) -> Result<Task> {
        self.factory.load_error_envelope(envelope).await
    }

    /// Loads task context from response metadata.
    pub async fn load_with_response(&self, response: &Response) -> Result<Task> {
        self.factory.load_with_response(response).await
    }

    /// Loads resolved module and optional login info for parser flow.
    pub async fn load_module_with_response(
        &self,
        response: &Response,
    ) -> Result<(Arc<Module>, Option<LoginInfo>)> {
        self.factory.load_module_with_response(response).await
    }

    /// Clears internal factory caches.
    pub async fn clear_factory_cache(&self) {
        self.factory.clear_cache().await;
    }

    /// Returns all registered module implementations.
    pub async fn get_all_modules(&self) -> Vec<Arc<dyn ModuleTrait>> {
        let assembler = self.module_assembler.read().await;
        assembler.get_all_modules()
    }

    /// Returns a default module DAG orchestrator.
    pub fn dag_orchestrator(&self) -> ModuleDagOrchestrator {
        ModuleDagOrchestrator::new(ModuleDagOrchestratorOptions::default())
    }

    /// Builds the canonical cutover scope key for one module runtime.
    pub fn module_cutover_scope(module: &Module) -> String {
        module.id()
    }

    /// Compiles a module DAG by module name using the unified `compile_module` strategy.
    ///
    /// - custom `dag_definition()` only → compile custom DAG.
    /// - `add_step()` only → compile linear DAG.
    /// - both present → merge into one multi-route DAG.
    pub async fn compile_module_dag(
        &self,
        module_name: &str,
    ) -> std::result::Result<Dag, DagError> {
        let assembler = self.module_assembler.read().await;
        let module = assembler
            .get_module(module_name)
            .ok_or_else(|| DagError::NodeNotFound(module_name.to_string()))?;
        drop(assembler);

        self.dag_orchestrator().compile_module(module).await
    }

    /// Returns whether cutover is allowed under current failure streak.
    pub fn should_allow_module_dag_cutover(
        &self,
        module_name: &str,
        failure_threshold: usize,
        recovery_window_secs: u64,
    ) -> bool {
        self.dag_cutover_tracker.should_allow_cutover(
            module_name,
            failure_threshold,
            recovery_window_secs,
        )
    }

    /// Records one cutover failure for module.
    pub fn record_module_dag_cutover_failure(&self, module_name: &str) {
        self.dag_cutover_tracker.record_cutover_failure(module_name);
    }

    /// Records shadow-compare result used by cutover warmup gating.
    pub fn record_module_dag_shadow_compare_result(&self, module_name: &str, compare_result: &str) {
        self.dag_cutover_tracker
            .record_shadow_compare_result(module_name, compare_result);
    }

    /// Returns whether warmup gate is satisfied for cutover.
    pub fn should_allow_module_dag_cutover_warmup(
        &self,
        module_name: &str,
        min_shadow_matches: usize,
        min_observation_window_secs: u64,
    ) -> bool {
        self.dag_cutover_tracker.should_allow_cutover_warmup(
            module_name,
            min_shadow_matches,
            min_observation_window_secs,
        )
    }

    /// Clears cutover failure streak after a successful execution.
    pub fn record_module_dag_cutover_success(&self, module_name: &str) {
        self.dag_cutover_tracker.record_cutover_success(module_name);
    }

    /// Returns cutover gate state snapshot for observability and control-plane diagnostics.
    pub fn module_dag_cutover_gate_state(
        &self,
        module_name: &str,
        failure_threshold: usize,
        recovery_window_secs: u64,
    ) -> ModuleDagCutoverGateState {
        let failure_state = self.dag_cutover_tracker.failure_state(module_name);
        let blocked = !self.should_allow_module_dag_cutover(
            module_name,
            failure_threshold,
            recovery_window_secs,
        );

        ModuleDagCutoverGateState {
            module_name: module_name.to_string(),
            failure_streak: failure_state.map(|state| state.streak).unwrap_or(0),
            last_failure_ms: failure_state.map(|state| state.last_failure_ms),
            blocked,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{DagCutoverStateTracker, ModuleDagCutoverGateState};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};

    fn tracker_with_time(time: Arc<AtomicU64>) -> DagCutoverStateTracker {
        DagCutoverStateTracker::new(Arc::new(move || time.load(Ordering::SeqCst)))
    }

    #[test]
    fn cutover_failure_isolation_by_scope_key() {
        let tracker = DagCutoverStateTracker::default();
        let scope_a = "acc-a-pf-a-same_module";
        let scope_b = "acc-b-pf-b-same_module";

        tracker.record_cutover_failure(scope_a);
        tracker.record_cutover_failure(scope_a);

        assert!(!tracker.should_allow_cutover(scope_a, 2, 0));
        assert!(tracker.should_allow_cutover(scope_b, 2, 0));
    }

    #[test]
    fn warmup_isolation_and_reset_by_scope_key() {
        let tracker = DagCutoverStateTracker::default();
        let scope_a = "acc-a-pf-a-same_module";
        let scope_b = "acc-b-pf-b-same_module";

        tracker.record_shadow_compare_result(scope_a, "match");
        tracker.record_shadow_compare_result(scope_a, "match");
        assert!(tracker.should_allow_cutover_warmup(scope_a, 2, 0));
        assert!(!tracker.should_allow_cutover_warmup(scope_b, 2, 0));

        tracker.record_shadow_compare_result(scope_a, "mismatch");
        assert!(!tracker.should_allow_cutover_warmup(scope_a, 1, 0));
    }

    #[test]
    fn cutover_recovery_window_allows_probe_after_cooldown() {
        let now = Arc::new(AtomicU64::new(1_000));
        let tracker = tracker_with_time(now.clone());
        let scope = "acc-a-pf-a-module-x";

        tracker.record_cutover_failure(scope);
        tracker.record_cutover_failure(scope);
        assert!(!tracker.should_allow_cutover(scope, 2, 5));

        now.store(5_999, Ordering::SeqCst);
        assert!(!tracker.should_allow_cutover(scope, 2, 5));

        now.store(6_000, Ordering::SeqCst);
        assert!(tracker.should_allow_cutover(scope, 2, 5));
        assert!(tracker.should_allow_cutover(scope, 2, 5));
    }

    #[test]
    fn warmup_observation_window_requires_elapsed_time() {
        let now = Arc::new(AtomicU64::new(10_000));
        let tracker = tracker_with_time(now.clone());
        let scope = "acc-a-pf-a-module-y";

        tracker.record_shadow_compare_result(scope, "match");
        tracker.record_shadow_compare_result(scope, "match");
        assert!(!tracker.should_allow_cutover_warmup(scope, 2, 3));

        now.store(12_999, Ordering::SeqCst);
        assert!(!tracker.should_allow_cutover_warmup(scope, 2, 3));

        now.store(13_000, Ordering::SeqCst);
        assert!(tracker.should_allow_cutover_warmup(scope, 2, 3));
    }

    #[test]
    fn cutover_gate_state_reports_streak_and_blocked_status() {
        let now = Arc::new(AtomicU64::new(100));
        let tracker = tracker_with_time(now.clone());
        let scope = "acc-pf-mod";

        tracker.record_cutover_failure(scope);
        tracker.record_cutover_failure(scope);

        let failure_state = tracker
            .failure_state(scope)
            .expect("failure state should exist");
        assert_eq!(failure_state.streak, 2);
        assert_eq!(failure_state.last_failure_ms, 100);

        let snapshot = ModuleDagCutoverGateState {
            module_name: scope.to_string(),
            failure_streak: failure_state.streak,
            last_failure_ms: Some(failure_state.last_failure_ms),
            blocked: !tracker.should_allow_cutover(scope, 2, 60),
        };

        assert_eq!(snapshot.module_name, scope);
        assert_eq!(snapshot.failure_streak, 2);
        assert!(snapshot.blocked);
    }
}

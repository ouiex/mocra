# mocra

A distributed, event-driven crawling and data collection framework for Rust.

## Simple


## Documentation
- version 0.2.0

- Major updates (v0.2.0):
	- Distributed DAG execution is now a first-class runtime capability.
	- ModuleTrait now supports DAG-oriented execution, with linear compatibility kept for existing modules.

- Distributed DAG runtime (v0.2.0):
	- Added distributed DAG scheduler capabilities for parallel and resilient execution.
	- Added remote dispatch and worker model for cross-node DAG node execution.
	- Added run guard and fencing token protection for safer distributed consistency.
	- Added run-state checkpoint and resume support for failure recovery.
	- Added singleflight/idempotency controls to reduce duplicate remote execution.

- ModuleTrait DAG support (v0.2.0):
	- Added DAG-oriented module orchestration integration in engine task flow.
	- Added linear-compatible DAG compile/execute path for incremental migration.
	- Added shadow/preview/cutover governance hooks for gradual rollout.

- Validation and rollout confidence (v0.2.0):
	- Added end-to-end coverage for Engine-driven and task-queue-driven DAG + ModuleTrait flows.
	- Added dual-node and single-node runtime regression coverage for critical execution paths.

- Previous release notes:
	- Config precedence fix (v0.1.5):
		- Effective config now consistently follows: ORM/module config > config.toml > hardcoded default.
		- Fixed fallback behavior for `module_locker` and `wss_timeout` when ORM value is missing.
		- Fixed boolean fallback in download config loading to inherit from config.toml when module value is absent.

	- Middleware interface update (v0.1.2):
		- `DownloadMiddleware` / `DataMiddleware` / `DataStoreMiddleware` methods now use `&mut self`.
		- Added store lifecycle hooks in `DataStoreMiddleware`:
			- `before_store(&mut self, _config: &Option<ModuleConfig>) -> Result<()>`
			- `after_store(&mut self, _config: &Option<ModuleConfig>) -> Result<()>`
		- Store execution order is now unified as:
			1. `before_store`
			2. `store_data`
			3. `after_store`
		- If `before_store` fails, `store_data` and `after_store` are skipped and the middleware enters existing error/retry flow.

	- ParserData update (v0.1.1): `ParserData.parser_task` changed from `Option<ParserTaskModel>` to `Vec<ParserTaskModel>`.
		- Parser can now return multiple next parser tasks in a single round.
		- Existing `with_task(...)` remains available (now appends to the vector).
		- Recommended check for "has next task": use `!parser_task.is_empty()`.
    
- Project docs: `docs/README.md`
- DAG docs index: `docs/dag/DAG_GUIDE.md`
- DAG API reference: `docs/dag/DAG_API_REFERENCE.md`
- DAG runbook: `docs/dag/DAG_RUNBOOK.md`
- API docs (after publish): <https://docs.rs/mocra>

## License

Licensed under either of:

- MIT license
- Apache License, Version 2.0

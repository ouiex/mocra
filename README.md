# mocra

A distributed, event-driven crawling and data collection framework for Rust.

## Documentation
- version 0.1.2

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
- API docs (after publish): <https://docs.rs/mocra>

## License

Licensed under either of:

- MIT license
- Apache License, Version 2.0

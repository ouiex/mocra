# mocra

A distributed, event-driven crawling and data collection framework for Rust.

## Documentation
- version 0.1.1 

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

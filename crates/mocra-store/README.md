# mocra-store

Multi-tenant **sea-orm entity models** for the [mocra](https://github.com/ouiex/mocra)
distributed crawler framework: the `account × platform × module` model plus data / download
middleware and relation tables, task results, and logs.

Pure data layer (sea-orm + serde, zero engine coupling), with slim features (no runtime /
sqlx driver) so it can be depended on for the entity types alone. The host `mocra` crate
uses it behind its `store` feature.

## Contents

- `entity::{Account, Platform, Module, TaskResult, Log, DataMiddleware, DownloadMiddleware}`
  and the `rel_*` relation entities (sea-orm `Entity` / `Model` / `Column`).

Part of the [mocra](https://github.com/ouiex/mocra) workspace.

## License

Licensed under either of MIT or Apache-2.0 at your option.

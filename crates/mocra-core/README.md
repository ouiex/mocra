# mocra-core

Shared runtime types for the [mocra](https://github.com/ouiex/mocra) crawler framework.

Currently this crate hosts the framework's error types (`mocra_core::errors`). The rest of the
shared runtime (domain models, cache service, the crawling pipeline) is being migrated here
incrementally so the host `mocra` crate can become a thin facade over reusable,
independently-compilable crates.

Not intended for direct use yet — depend on `mocra` instead.

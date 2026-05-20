# Pack 8 Priority Decision

## Scope

This note freezes the implementation order for the three deferred target-state items that were re-opened before Pack 8:

1. QueueManager native compensation
2. Unified CacheService convergence
3. Federation Download Pool

The goal is not to restate the target architecture. The goal is to decide what must enter the active backlog first, what must stay behind rollback anchors, and what should remain deferred until the control-plane cutover is more stable.

## Decision Criteria

The ordering is based on three concrete criteria:

1. Whether the item blocks removal of Redis-era compatibility from the current main path.
2. Whether the item changes hot-path assembly across State, Engine, and scheduler at the same time.
3. Whether the item can be introduced with a narrow rollback surface and focused regression coverage.

## Frozen Priority Order

| Priority | Item | Decision |
|----------|------|----------|
| P0 | QueueManager native compensation | Start first. This is the only item that still sits directly in the current queue consume/ack path and still depends on `RedisCompensator`. |
| P1 | Unified CacheService convergence | Do second, but keep it as a separate batch after compensation stabilizes. This is foundational, but the blast radius is much larger because `State` still assembles lock, limiter, cache, and status tracking separately. |
| P2 | Federation Download Pool | Defer until after the control-plane/cache convergence. It is an important target-state capability, but it does not currently block Pack 8 compatibility cleanup. |

## Rationale

### P0. QueueManager native compensation

This moves first because it is the narrowest remaining Redis-era compatibility slice that still participates in the live queue processing path:

- `QueueManager::from_channel_config(...)` still initializes `RedisCompensator` from `channel_config.compensator`.
- queue subscription handling still calls `comp.add_task(...)` before local processing.
- processor success paths and zombie cleanup still call `comp.remove_task(...)`.

That means compensation is still a direct dependency for removing Redis-shaped queue compatibility. Converting it to queue-native Begin/Done events removes a real hot-path compatibility edge without forcing a full control-plane rewrite in the same batch.

### P1. Unified CacheService convergence

This remains the architectural foundation for the target control plane, but it should not be mixed into the same batch as compensation cleanup:

- `State::new(...)` still wires `DistributedLockManager`, `DistributedSlidingWindowRateLimiter`, `CacheService`, and `StatusTracker` as separate runtime pieces.
- downloader, limiter, lock, status, and cache behavior still share Redis-backed assumptions and namespace-local wiring.
- the target `CacheBackend` shape is already documented, but the current runtime assembly has a wide blast radius.

The decision is to keep this second: important, but intentionally isolated into its own migration window after queue-native compensation has regression evidence and rollback anchors.

The first batch should stay narrow:

- converge `State` assembly behind one internal coordination-services builder before changing runtime behavior;
- keep `CacheService`, lock, limiter, API limiter, and status tracker public fields unchanged;
- validate the batch with focused `common::state` regression coverage before moving on to backend-level convergence.

### P2. Federation Download Pool

This stays third because it is more of a target-state expansion than a current Pack 8 blocker:

- the architecture already defines download as the cross-namespace exception, but current code still uses local `RequestDownloader` assembly.
- the remaining prerequisites are not just downloader routing; they also include `Response.namespace` return routing and the unfinished remote cache retrieval path.
- it does not currently block scheduler ingress cutover, Pack 8 legacy cleanup, or rollback rehearsal.

The decision is to keep Download Pool design frozen, but defer active implementation until the control-plane and cache convergence path is less volatile.

## Execution Rule

Pack 8 should therefore consume these items in the following way:

1. Pull QueueManager native compensation into the active cleanup backlog.
2. Keep Unified CacheService as the next planned pack, but do not mix it into the same deletion batch.
3. Keep Federation Download Pool out of the Pack 8 critical path; only revisit it after compensation and cache convergence are stable.

## Rollback Guidance

- Native compensation rollout must keep the existing Redis compensator path as a temporary rollback anchor until Begin/Done replay behavior is verified.
- CacheService convergence must keep batch-local rollback anchors because it rewires lock, limiter, tracker, and cache together.
- Federation Download Pool should not begin until rollback ownership for `Response.namespace` and remote cache retrieval is explicit.

## Current Status

- 2026-04-17: the queue hot path now defaults to `QueueNativeCompensator` whenever a remote queue backend is active.
- The current batch only replaces the Redis side-record dependency used by `add_task/remove_task`; it does not yet land `CompensationReplayer` or Begin/Done topic replay.
- `RedisCompensator` remains in the codebase as the temporary rollback anchor while the native path is validated.
- 2026-04-17: the first `Unified CacheService convergence` batch is now in place inside `State`: `CacheService`, cookie cache, distributed lock, download/API limiters, and `StatusTracker` are assembled through a single internal `CoordinationServices::build(...)` path.
- Focused regression evidence for that batch: `cargo test --lib common::state::tests -- --nocapture`.
- 2026-04-17: a first Federation Download Pool prerequisite batch is in place at the transport edge: request/response dispatch now preserves the origin namespace through the envelope round-trip, so download workers no longer have to infer response ownership from their local namespace.
- 2026-04-17: the queue backend layer now accepts an explicit namespace-qualified topic (`namespace::response-normal`) as a compatibility extension. Kafka/Redis publish, subscribe, and DLQ routing can now target a non-local namespace without changing the default local-namespace behavior.
- Focused regression evidence for that batch: `cargo test --lib explicit_namespace_topic -- --nocapture`.
- 2026-04-17: response publish call sites now consume that capability: remote-origin responses are tagged with a namespace override, bypass the local response fast path, and are forwarded as `origin::response-*` instead of being rebound to the download node namespace.
- 2026-04-17: request-side cross-namespace subscription now has a minimal explicit source in `channel_config.federation_request_namespaces`. A download-pool node can subscribe to `origin::request-*` topics while preserving the default local-only behavior when the list is empty.
- Focused regression evidence for that batch: `cargo test --lib queue_manager_resolves_remote_request_namespace_subscriptions -- --nocapture` and `cargo test --lib federated_download_pool_subscribes_remote_request_topics -- --nocapture`.
- 2026-04-17: the downloader cache path now writes cached `Response` bodies back into `CacheService` and preserves cache ownership metadata (`response_cache_owner_namespace`, optional `response_cache_owner_node_id`, `response_cache_lookup_key`) across cache hits instead of dropping cached response metadata at read time.
- Focused regression evidence for that batch: `cargo test --lib response_cache_write_stores_ownership_metadata -- --nocapture`, `cargo test --lib cache_hit_preserves_cached_response_metadata -- --nocapture`, and `cargo test --lib request_downloader -- --nocapture`.
- 2026-04-17: parser-side cache refresh now turns federated responses into an explicit local rollback copy. When a response reaches the parser namespace, the refresh path rewrites `response_cache_owner_namespace` (and optional `response_cache_owner_node_id`) to the local parser ownership before writing the local cache entry, and now also records the localized owner in the namespace-local response-cache-owner index, so remote-origin responses no longer leave a foreign namespace as the rollback anchor inside the parser namespace.
- Focused regression evidence for that batch: `cargo test --lib parser_side_cache_refresh -- --nocapture` and `cargo test --lib parser_side_cache_refresh_records_local_owner_index -- --nocapture`.
- 2026-04-17: a first remote-cache-retrieval service-side groundwork batch is in place. Node heartbeats now publish an optional `api_port`, and the protected API exposes `GET /debug/cache/response/{cache_key}` to read one locally cached `Response` payload by cache key.
- 2026-04-17: caller-side remote cache fallback is now wired for both the current same-namespace path and a minimal explicit cross-namespace path. `RequestDownloader` records a lightweight `cache_key -> owner_node_id` index in the control-plane store, resolves same-namespace active owner nodes via heartbeat metadata, and can fetch `/debug/cache/response/{cache_key}` before degrading to a live download. When that remote cache fetch succeeds, the warmed local cache copy is now localized to the current downloader node and the owner index is rewritten to that local copy, so the warmed replica becomes a discoverable same-namespace fallback anchor. For foreign-owner namespaces, it now uses an explicit `channel_config.federation_response_cache_api_endpoints` mapping instead of ad-hoc service discovery.
- 2026-04-18: the response-cache owner index now has a first safe invalidation path instead of growing monotonically. Downloader-side lookup clears stale owner records only on negative signals that are safe to treat as authoritative for the current record: a self-owned local cache miss, a same-namespace owner that is no longer present in the active heartbeat set, or a remote cache lookup returning `404 Not Found`. Cleanup is conditional on the stored owner tuple still matching, so a newer owner record is not deleted by an older miss path.
- 2026-04-18: owner-side background cleanup is now in place for the current node identity. `DownloaderManager` periodically scans the namespace-local response-cache-owner index for records owned by the local node and conditionally clears records whose local cached payload is already missing, corrupt, or mismatched with the indexed `cache_key`, so stale owner tuples no longer depend exclusively on a future request-path lookup to be repaired.
- 2026-04-18: response-cache owner records now carry an explicit `expires_at` derived from the active downloader `cache_ttl`. New downloader-owned entries are written with that expiry, request-path lookup clears owner records that are already expired before attempting any remote fetch, and background owner cleanup also treats expired local owner tuples as authoritative stale state. The stored owner record remains backward-compatible with legacy MsgPack payloads that predate `expires_at`.
- 2026-04-19: response-cache lifecycle handling is now centralized behind a shared helper module instead of being split between downloader-side writes and parser-side rollback refresh. The shared path owns metadata annotation (`owner_namespace`, optional `owner_node_id`, lookup key, and absolute `response_cache_expires_at`), cache persistence, and owner-index upsert. Parser-side localized rollback copies now inherit the same explicit expiry contract carried by the response metadata rather than silently falling back to an unrelated local cache default whenever the downloader path had already established a cache TTL.
- 2026-04-19: request generation now materializes that cache contract directly onto `Request` values instead of leaving expiry semantics as downloader-local inference. Both the legacy chain path and `ModuleNodeDagAdapter` apply resolved common cache policy after node generation: `response_cache_enabled` now flips `Request.enable_response_cache`, and `response_cache_ttl_secs` is converted into an absolute `response_cache_expires_at` in `Request.meta`, allowing downloader-side lifecycle code to prefer explicit request policy while still keeping local TTL as a compatibility fallback.
- 2026-04-19: downloader-side expiry resolution is now explicitly frozen to the same rule in code and tests: `Response.metadata.response_cache_expires_at` and request-stamped expiry win over local downloader TTL fallback, and remote cache warm-copy inherits the source absolute expiry instead of renewing it from the current node's `cache_ttl`.
- 2026-04-19: the remaining legacy common-config path now preserves inherited module trait defaults for optional fields instead of wiping them with `None`. That closes the main no-profile gap for request-side cache policy: `Module::generate` now reapplies response-cache policy from effective `profile.common` or `ModuleTrait::default_common_config() + ModuleConfig overrides`, and `apply_legacy_common_overrides(...)` no longer drops inherited `rate_limit`, `proxy_pool`, `rate_limit_group`, or `response_cache_ttl_secs` when the module config omits them.
- 2026-04-19: the main legacy runtime-context path now also inherits those defaults before node code runs, instead of only repairing requests after generation. `ModuleDagProcessor` and the legacy scheduler runtime-input bridge now thread `ModuleTrait::default_common_config()` into `build_legacy_*_context`, so `NodeGenerateContext.config.common` / `NodeParseContext.config.common` expose the same effective common config that request-side policy stamping now uses on the no-profile path.
- 2026-04-19: foreign response-cache lookup is no longer hard-coupled to namespace-level endpoint mapping. Response-cache owner records and cached response metadata now persist an optional `response_cache_owner_api_base_url`, derived from the owner node heartbeat when a local downloader writes the cache entry. On a foreign-owner miss, `RequestDownloader` now prefers that owner-specific endpoint and only falls back to `channel_config.federation_response_cache_api_endpoints` when the owner record does not yet carry a direct route.
- 2026-04-19: owner-side background cleanup now also self-heals direct owner routes instead of only deleting bad tuples. When `DownloaderManager` scans local `cache_key -> owner` records, a valid local cache entry will refresh `response_cache_owner_api_base_url` from the current node heartbeat when `api_port` changes, and it will clear a stale endpoint when the local node no longer advertises an API port so foreign lookup can fall back cleanly to namespace-level mapping.
- 2026-04-19: foreign remote-cache lookup now treats persisted owner endpoints as a preferred first hop, not a hard override. If a foreign owner record carries `response_cache_owner_api_base_url` but that direct route fails or returns `404`, `RequestDownloader` now keeps probing the namespace-level configured endpoint when one exists. Only an authoritative `404` from the namespace bridge path clears the owner record; a stale direct owner route no longer masks a still-usable compatibility mapping.
- 2026-04-20: parser-side localized cache refresh now publishes the same owner endpoint metadata as downloader-owned writes when the local parser node has a live heartbeat `api_port`. That means parser-generated namespace-local rollback copies can also become directly routable foreign owners instead of always depending on static namespace mapping once they leave the originating namespace.
- Focused regression evidence for that batch: `cargo test --lib load_cached_response_returns_cached_payload -- --nocapture`, `cargo test --lib node_info_deserializes_legacy_payload_without_api_port -- --nocapture`, `cargo test --lib response_cache_write_stores_ownership_metadata -- --nocapture`, `cargo test --lib remote_cache_hit_fetches_owner_response_and_warms_local_cache -- --nocapture`, `cargo test --lib remote_cache_lookup_skips_foreign_namespace_owner_without_endpoint_mapping -- --nocapture`, `cargo test --lib remote_cache_hit_fetches_foreign_namespace_response_via_configured_endpoint -- --nocapture`, `cargo test --lib stale_local_owner_record_is_cleared_when_local_cache_misses -- --nocapture`, `cargo test --lib inactive_same_namespace_owner_record_is_cleared -- --nocapture`, and `cargo test --lib remote_cache_not_found_clears_stale_owner_record -- --nocapture`.
- Focused regression evidence for the request-side contract batch: `cargo test --lib adapter_executes_generate_when_runtime_input_is_present -- --nocapture`, `cargo test --lib execute_request_applies_response_cache_policy_from_legacy_common_config -- --nocapture`, and `cargo test --lib response_cache_write_records_owner_expiry_from_configured_ttl -- --nocapture`.
- Focused regression evidence for downloader-side precedence/inheritance: `cargo test --lib response_cache_write_prefers_explicit_expiry_over_configured_ttl -- --nocapture`, `cargo test --lib remote_cache_hit_preserves_explicit_source_expiry_when_warming_local_cache -- --nocapture`, and `cargo test --lib response_cache_write_records_owner_expiry_from_configured_ttl -- --nocapture`.
- Focused regression evidence for owner-endpoint routing: `cargo test --lib response_cache_owner_record_round_trips_with_endpoint_and_expiry -- --nocapture`, `cargo test --lib response_cache_write_records_owner_api_endpoint_from_local_heartbeat -- --nocapture`, `cargo test --lib remote_cache_hit_fetches_foreign_namespace_response_via_owner_endpoint -- --nocapture`, `cargo test --lib response_cache_write_stores_ownership_metadata -- --nocapture`, and `cargo test --lib remote_cache_hit_fetches_foreign_namespace_response_via_configured_endpoint -- --nocapture`.
- Focused regression evidence for owner-endpoint self-healing: `cargo test --lib cleanup_local_response_cache_owner_records_refreshes_owner_endpoint_from_heartbeat -- --nocapture`, `cargo test --lib cleanup_local_response_cache_owner_records_clears_stale_owner_endpoint_when_api_port_missing -- --nocapture`, `cargo test --lib remote_cache_hit_fetches_foreign_namespace_response_via_owner_endpoint -- --nocapture`, and `cargo test --lib response_cache_write_records_owner_api_endpoint_from_local_heartbeat -- --nocapture`.
- Focused regression evidence for owner-endpoint runtime fallback: `cargo test --lib remote_cache_hit_falls_back_to_configured_endpoint_when_owner_endpoint_is_stale -- --nocapture`, `cargo test --lib remote_cache_hit_fetches_foreign_namespace_response_via_owner_endpoint -- --nocapture`, and `cargo test --lib remote_cache_hit_fetches_foreign_namespace_response_via_configured_endpoint -- --nocapture`.
- Focused regression evidence for parser-side owner-endpoint publication: `cargo test --lib parser_side_cache_refresh_records_local_owner_endpoint_from_heartbeat -- --nocapture`, `cargo test --lib response_cache_write_records_owner_api_endpoint_from_local_heartbeat -- --nocapture`, and `cargo test --lib cleanup_local_response_cache_owner_records_refreshes_owner_endpoint_from_heartbeat -- --nocapture`.
- Focused regression evidence for legacy-default inheritance and top-level request stamping: `cargo test --lib generate_applies_response_cache_policy_from_module_trait_defaults -- --nocapture`, `cargo test --lib apply_legacy_common_overrides_preserves_inherited_option_defaults -- --nocapture`, and `cargo test --lib generate_returns_requests_for_single_node_module -- --nocapture`.
- Focused regression evidence for legacy runtime-context inheritance: `cargo test --lib generate_context_common_preserves_module_trait_defaults_on_legacy_path -- --nocapture`, `cargo test --lib generate_applies_response_cache_policy_from_module_trait_defaults -- --nocapture`, and `cargo test --lib generate_returns_requests_for_single_node_module -- --nocapture`.
- This still does not complete the full Federation Download Pool rollout by itself, because the foreign-namespace path still falls back to explicit API endpoint mapping when an owner record has no direct endpoint, and the target-state `shared-download` discovery topology is still pending.
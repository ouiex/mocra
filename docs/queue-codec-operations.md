# Queue Codec Operations Runbook

## Scope

`channel_config.queue_codec` controls the queue message codec.

Supported values:

- `msgpack`: default, production preferred
- `json`: debugging and cross-language inspection

The codec is fixed by `OnceCell` during process startup. Updating the config file does not switch codec online for an already-running process.

## Non-Negotiable Rule

Changing `channel_config.queue_codec` is a rolling-restart change.

Do not leave the cluster in a mixed-codec state while message production is active.

## Standard Change Procedure

1. Confirm the target codec value for every node configuration file.
2. Pause new engine activity:

```bash
curl -sS -X POST http://127.0.0.1:8080/control/pause
```

3. Wait for the current workload to drain to an acceptable level.
4. Restart nodes one by one with the updated `channel_config.queue_codec` value.
5. After each restart, verify the node is back and the control plane remains reachable.
6. When all nodes run the same codec, resume the engine:

```bash
curl -sS -X POST http://127.0.0.1:8080/control/resume
```

## Verification

Pre-change validation:

```bash
cargo test --lib queue::codec::tests -- --nocapture
```

Operational checks:

1. Confirm all nodes were restarted with the same codec value.
2. Confirm queue traffic resumes only after the final restart.
3. Confirm no decode failures or DLQ spikes appear after resume.

## Rollback

1. Pause the engine again.
2. Revert `channel_config.queue_codec` on every node.
3. Roll the same nodes again, one by one.
4. Resume the engine only after the entire fleet is back on the previous codec.

## Related Files

- `src/queue/codec.rs`
- `tests/config.toml`
- `tests/config.prod_like.toml`
use uuid::Uuid;

pub fn task_runtime_id(platform: &str, account: &str, run_id: Uuid) -> String {
    format!("{}:{}:{}", platform, account, run_id)
}

pub fn task_brief_id(account: &str, platform: &str) -> String {
    format!("{}-{}", account, platform)
}

pub fn module_runtime_id(account: &str, platform: &str, module_name: &str) -> String {
    format!("{}-{}-{}", account, platform, module_name)
}

pub fn execution_state_key(run_id: Uuid, module_id: &str) -> String {
    format!("chain:exec:{}:{}", run_id, module_id)
}

pub fn legacy_execution_state_key(run_id: Uuid, module_id: &str) -> String {
    format!("run:{}:module:{}", run_id, module_id)
}

pub fn execution_state_compat_keys(run_id: Uuid, module_id: &str) -> [String; 2] {
    [
        execution_state_key(run_id, module_id),
        legacy_execution_state_key(run_id, module_id),
    ]
}

pub fn dedup_key(ptm_key: &str) -> String {
    format!("chain:ptm:{}", ptm_key)
}

pub fn ptm_key(
    run_id: Uuid,
    account: &str,
    platform: &str,
    module_id: &str,
    step_idx: u32,
    prefix_request: Uuid,
) -> String {
    format!(
        "{}:{}:{}:{}:{}:{}",
        run_id, account, platform, module_id, step_idx, prefix_request
    )
}

pub fn advance_gate_key(run_id: Uuid, module_id: &str, from: usize, to: usize, prefix: Uuid) -> String {
    format!(
        "chain:gate:advance:{}:{}:{}:{}:{}",
        run_id, module_id, from, to, prefix
    )
}

pub fn module_step_advance_once_key(run_id: Uuid, module_id: &str, step_idx: usize) -> String {
    format!("chain:gate:step:{}:{}:{}", run_id, module_id, step_idx)
}

pub fn module_step_fallback_once_key(run_id: Uuid, module_id: &str, step_idx: usize, prefix: Uuid) -> String {
    format!(
        "chain:gate:fallback:{}:{}:{}:{}",
        run_id, module_id, step_idx, prefix
    )
}

pub fn legacy_module_step_advance_once_key(run_id: Uuid, module_id: &str, step_idx: usize) -> String {
    format!("{}:step:{}", legacy_execution_state_key(run_id, module_id), step_idx)
}

pub fn legacy_module_step_fallback_once_key(
    run_id: Uuid,
    module_id: &str,
    step_idx: usize,
    prefix: Uuid,
) -> String {
    format!(
        "{}:step:{}:prefix:{}",
        legacy_execution_state_key(run_id, module_id),
        step_idx,
        prefix
    )
}

pub fn error_emit_key(run_id: Uuid, module_id: &str, step: usize, prefix: Uuid, error_hash: &str) -> String {
    format!(
        "chain:error:emit:{}:{}:{}:{}:{}",
        run_id, module_id, step, prefix, error_hash
    )
}

pub fn module_threshold_key(task_id: &str, module_id: &str) -> String {
    format!("chain:threshold:module:{}:{}", task_id, module_id)
}

pub fn task_threshold_key(task_id: &str) -> String {
    format!("chain:threshold:task:{}", task_id)
}

pub fn terminate_task_key(task_id: &str) -> String {
    format!("chain:terminate:task:{}", task_id)
}

pub fn terminate_module_key(task_id: &str, module_id: &str) -> String {
    format!("chain:terminate:module:{}:{}", task_id, module_id)
}

pub fn error_retry_schedule_key(task_id: &str) -> String {
    format!("chain:retry:error_task:{}", task_id)
}

pub fn parser_processed_key(namespace: &str, id: &str) -> String {
    format!("{}:processed:parser:{}", namespace, id)
}

pub fn parser_lock_key(namespace: &str, id: &str) -> String {
    format!("{}:lock:parser:{}", namespace, id)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn runtime_task_and_module_id_are_stable() {
        let run_id = Uuid::parse_str("0194e7af-90f0-7c0a-a3cb-4f8f7d11ed88").unwrap();
        assert_eq!(
            task_runtime_id("x", "a", run_id),
            "x:a:0194e7af-90f0-7c0a-a3cb-4f8f7d11ed88"
        );
        assert_eq!(module_runtime_id("a", "x", "m1"), "a-x-m1");
    }

    #[test]
    fn chain_keys_match_expected_prefixes() {
        let run_id = Uuid::parse_str("0194e7af-90f0-7c0a-a3cb-4f8f7d11ed88").unwrap();
        let prefix = Uuid::parse_str("0194e7af-90f0-7c0a-a3cb-4f8f7d11ed89").unwrap();
        assert_eq!(
            execution_state_key(run_id, "a-x-m1"),
            "chain:exec:0194e7af-90f0-7c0a-a3cb-4f8f7d11ed88:a-x-m1"
        );
        assert!(advance_gate_key(run_id, "a-x-m1", 1, 2, prefix)
            .starts_with("chain:gate:advance:"));
        assert_eq!(
            parser_processed_key("mocra", "abc"),
            "mocra:processed:parser:abc"
        );
        assert_eq!(
            error_retry_schedule_key("x:a:0194e7af-90f0-7c0a-a3cb-4f8f7d11ed88"),
            "chain:retry:error_task:x:a:0194e7af-90f0-7c0a-a3cb-4f8f7d11ed88"
        );
    }

    #[test]
    fn execution_state_compat_keys_include_new_and_legacy() {
        let run_id = Uuid::parse_str("0194e7af-90f0-7c0a-a3cb-4f8f7d11ed88").unwrap();
        let keys = execution_state_compat_keys(run_id, "acc-pf-m1");
        assert_eq!(
            keys[0],
            "chain:exec:0194e7af-90f0-7c0a-a3cb-4f8f7d11ed88:acc-pf-m1"
        );
        assert_eq!(
            keys[1],
            "run:0194e7af-90f0-7c0a-a3cb-4f8f7d11ed88:module:acc-pf-m1"
        );
    }

    #[test]
    fn module_step_gate_keys_are_stable_and_legacy_compatible() {
        let run_id = Uuid::parse_str("0194e7af-90f0-7c0a-a3cb-4f8f7d11ed88").unwrap();
        let prefix = Uuid::parse_str("0194e7af-90f0-7c0a-a3cb-4f8f7d11ed89").unwrap();
        assert_eq!(
            module_step_advance_once_key(run_id, "acc-pf-m1", 3),
            "chain:gate:step:0194e7af-90f0-7c0a-a3cb-4f8f7d11ed88:acc-pf-m1:3"
        );
        assert_eq!(
            module_step_fallback_once_key(run_id, "acc-pf-m1", 3, prefix),
            "chain:gate:fallback:0194e7af-90f0-7c0a-a3cb-4f8f7d11ed88:acc-pf-m1:3:0194e7af-90f0-7c0a-a3cb-4f8f7d11ed89"
        );
        assert_eq!(
            legacy_module_step_advance_once_key(run_id, "acc-pf-m1", 3),
            "run:0194e7af-90f0-7c0a-a3cb-4f8f7d11ed88:module:acc-pf-m1:step:3"
        );
        assert_eq!(
            legacy_module_step_fallback_once_key(run_id, "acc-pf-m1", 3, prefix),
            "run:0194e7af-90f0-7c0a-a3cb-4f8f7d11ed88:module:acc-pf-m1:step:3:prefix:0194e7af-90f0-7c0a-a3cb-4f8f7d11ed89"
        );
    }
}

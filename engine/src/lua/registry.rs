use std::collections::HashMap;
use std::sync::Arc;

use cacheable::CacheService;
use deadpool_redis::redis::from_redis_value;
use errors::CacheError;
use log::{info, warn};

#[derive(Clone)]
pub struct LuaScriptSpec {
    pub name: &'static str,
    pub body: &'static str,
}

#[derive(Default, Clone)]
pub struct LuaScriptRegistry {
    scripts: Vec<LuaScriptSpec>,
    sha_map: Arc<tokio::sync::RwLock<HashMap<String, String>>>,
}

impl LuaScriptRegistry {
    pub fn new_default() -> Self {
        Self {
            scripts: vec![
                LuaScriptSpec {
                    name: "ptm_claim.lua",
                    body: include_str!("../../lua/ptm_claim.lua"),
                },
                LuaScriptSpec {
                    name: "ptm_commit_success.lua",
                    body: include_str!("../../lua/ptm_commit_success.lua"),
                },
                LuaScriptSpec {
                    name: "ptm_commit_error.lua",
                    body: include_str!("../../lua/ptm_commit_error.lua"),
                },
                LuaScriptSpec {
                    name: "etm_threshold_decide.lua",
                    body: include_str!("../../lua/etm_threshold_decide.lua"),
                },
                LuaScriptSpec {
                    name: "etm_retry_schedule.lua",
                    body: include_str!("../../lua/etm_retry_schedule.lua"),
                },
                LuaScriptSpec {
                    name: "etm_terminate_mark.lua",
                    body: include_str!("../../lua/etm_terminate_mark.lua"),
                },
            ],
            sha_map: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
        }
    }

    pub async fn preload(&self, cache_service: &CacheService) {
        for spec in &self.scripts {
            match cache_service.script_load(spec.body).await {
                Ok(sha) => {
                    self.sha_map
                        .write()
                        .await
                        .insert(spec.name.to_string(), sha.clone());
                    info!("[LuaScriptRegistry] script loaded: {} => {}", spec.name, sha);
                }
                Err(err) => {
                    warn!(
                        "[LuaScriptRegistry] script preload skipped: {} error={}",
                        spec.name, err
                    );
                }
            }
        }
    }

    pub async fn get_sha(&self, name: &str) -> Option<String> {
        self.sha_map.read().await.get(name).cloned()
    }

    pub async fn eval_triplet_with_fallback(
        &self,
        cache_service: &CacheService,
        name: &str,
        script: &str,
        keys: &[&str],
        args: &[&str],
    ) -> Result<(i64, String, String), CacheError> {
        let maybe_sha = self.get_sha(name).await;

        let value_res = if let Some(sha) = maybe_sha {
            match cache_service.evalsha(&sha, keys, args).await {
                Ok(val) => Ok(val),
                Err(err) => {
                    if err.to_string().contains("NOSCRIPT") {
                        let loaded_sha = cache_service.script_load(script).await?;
                        self.sha_map
                            .write()
                            .await
                            .insert(name.to_string(), loaded_sha.clone());
                        cache_service.evalsha(&loaded_sha, keys, args).await
                    } else {
                        Err(err)
                    }
                }
            }
        } else {
            let loaded_sha = cache_service.script_load(script).await?;
            self.sha_map
                .write()
                .await
                .insert(name.to_string(), loaded_sha.clone());
            cache_service.evalsha(&loaded_sha, keys, args).await
        };

        let value = value_res?;
        let parsed: (i64, String, String) =
            from_redis_value(&value).map_err(|e| CacheError::Pool(e.to_string()))?;
        Ok(parsed)
    }
}

#[cfg(test)]
mod tests {
    use super::LuaScriptRegistry;
    use futures::future::join_all;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn script_body<'a>(registry: &'a LuaScriptRegistry, name: &str) -> &'a str {
        registry
            .scripts
            .iter()
            .find(|s| s.name == name)
            .map(|s| s.body)
            .unwrap_or("")
    }

    #[test]
    fn default_registry_contains_error_task_scripts() {
        let registry = LuaScriptRegistry::new_default();
        let names: Vec<&str> = registry.scripts.iter().map(|s| s.name).collect();

        assert!(names.contains(&"etm_threshold_decide.lua"));
        assert!(names.contains(&"etm_retry_schedule.lua"));
        assert!(names.contains(&"etm_terminate_mark.lua"));
    }

    #[test]
    fn threshold_decide_uses_inclusive_boundary_checks() {
        let registry = LuaScriptRegistry::new_default();
        let body = script_body(&registry, "etm_threshold_decide.lua");

        assert!(
            body.contains("module_count >= module_threshold"),
            "module threshold must use inclusive boundary (>=)"
        );
        assert!(
            body.contains("task_count >= task_threshold"),
            "task threshold must use inclusive boundary (>=)"
        );
    }

    #[test]
    fn terminate_mark_has_idempotent_guard() {
        let registry = LuaScriptRegistry::new_default();
        let body = script_body(&registry, "etm_terminate_mark.lua");

        assert!(
            body.contains("redis.call('EXISTS', terminate_key) == 1"),
            "terminate mark must short-circuit when key already exists"
        );
        assert!(
            body.contains("'ALREADY_TERMINATED'"),
            "terminate mark must return ALREADY_TERMINATED on duplicate call"
        );
        assert!(
            body.contains("redis.call('SET', terminate_key, payload)"),
            "terminate mark must persist payload when first applied"
        );
    }

    #[test]
    fn ptm_claim_contains_step_and_state_guards() {
        let registry = LuaScriptRegistry::new_default();
        let body = script_body(&registry, "ptm_claim.lua");

        assert!(body.contains("dedup_state == 'done'"));
        assert!(body.contains("dedup_state == 'processing'"));
        assert!(body.contains("expected_step <= committed_step"));
        assert!(body.contains("expected_step > committed_step + 1"));
        assert!(body.contains("'EXECUTE'"));
        assert!(body.contains("'DUPLICATE_DONE'"));
        assert!(body.contains("'STALE_STEP'"));
        assert!(body.contains("'OUT_OF_ORDER'"));
        assert!(body.contains("'LOCKED_BY_OTHER'"));
    }

    #[test]
    fn ptm_commit_scripts_contain_fencing_and_cas_guards() {
        let registry = LuaScriptRegistry::new_default();
        let success = script_body(&registry, "ptm_commit_success.lua");
        let error = script_body(&registry, "ptm_commit_error.lua");

        assert!(success.contains("'FENCING_REJECT'"));
        assert!(success.contains("'CAS_CONFLICT'"));
        assert!(success.contains("'ALREADY_COMMITTED'"));
        assert!(success.contains("'COMMIT_OK'"));

        assert!(error.contains("'FENCING_REJECT'"));
        assert!(error.contains("'ERROR_ALREADY_EMITTED'"));
        assert!(error.contains("'ERROR_EMIT_OK'"));
    }

    fn redis_test_url() -> Option<String> {
        std::env::var("REDIS_URL")
            .ok()
            .or_else(|| std::env::var("MOCRA_REDIS_TEST_URL").ok())
    }

    #[tokio::test]
    async fn redis_threshold_decide_hits_module_boundary_exactly() {
        let Some(redis_url) = redis_test_url() else {
            eprintln!("skip redis integration: REDIS_URL/MOCRA_REDIS_TEST_URL not set");
            return;
        };

        let registry = LuaScriptRegistry::new_default();
        let body = script_body(&registry, "etm_threshold_decide.lua");
        let client = redis::Client::open(redis_url).expect("redis client should open");
        let mut conn = client
            .get_multiplexed_async_connection()
            .await
            .expect("redis connection should be available");

        let sha: String = redis::cmd("SCRIPT")
            .arg("LOAD")
            .arg(body)
            .query_async(&mut conn)
            .await
            .expect("script load should succeed");

        let now_ns = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let module_key = format!("t09:test:module_counter:{}", now_ns);
        let task_key = format!("t09:test:task_counter:{}", now_ns);

        let _: i64 = redis::cmd("DEL")
            .arg(&module_key)
            .arg(&task_key)
            .query_async(&mut conn)
            .await
            .expect("cleanup should succeed");

        let first: (i64, String, String) = redis::cmd("EVALSHA")
            .arg(&sha)
            .arg(2)
            .arg(&module_key)
            .arg(&task_key)
            .arg("2")
            .arg("99")
            .arg("1000")
            .arg("60")
            .query_async(&mut conn)
            .await
            .expect("first eval should succeed");
        assert_eq!(first.0, 1);
        assert_eq!(first.1, "RETRY_AFTER");

        let second: (i64, String, String) = redis::cmd("EVALSHA")
            .arg(&sha)
            .arg(2)
            .arg(&module_key)
            .arg(&task_key)
            .arg("2")
            .arg("99")
            .arg("1000")
            .arg("60")
            .query_async(&mut conn)
            .await
            .expect("second eval should succeed");
        assert_eq!(second.0, 2);
        assert_eq!(second.1, "TERMINATE_MODULE");

        let module_count: i64 = redis::cmd("GET")
            .arg(&module_key)
            .query_async(&mut conn)
            .await
            .expect("module counter should exist");
        assert_eq!(module_count, 2);
    }

    #[tokio::test]
    async fn redis_terminate_mark_is_idempotent_under_concurrency() {
        let Some(redis_url) = redis_test_url() else {
            eprintln!("skip redis integration: REDIS_URL/MOCRA_REDIS_TEST_URL not set");
            return;
        };

        let registry = LuaScriptRegistry::new_default();
        let body = script_body(&registry, "etm_terminate_mark.lua");
        let client = redis::Client::open(redis_url).expect("redis client should open");
        let mut seed_conn = client
            .get_multiplexed_async_connection()
            .await
            .expect("redis connection should be available");

        let sha: String = redis::cmd("SCRIPT")
            .arg("LOAD")
            .arg(body)
            .query_async(&mut seed_conn)
            .await
            .expect("script load should succeed");

        let now_ns = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let terminate_key = format!("t09:test:terminate:{}", now_ns);
        let _: i64 = redis::cmd("DEL")
            .arg(&terminate_key)
            .query_async(&mut seed_conn)
            .await
            .expect("cleanup should succeed");

        let attempts = 20;
        let mut jobs = Vec::with_capacity(attempts);
        for idx in 0..attempts {
            let client = client.clone();
            let sha = sha.clone();
            let terminate_key = terminate_key.clone();
            jobs.push(tokio::spawn(async move {
                let mut conn = client
                    .get_multiplexed_async_connection()
                    .await
                    .expect("connection should be available");
                let now_ms = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis()
                    .to_string();
                redis::cmd("EVALSHA")
                    .arg(&sha)
                    .arg(1)
                    .arg(&terminate_key)
                    .arg(format!("reason-{idx}"))
                    .arg(now_ms)
                    .arg("60")
                    .query_async::<(i64, String, String)>(&mut conn)
                    .await
                    .expect("evalsha should succeed")
            }));
        }

        let results = join_all(jobs).await;
        let mut terminated = 0;
        let mut already_terminated = 0;

        for result in results {
            let (code, status, _payload) = result.expect("task should join");
            match (code, status.as_str()) {
                (0, "TERMINATED") => terminated += 1,
                (1, "ALREADY_TERMINATED") => already_terminated += 1,
                other => panic!("unexpected terminate result: {:?}", other),
            }
        }

        assert_eq!(terminated, 1);
        assert_eq!(already_terminated, attempts - 1);
    }

    #[tokio::test]
    async fn redis_ptm_claim_and_commit_success_follow_expected_state_machine() {
        let Some(redis_url) = redis_test_url() else {
            eprintln!("skip redis integration: REDIS_URL/MOCRA_REDIS_TEST_URL not set");
            return;
        };

        let registry = LuaScriptRegistry::new_default();
        let claim_body = script_body(&registry, "ptm_claim.lua");
        let commit_body = script_body(&registry, "ptm_commit_success.lua");
        let client = redis::Client::open(redis_url).expect("redis client should open");
        let mut conn = client
            .get_multiplexed_async_connection()
            .await
            .expect("redis connection should be available");

        let claim_sha: String = redis::cmd("SCRIPT")
            .arg("LOAD")
            .arg(claim_body)
            .query_async(&mut conn)
            .await
            .expect("claim script load should succeed");
        let commit_sha: String = redis::cmd("SCRIPT")
            .arg("LOAD")
            .arg(commit_body)
            .query_async(&mut conn)
            .await
            .expect("commit script load should succeed");

        let now_ns = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let dedup_key = format!("t07:test:ptm:dedup:{}", now_ns);
        let exec_key = format!("t07:test:ptm:exec:{}", now_ns);

        let _: i64 = redis::cmd("DEL")
            .arg(&dedup_key)
            .arg(&exec_key)
            .query_async(&mut conn)
            .await
            .expect("cleanup should succeed");

        let claim_res: (i64, String, String) = redis::cmd("EVALSHA")
            .arg(&claim_sha)
            .arg(2)
            .arg(&dedup_key)
            .arg(&exec_key)
            .arg("1")
            .arg("owner-a")
            .arg("60")
            .arg("1000")
            .query_async(&mut conn)
            .await
            .expect("claim should succeed");
        assert_eq!(claim_res.0, 0);
        assert_eq!(claim_res.1, "EXECUTE");

        let commit_res: (i64, String, String) = redis::cmd("EVALSHA")
            .arg(&commit_sha)
            .arg(2)
            .arg(&dedup_key)
            .arg(&exec_key)
            .arg("1")
            .arg("owner-a")
            .arg("1001")
            .arg("120")
            .query_async(&mut conn)
            .await
            .expect("commit should succeed");
        assert_eq!(commit_res.0, 0);
        assert_eq!(commit_res.1, "COMMIT_OK");

        let duplicate_claim: (i64, String, String) = redis::cmd("EVALSHA")
            .arg(&claim_sha)
            .arg(2)
            .arg(&dedup_key)
            .arg(&exec_key)
            .arg("1")
            .arg("owner-a")
            .arg("60")
            .arg("1002")
            .query_async(&mut conn)
            .await
            .expect("duplicate claim should succeed");
        assert_eq!(duplicate_claim.0, 1);
        assert_eq!(duplicate_claim.1, "DUPLICATE_DONE");
    }
}

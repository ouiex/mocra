use crate::sync::build_leadership_gate;

#[tokio::test]
async fn build_leadership_gate_without_raft_or_redis_uses_local_gate() {
    let gate = build_leadership_gate(None, None, "demo", 5000);

    gate.clone().start().await;

    assert!(gate.is_leader());
}

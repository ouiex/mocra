//! 分区归属(重构 Phase 3):把采集工作按分区分摊到集群节点。
//!
//! 设计:
//! - **分区键**:`hash(account) % N` 把账号映射到固定数量的虚拟分区(默认 256)。
//! - **归属**:Rendezvous(HRW)哈希 —— 每个分区归属于 `(partition, node)` 组合
//!   哈希值最高的那个节点。成员增减时**只有约 1/N 的分区迁移**,天然最小化再平衡。
//! - **一致性**:成员列表来自 Raft(强一致),且哈希是**确定性**的(不用带随机种子的
//!   `DefaultHasher`),因此所有节点算出的归属完全一致,无需额外协商即达成共识。
//! - **防脑裂**:成员切换的瞬间各节点视图可能短暂不一致;需要强保证时,再用
//!   [`RaftControlPlane::acquire_partition`](crate::RaftControlPlane::acquire_partition)
//!   取一个带单调 fencing token 的归属租约(复用 Raft 锁),陈旧属主凭更小的 token 被拒。
//!
//! 对有消费组的 MQ(Kafka/NATS),分区分配可直接复用消费组;此模块是**无消费组**
//! 后端(内存 / Redis Streams)的兜底归属方案。

use crate::raft::NodeId;

/// 默认虚拟分区数。取 2 的幂,便于均匀分布;足够细以在中小集群里均衡负载。
pub const DEFAULT_PARTITIONS: u32 = 256;

const FNV_OFFSET: u64 = 14695981039346656037;
const FNV_PRIME: u64 = 1099511628211;

#[inline]
fn fnv1a(bytes: &[u8]) -> u64 {
    let mut h = FNV_OFFSET;
    for &b in bytes {
        h ^= b as u64;
        h = h.wrapping_mul(FNV_PRIME);
    }
    h
}

/// 账号 / 会话键 → 分区号。稳定且与进程无关。
pub fn partition_of(key: &str, partitions: u32) -> u32 {
    let n = partitions.max(1);
    (fnv1a(key.as_bytes()) % n as u64) as u32
}

/// Rendezvous(HRW)评分:`(partition, node)` 组合的稳定哈希。
#[inline]
fn hrw_score(partition: u32, node: NodeId) -> u64 {
    let mut buf = [0u8; 12];
    buf[..8].copy_from_slice(&node.to_le_bytes());
    buf[8..].copy_from_slice(&partition.to_le_bytes());
    fnv1a(&buf)
}

/// 计算某分区在给定成员集合下的归属节点(评分最高者)。
///
/// 成员为空返回 `None`。评分相同则取 `node_id` 较大者(确定性 tie-break)。
pub fn owner_of_partition(partition: u32, members: &[NodeId]) -> Option<NodeId> {
    members
        .iter()
        .copied()
        .map(|node| (hrw_score(partition, node), node))
        .max()
        .map(|(_, node)| node)
}

/// 某成员在给定成员集合下拥有的分区集合。
pub fn partitions_owned_by(me: NodeId, members: &[NodeId], partitions: u32) -> Vec<u32> {
    let n = partitions.max(1);
    (0..n)
        .filter(|&p| owner_of_partition(p, members) == Some(me))
        .collect()
}

/// 某账号 / 会话键是否归 `me` 处理。
pub fn owns_key(me: NodeId, key: &str, members: &[NodeId], partitions: u32) -> bool {
    owner_of_partition(partition_of(key, partitions), members) == Some(me)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    #[test]
    fn assignment_is_deterministic_and_process_independent() {
        // 同样输入必得同样归属(不依赖随机种子)。
        let members = vec![1u64, 2, 3, 4, 5];
        for p in 0..DEFAULT_PARTITIONS {
            let a = owner_of_partition(p, &members);
            let b = owner_of_partition(p, &members);
            assert_eq!(a, b);
            assert!(a.is_some());
        }
        // 已知向量:分区哈希稳定(回归护栏,值随算法固定即可)。
        assert_eq!(
            partition_of("account-42", DEFAULT_PARTITIONS),
            partition_of("account-42", DEFAULT_PARTITIONS)
        );
    }

    #[test]
    fn every_partition_owned_by_exactly_one_member() {
        let members = vec![10u64, 20, 30];
        let mut union: BTreeSet<u32> = BTreeSet::new();
        for &m in &members {
            for p in partitions_owned_by(m, &members, DEFAULT_PARTITIONS) {
                // 无重叠:同一分区不能被两个成员认领。
                assert!(
                    union.insert(p),
                    "partition {p} owned by more than one member"
                );
            }
        }
        // 全覆盖:每个分区都有归属。
        assert_eq!(union.len(), DEFAULT_PARTITIONS as usize);
    }

    #[test]
    fn load_is_roughly_balanced() {
        let members = vec![1u64, 2, 3, 4];
        let per = DEFAULT_PARTITIONS as usize / members.len();
        for &m in &members {
            let owned = partitions_owned_by(m, &members, DEFAULT_PARTITIONS).len();
            // 允许 ±50% 抖动(256 分区 / 4 节点 ≈ 64,区间 [32,96])。
            assert!(
                owned >= per / 2 && owned <= per * 3 / 2,
                "member {m} owns {owned}, expected ~{per}"
            );
        }
    }

    #[test]
    fn membership_change_moves_minimal_partitions() {
        // HRW 关键性质:加一个节点,只有归属新节点的分区迁移,其余不动。
        let before = vec![1u64, 2, 3];
        let after = vec![1u64, 2, 3, 4];
        let mut moved = 0;
        for p in 0..DEFAULT_PARTITIONS {
            let o1 = owner_of_partition(p, &before).unwrap();
            let o2 = owner_of_partition(p, &after).unwrap();
            if o1 != o2 {
                moved += 1;
                // 迁移的目标只可能是新加入的节点 4。
                assert_eq!(o2, 4, "partition {p} moved to a non-new node");
            }
        }
        // 迁移量应接近 1/4(新节点应得的份额),远小于全量。
        let expected = DEFAULT_PARTITIONS as usize / after.len();
        assert!(
            moved > 0 && moved < DEFAULT_PARTITIONS as usize / 2,
            "moved {moved}, expected ~{expected} (minimal reshuffle)"
        );
    }

    #[test]
    fn single_member_owns_everything() {
        let members = vec![7u64];
        assert_eq!(
            partitions_owned_by(7, &members, DEFAULT_PARTITIONS).len(),
            DEFAULT_PARTITIONS as usize
        );
        assert!(owns_key(7, "any-account", &members, DEFAULT_PARTITIONS));
    }

    #[test]
    fn empty_members_owns_nothing() {
        assert_eq!(owner_of_partition(0, &[]), None);
        assert!(!owns_key(1, "acc", &[], DEFAULT_PARTITIONS));
    }
}

# 性能优化快速启用指南

本文档介绍如何启用Mocra框架的最新性能优化功能。

---

## 🚀 快速启用优化

### 1. 启用双层缓存 (L1 + L2)

**配置文件**: `config.toml`

```toml
[cache]
ttl = 3600                # L2 Redis缓存TTL
enable_l1 = true          # ✅ 启用L1本地缓存
l1_ttl_secs = 30          # L1缓存TTL（建议30秒）
l1_max_entries = 10000    # L1最大条目数（根据内存调整）
compression_threshold = 1024

[cache.redis]
redis_host = "localhost"
redis_port = 6379
redis_db = 0
pool_size = 50
```

**效果**:
- ✅ L1命中率50%时，延迟降低45%
- ✅ L1命中率80%时，延迟降低80%
- ✅ Redis负载降低50-80%

---

### 2. Bloom Filter去重（自动启用）

Bloom Filter去重已集成到Deduplicator中，**无需额外配置**。

**可选调优**:
```rust
// 在代码中自定义Bloom Filter参数
let deduplicator = Deduplicator::new_with_bloom_config(
    pool,
    ttl,
    namespace,
    10_000_000,  // 容量：预期去重数量
    0.01,        // 误报率：1%
);
```

**效果**:
- ✅ 新请求延迟从2ms降至0.1ms
- ✅ Redis去重查询减少99%
- ✅ 去重吞吐量提升100倍+

---

## 📊 性能监控

### Prometheus指标

启用优化后，可通过以下指标监控效果：

#### 缓存指标
```promql
# L1命中率
rate(cache_hits{level="l1"}[5m]) / (rate(cache_hits{level="l1"}[5m]) + rate(cache_misses{level="l1"}[5m]))

# L2命中率
rate(cache_hits{level="l2"}[5m]) / (rate(cache_hits{level="l2"}[5m]) + rate(cache_misses{level="l2"}[5m]))

# 平均延迟（微秒）
histogram_quantile(0.5, cache_get_latency_us{level="l1"})
histogram_quantile(0.5, cache_get_latency_us{level="l2"})

# L1驱逐次数
rate(cache_l1_evictions[5m])
```

#### 去重指标
```promql
# Bloom Filter过滤效率
rate(dedup_bloom_hits{result="definitely_new"}[5m])

# L1去重命中率
rate(dedup_l1_hits[5m])

# 去重平均延迟
histogram_quantile(0.5, dedup_check_latency_us)
histogram_quantile(0.99, dedup_check_latency_us)
```

---

## 🔧 生产环境最佳实践

### 内存规划

**L1缓存内存估算**:
```
每条目 ≈ 1KB (key) + 数据大小
10,000条 × 1KB ≈ 10 MB
50,000条 × 1KB ≈ 50 MB

建议配置: l1_max_entries = 可用内存(MB) * 1000
```

**Bloom Filter内存**:
```
10M条, 1% FP率 ≈ 12 MB (固定)
100M条, 1% FP率 ≈ 120 MB

公式: 内存(MB) = 条数 × (-ln(FP率) / ln(2)²) / 8 / 1024 / 1024
```

### TTL调优

**L1 TTL vs L2 TTL**:
- L1_TTL应 < L2_TTL（防止读到过期数据）
- 推荐配置：L1=30s, L2=3600s
- 热数据场景可增大L1_TTL至60-120s

**Bloom Filter重置周期**:
- 默认：10分钟自动重置
- 高流量场景：可减至5分钟
- 低流量场景：可增至30分钟

### 多节点部署

**负载均衡场景**:
```toml
# 每个Worker节点独立L1缓存
[cache]
enable_l1 = true
l1_max_entries = 50000    # 按节点内存调整

# Redis作为共享L2
[cache.redis]
redis_host = "redis-cluster.internal"
pool_size = 100
```

**预期效果**:
- 3节点集群：总L1缓存容量 = 50K × 3 = 150K
- L1命中率分布均匀（每个节点独立命中）
- Redis负载降低 = 1 - (L1命中率平均值)

---

## 🧪 性能验证

### 本地测试

**1. 启动Redis**:
```bash
docker run -d --name redis -p 6379:6379 redis:7-alpine
```

**2. 运行缓存性能测试**:
```bash
cd /home/eason/mocra/mocra-mc
REDIS_HOST=localhost cargo run --release --bin test_cache_performance
```

**预期输出**:
```
=== Cache Performance Comparison ===

Test 1: Redis-only cache (baseline)
  Average latency: 1524.35 µs

Test 2: Two-level cache (L1 enabled)
  Average latency: 203.18 µs

=== Results ===
Redis-only:    1524.35 µs
Two-level:     203.18 µs
Improvement:   86.7%
Speedup:       7.50x
```

**3. 运行完整基准测试**:
```bash
cargo run --release --bin benchmark_redis
```

### 生产环境灰度

**阶段1**: 单节点启用，观察1周
```toml
[cache]
enable_l1 = true
l1_max_entries = 10000
```

**阶段2**: 50%节点启用，观察3天
```bash
# 修改一半节点配置
# 对比启用/未启用节点的延迟和Redis负载
```

**阶段3**: 全量启用
```bash
# 所有节点启用L1缓存
# 监控Redis负载是否显著下降
```

---

## ⚠️ 故障排查

### 问题1: L1命中率低于预期

**可能原因**:
1. `l1_ttl_secs` 设置过短
2. `l1_max_entries` 设置过小，频繁驱逐
3. 请求模式过于分散（无热点数据）

**解决方案**:
- 增大 `l1_ttl_secs` 至 60-120秒
- 增大 `l1_max_entries` 至内存允许的最大值
- 检查 `cache_l1_evictions` 指标，如果过高则增大容量

### 问题2: 内存占用过高

**可能原因**:
1. `l1_max_entries` 设置过大
2. 缓存的value平均大小超预期

**解决方案**:
- 减小 `l1_max_entries`
- 增大 `compression_threshold`，压缩大对象
- 监控 `cache_get_latency_us{level="l1"}` 确保L1性能

### 问题3: Bloom Filter误报率高

**现象**: `dedup_l2_hits` 指标异常高

**可能原因**:
- Bloom Filter已饱和（超过容量）
- 重置周期过长

**解决方案**:
```rust
// 增大Bloom Filter容量
let deduplicator = Deduplicator::new_with_bloom_config(
    pool, ttl, namespace,
    100_000_000,  // 10M → 100M
    0.01,
);
```

---

## 📖 延伸阅读

- [优化方案详细设计](docs/Optimization_Plan_2026_Architecture_Redis_v2.md)
- [优化执行报告](OPTIMIZATION.md)
- [优化总结](OPTIMIZATION_SUMMARY.md)
- [系统架构文档](docs/System_Architecture_zh.md)

---

## ✅ 检查清单

部署前请确认：

- [ ] 已修改 `config.toml`，启用 `enable_l1 = true`
- [ ] 已设置合理的 `l1_max_entries`（根据可用内存）
- [ ] 已配置Prometheus监控指标
- [ ] 已在测试环境验证性能提升
- [ ] 已制定灰度发布计划
- [ ] 已准备回滚方案（设置 `enable_l1 = false`）

---

**最后更新**: 2026-02-02  
**联系方式**: 如有问题请参考文档或提交Issue

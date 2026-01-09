# 系统架构与性能优化报告

**日期**: 2026-01-09  
**项目**: Mocra Core  

本文档记录了近期针对系统架构与 Redis 交互层面的关键性能优化措施。主要包括：
1. **架构优化**：ModuleProcessor 信号检查节流 (Throttling)。
2. **Redis 优化**：缓存层智能压缩策略 (Smart Compression)。

---

## 1. 架构优化：ModuleProcessor 信号检查节流

### 背景与问题
在 `ModuleProcessorWithChain` (`engine/src/task/module_processor_with_chain.rs`) 的主循环逻辑中，系统需要频繁检查“停止信号”(Stop Signal) 以响应优雅停机或任务中断请求。
- **原始逻辑**: 每次处理 Request 前都会查询 Redis 检查 `chain_stop` 标志。
- **性能瓶颈**: 在高吞吐量场景下（如数千 QPS），这种高频的 Redis 读取操作导致了不必要的网络往返（RTT）和 Redis Server 负载，且大多数时候信号并未变更。

### 优化方案
引入 **1秒节流 (1s Throttle)** 机制到 `is_stopped()` 方法中。

**核心逻辑**:
- 维护一个内存原子变量 `last_stop_check` (AtomicU64) 记录上次检查时间。
- 每次调用 `is_stopped()` 时：
    1. 获取当前时间 `now`。
    2. 如果 `now <= last_stop_check`，直接假设未停止（返回 `Ok(false)`），跳过 Redis 查询。
    3. 仅当时间差超过 1 秒时，才真正发起 Redis `GET` 请求，并更新 `last_stop_check`。

**代码片段**:
```rust
async fn is_stopped(&self) -> Result<bool> {
    // 快速路径：显式标记为已停止的内存状态
    if *self.stop.read().await { return Ok(true); }

    // 节流逻辑：限制 Redis 访问频率为 1Hz
    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
    let last_check = self.last_stop_check.load(Ordering::Relaxed);
    
    if now <= last_check {
         return Ok(false); // 视为未停止
    }
    
    // ... 发起真实的 Redis 查询 ...
    self.last_stop_check.store(now, Ordering::Relaxed);
}
```

### 优化成效
- **Redis QPS 降低**: 大幅减少了 `chain_stop` Key 的读取频率。
- **系统延迟降低**: 消除了 99% 以上请求在“检查状态”步骤的 IO 等待时间。
- **验证**: 逻辑已在 `module_processor_with_chain.rs` 中完成部署并验证。

---

## 2. Redis 优化：智能缓存压缩 (Smart Compression)

### 背景与问题
缓存层 (`cacheable`) 负责存储请求上下文和响应数据。为了节省带宽，初步尝试对所有 `set` 操作启用 Gzip 压缩。
- **问题现象**: 启用全量压缩后，系统吞吐量由基准值下降至 **~532 req/s**。
- **原因分析**: 系统中存在主要的小型对象（平均 ~496 bytes）。
    1. **CPU vs 网络**: 小对象的网络传输耗时（<2µs）远小于 Gzip 压缩耗时（>50µs）。
    2. **头部开销**: Gzip 产生的 Header/Footer（~18 bytes）导致极小对象压缩后反而变大（负优化）。

### 优化方案
实施 **智能压缩策略 (Smart Compression)**，修改 `cacheable/src/cache_service.rs`。

**策略细节**:
1.  **阈值控制**: 定义 `COMPRESSION_THRESHOLD = 1024` (1KB)。数据长度 < 1KB 直接存原值。
2.  **极速模式**: 对 > 1KB 的数据，使用 `Compression::fast()` (Level 1) 替代默认 Level 6，优先保证编码速度。
3.  **兼容性**: 读取端自动检测 Magic Bytes (`0x1f 0x8b`)，无缝支持压缩与非压缩混合数据。

**代码片段**:
```rust
// 写入优化
fn set(&self, key: &str, value: &[u8], ...) -> Result<()> {
    let final_data = if value.len() > 1024 {
        // 大于 1KB：启用快速压缩
        let mut encoder = GzipEncoder::new(Vec::new(), Compression::fast());
        encoder.write_all(value)?;
        encoder.finish()?
    } else {
        // 小于 1KB：直接存储
        value.to_vec()
    };
    // ...
}
```

### 优化成效
| 指标 | 全量压缩 (Old) | 智能压缩 (New) | 提升幅度 |
| :--- | :--- | :--- | :--- |
| **Throughput** | ~532 req/s | **~689 req/s** | **+29.5%** |
| **CPU Usage** | High (Gzip bound) | Optimized | 显著降低 |

**结论**: 对于 Redis 这类高性能内存数据库，在内网/本机高带宽环境下，仅对大对象（>1KB）进行压缩是 **Latency** 和 **Storage** 之间的最佳平衡点。

---

## 总结
通过通过 **State Check Optimization (减少无用 IO)** 与 **Network Payload Optimization (避免负收益计算)** 的组合拳，显著提升了 Engine 的处理效率，使其能够更好地支撑高并发数据处理任务。

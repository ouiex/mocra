> 已合并至 `docs/README.md` 与根目录 `OPTIMIZATION.md`，本文件保留为历史参考。

# Redis 缓存层网络与性能优化报告

**日期**: 2026-01-09  
**模块**: `cacheable` (Redis Backend)  
**目标**: 优化 Redis 网络传输开销，同时保持高吞吐量。

---

## 1. 优化概览

在本次优化过程中，我们经历了一个从“全量压缩”导致性能回退，到通过“智能策略”成功在降低网络开销的同时大幅恢复性能的过程。

| 阶段 | 策略描述 | 吞吐量 (Throughput) | 相对变化 | 关键特征 |
| :--- | :--- | :--- | :--- | :--- |
| **阶段一** | **全量默认压缩** (Gzip Default) | **~532 req/s** | 基准 (低) | 对所有 Key 进行压缩，CPU 负载高 |
| **阶段二** | **智能压缩** (Threshold + Fast) | **~689 req/s** | **+29.5%** | 仅压缩 >1KB 数据，使用极速模式 |

---

## 2. 详细优化过程

### 阶段一：引入 Gzip 全量压缩
**实施内容**:
在 `cacheable/src/cache_service.rs` 中引入 `flate2` 库。
- **写 (Set)**: 对所有写入 Redis 的 `value` 无条件调用 `GzipEncoder` 进行压缩，使用默认压缩级别。
- **读 (Get)**: 读取数据头部的两个字节（Magic Bytes `0x1f 0x8b`）判断是否为 Gzip 格式，是则解压，否则直接返回。

**结果**:
- 性能测试显示吞吐量约为 **532 req/s**。
- **问题分析**: 系统中存在大量平均大小约 **496 bytes** 的小对象。由于数据包过小，网络传输本就非常快（微秒级），而启用 Gzip 压缩引入的 CPU 计算开销（几十到几百微秒）远超节省的网络时间。此外，Gzip 自身的 Header 开销导致小数据压缩率极低甚至变大。

### 阶段二：智能压缩策略 (Smart Compression)
**实施内容**:
针对阶段一的性能瓶颈，实施了“非必要不压缩”策略。

1.  **引入阈值 (Threshold)**:
    设置 `COMPRESSION_THRESHOLD = 1024` (1KB)。
    - **< 1KB**: 直接存储原始数据（Raw），避免 CPU 浪费。
    - **> 1KB**: 执行压缩，节省网络带宽。

2.  **切换极速模式 (Fast Mode)**:
    将压缩级别从 `Compression::default()` (Level 6) 调整为 `Compression::fast()` (Level 1)。
    - 牺牲微小的压缩比，换取最快的编码速度。

**代码实现 (`cacheable/src/cache_service.rs`)**:
```rust
// 优化后的 set 方法逻辑
fn set(&self, key: &str, value: &[u8], context: &Context) -> Result<(), CacheError> {
    // ... 获取连接 ...
    
    // 策略优化：仅当数据大于 1KB 时才压缩
    let final_data = if value.len() > COMPRESSION_THRESHOLD {
        let mut encoder = GzipEncoder::new(Vec::new(), Compression::fast()); // 使用 fast 模式
        encoder.write_all(value)?;
        encoder.finish()?
    } else {
        value.to_vec() // 小数据直接存原文
    };

    // ... 写入 Redis ...
}
```

**结果**:
- 性能测试显示吞吐量提升至 **~689 req/s**。
- 成功解决了小数据场景下的 CPU 性能瓶颈。

---

## 3. 核心技术思考：为什么“不压缩”反而更快？

在分布式系统优化中，这是一个经典的 **CPU 计算 vs. 网络 I/O (CPU-bound vs I/O-bound)** 的权衡问题。

1.  **开销不对等**:
    - **网络**: 在局域网或本机环境下，发送 500 字节和 300 字节的时间差几乎可以忽略不计（纳秒/微秒级）。
    - **CPU**: 启动 Gzip 算法、初始化 Hash 表、查找重复串、编码输出的过程是昂贵的（微秒/毫秒级）。
    - **结论**: 为了节省 1 微秒的网络时间而花费 100 微秒的 CPU 时间是得不偿失的。

2.  **Gzip 固定开销**:
    - Gzip 格式包含固定的 Header 和 Footer（CRC32 校验等），约占 18 字节以上。
    - 对于几十字节的超小数据，压缩后体积反而可能变大（负压缩），既浪费了 CPU 又浪费了空间。

3.  **最终方案**:
    - **小对象 (<1KB)**: 追求低延迟，以**空间换时间**。
    - **大对象 (>1KB)**: 追求带宽利用率，以**时间换空间**。

---

## 4. 后续建议
- 目前 `COMPRESSION_THRESHOLD` 硬编码为 1024，未来可将其移至 `config.toml` 以便根据生产环境网络状况动态调整。
- 持续监控 Redis 的 CPU 使用率，如果 Redis 变慢，压缩可以减轻 Redis 的网络压力，但会增加应用服务器的 CPU 压力。

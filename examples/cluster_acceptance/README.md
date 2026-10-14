# Three-node cluster acceptance / 三节点集群验收

This example builds the current single `mocra` package and runs three crawler processes,
three embedded Raft voters, one NATS JetStream server, and a controlled HTTP site in Docker.
The runner uses Python's standard library. It deliberately kills crawler processes and
restarts them while recording output, HTTP requests, Raft state, and broker counters.

本示例直接构建当前单包 `mocra`：三个爬虫进程同时组成三个 Raft 投票节点，使用一台
NATS JetStream 共享数据队列。受控 HTTP 服务提供列表页和详情页，脚本通过真实进程
中断和重启验证恢复行为，并保存结果、请求记录、Raft 状态和队列计数。

## Run / 运行

Prerequisites: Docker with Compose, Python 3.9+, and network access to download the build
images, Rust dependencies, and `quotes.toscrape.com`. The build needs Rust 1.95 or later;
the Dockerfile supplies the toolchain and system libraries.

需要 Docker 与 Compose、Python 3.9+，以及拉取镜像、下载 Rust 依赖和访问
`quotes.toscrape.com` 的网络。Dockerfile 提供 Rust 工具链及系统依赖。

From the repository root / 在仓库根目录执行：

```sh
python3 examples/cluster_acceptance/run.py
```

The initial build can take several minutes. Every run creates its own project named
`mocra-06-<id>` and saves its report under `data/cluster_acceptance/<id>/summary.json`.
Host ports are dynamically assigned and bound to `127.0.0.1` only. The API key in the
example is for this local test.

首次构建可能需要数分钟。每次运行使用独立项目 `mocra-06-<id>`，报告保存在
`data/cluster_acceptance/<id>/summary.json`；主机端口动态分配，仅绑定 `127.0.0.1`。
示例 API key 只用于本地验收。

To reuse an image built from the current checkout, name it when building or running:

```sh
python3 examples/cluster_acceptance/run.py --image mocra-cluster:current
python3 examples/cluster_acceptance/run.py --skip-build --image mocra-cluster:current
```

`--skip-build` requires `--image`; use an image containing this checkout's
`cluster_acceptance` example. `--skip-public-site` skips the external quote crawl while
retaining all local fault and recovery scenarios. `--timeout 300` adjusts the wait limit
for each scenario; a timeout fails the run.

`--skip-build` 必须配合 `--image`，并使用当前源码构建的 `cluster_acceptance` 镜像。
`--skip-public-site` 仅跳过公网引文抓取，保留本地故障和恢复测试；
`--timeout 300` 设置各场景等待上限，超时会判定失败。

Optional image overrides / 可选镜像源覆盖：

```sh
MOCRA_ACCEPTANCE_NATS_IMAGE=nats:2.10-alpine python3 examples/cluster_acceptance/run.py
```

## Scenarios / 场景

| Scenario / 场景 | Required evidence / 通过条件 |
| --- | --- |
| Public quotes / 公网引文 | Independently fetch a 100-pair text/author baseline; crawler output matches all 10 source pages without duplicates. / 独立采集 100 组文本与作者，输出和全部 10 页匹配且无重复。 |
| Baseline / 基线 | 600 page/detail pairs, all three nodes download and parse. / 600 组列表与详情，三个节点都参与下载和解析。 |
| Leader failure / 领导者故障 | Kill the elected leader; two survivors elect a leader in a higher term, a follower accepts pause/resume writes, then processes 400 pairs. / 中断实际领导者，余下节点在更高任期选主，从节点接受暂停/恢复写入，再处理 400 组数据。 |
| Worker replay / 工作节点重投 | Kill a follower during an audited HTTP request; observe request redelivery in NATS, a different node repeats that URL, and all 30 pairs complete. / 请求在途时中断从节点，观察 NATS 重投及其他节点重抓同一 URL，30 组数据完整产出。 |
| Worker restart / 工作节点恢复 | Restore the killed node and process 150 pairs across all three nodes. / 重启故障节点，三个节点处理 150 组数据。 |
| Shared blobs / 共享大响应 | 120 pairs with 70 KiB bodies; a separate producer offloads another response to the shared blob volume and a different process parses it. / 120 组 70 KiB 响应；独立生产者卸载另一条响应到共享卷，并由其他进程解析。 |
| Full graceful restart / 全体优雅重启 | All three crawlers exit with code 0; restore the same Raft volumes and applied log positions, then process 90 pairs across three nodes. / 三个爬虫退出码均为 0，使用相同 Raft 卷恢复已应用日志位置，再处理 90 组数据。 |

Each completed phase checks exact output identities and values, run IDs, zero duplicates,
all 15 expected JetStream consumers, and two consecutive samples with zero pending and
unacknowledged broker messages and zero outstanding engine work.

每个阶段校验输出标识与值、运行 ID、零重复、完整的 15 个 JetStream 消费者，并连续
两次确认队列待投递数、未确认数和引擎未完成工作量均为零。结果文件在仓库的
`data/` 下，已被忽略，不应提交原始运行日志和输出。

## Files and cleanup / 文件与清理

- [`../cluster_acceptance.rs`](../cluster_acceptance.rs): crawler, initialization, and blob producer / 爬虫、初始化和大响应生产者。
- [`compose.yml`](compose.yml), [`Dockerfile`](Dockerfile), [`node.toml`](node.toml): topology, build, and engine configuration template / 拓扑、构建和引擎配置模板。
- [`run.py`](run.py): isolated setup, fault injection, validation, and cleanup / 隔离环境、故障注入、校验与清理。
- [`site.py`](site.py): deterministic HTTP corpus and audit / 受控 HTTP 数据与请求记录。

By default the runner removes only its own containers, network, and named volumes, including
on failure. Reports and logs remain on disk; built images are retained for reuse.
`--keep` retains this project's containers and volumes for inspection. To remove a retained
run, use its project name and result directory from `summary.json`:

默认结束时（包括失败）只移除本次项目的容器、网络和命名卷；保留磁盘报告、日志和
构建镜像。`--keep` 保留本次环境供观察；需要清理时，将下面的值替换为报告中的实际
项目、目录和镜像名：

```sh
MOCRA_ACCEPTANCE_PROJECT="mocra-06-<id>" \
MOCRA_ACCEPTANCE_RUN_DIR="$PWD/data/cluster_acceptance/<id>" \
MOCRA_ACCEPTANCE_IMAGE="mocra-06-<id>:local" \
docker compose -p "mocra-06-<id>" -f examples/cluster_acceptance/compose.yml down --volumes
```

The test covers process failures on one Docker host with one broker. It does not cover
host loss, network partitions, broker replication, or establish exactly-once delivery;
the documented local stage-handoff crash-loss window still applies.

测试范围是同一 Docker 主机上的爬虫进程故障和单个 broker，不覆盖主机故障、网络
分区和 broker 多副本，也不能证明 exactly-once；文档中的本地阶段交接崩溃丢失窗口
仍然适用。

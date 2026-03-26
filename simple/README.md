# simple: Engine + ModuleNodeTrait + DAG registration

这个目录给出一个最小可读示例，展示三件事：

1. 如何初始化 `Engine`
2. 如何实现 `ModuleNodeTrait`
3. 如何通过 `ModuleTrait::add_step` 注册到 DAG（线性兼容 DAG）
4. 如何定义多条链路 DAG（分叉 + 汇合）

## 文件说明

- `module_node_trait_dag.rs`：完整示例代码（包含线性 DAG 和多链路 DAG）

## 关键流程

1. 使用配置文件初始化 `State`
2. 构建 `Engine`
3. 实现一个或多个 `ModuleNodeTrait` 节点
4. 在 `ModuleTrait::add_step` 中返回节点顺序（引擎会按 DAG 线性兼容路径处理）
5. `engine.register_module(...)` 完成模块注册
6. （可选）用 `ModuleDagOrchestrator::compile_linear_compat(...)` 验证线性 DAG 节点生成
7. （可选）用 `ModuleDagOrchestrator::compile_definition(...)` 编译显式多链路 DAG

## 多链路 DAG 结构

示例中的显式 DAG 结构如下：

`start -> branch_a`

`start -> branch_b`

`branch_a -> merge`

`branch_b -> merge`

## 运行提示

- 示例默认读取 `tests/config.mock.pure.engine.toml`，仅用于演示。
- 生产使用请替换为你自己的配置文件路径。
- 若你只想快速验证编译逻辑，可先运行到 `compile_linear_compat`，无需启动完整处理循环。

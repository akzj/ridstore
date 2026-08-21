# ridstore 设计文档索引

ridstore 当前定位为嵌入式、单机、单目录独占的 Stable-ID Log-Structured Record Store Library。

文档按约束优先级阅读：

1. [总体设计](design.md)：项目目标、核心不变量和模块关系；
2. [与 LSM/RocksDB 的定位边界](positioning-vs-lsm.md)：明确不研发通用有序 KV；
3. [嵌入式 Library 与 API 契约](api-contract.md)：进程模型、公开接口、LogicalRevision、乐观冲突和数据所有权；
4. [磁盘与二进制格式](on-disk-format.md)：目录、Segment、Frame、Manifest、版本和校验；
5. [Commit 与 Recovery 协议](commit-recovery-protocol.md)：Batch、group commit、Abort、CommitUnknown 和恢复；
6. [Mapping 设计](mapping-design.md)：Delta Overlay、持久化 Radix、Checkpoint 和缓存；
7. [GC 协议](gc-protocol.md)：Relocation、CAS、Reader Pin、Retire 和删除；
8. [SegmentStats 设计](segment-stats-design.md)：Checkpoint 批量统计、恢复上界和 GC 候选；
9. [配置、预算与 Backpressure](runtime-config.md)：持久化硬限制、内存预算和 admission；
10. [验证计划](verification-plan.md)：故障注入、属性测试、基准与门禁；
11. [实施计划](implementation-plan.md)：模块依赖、迭代顺序和每阶段完成定义。

## 文档状态

| 文档 | 状态 | 作用 |
|---|---|---|
| `design.md` | Accepted architecture | 总体边界与不变量 |
| `positioning-vs-lsm.md` | Accepted boundary | 防止漂移成 RocksDB 替代品 |
| `api-contract.md` | Development contract v1 | 第一版 Library API |
| `on-disk-format.md` | Format draft v1 | 实现前冻结；当前不承诺向后兼容 |
| `commit-recovery-protocol.md` | Development contract v1 | 提交与恢复状态机 |
| `mapping-design.md` | Development contract v1 | 第一版和目标 Mapping 架构 |
| `gc-protocol.md` | Development contract v1 | GC 安全协议 |
| `segment-stats-design.md` | Development contract v1 | Segment 统计与恢复协议 |
| `runtime-config.md` | Development contract v1 | 配置、资源预算与 Backpressure |
| `verification-plan.md` | Acceptance contract v1 | 测试证据要求 |
| `implementation-plan.md` | Execution plan v1 | 开发推进顺序 |

磁盘格式只有在 Phase 0 的 golden vectors、decoder fuzz 和 crash harness 通过并单独提交 Format Freeze 后，才成为兼容性承诺。在此之前可以修改，但必须同步更新所有协议文档和测试向量。

## 当前完整性结论

设计已闭合到可以开始 **Phase 0 格式与 Harness 开发**：Frame/Node/Manifest/Journal 有字节级边界，Commit/Recovery、Root/SegmentStats、GC 删除和 Delta admission 有对应失败时序，锁协议不要求在发布临界区执行磁盘 I/O。

仍可由实现基准选择而不改变契约的内容包括 Delta shard 数、Node Cache 的 CLOCK/SLRU 具体策略、I/O buffer 大小和后台调度权重。它们不得改变持久化格式、Batch 原子性、内存 hard limit、recovery 结果或 GC 删除门禁。

“可以开发”不等于“格式已冻结”或“可以生产”：Phase 0 必须先用 golden vectors、decoder fuzz、INITIALIZING/Journal/Manifest crash matrix 证明当前 draft；若证据迫使格式变化，应在 Format Freeze 前回改文档和 vectors。

## 冲突处理

文档冲突时遵循：

```text
项目定位边界
> 核心不变量
> Commit/Recovery 与 GC 安全协议
> 磁盘格式
> API 便利性
> 性能优化
```

任何性能优化都不能覆盖 durability、Batch 原子性、ID 不复用或 GC 安全约束。

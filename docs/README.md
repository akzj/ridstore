# ridstore v2 设计文档索引

当前代码只有 v2 运行路径。公开 API 直接调用 `internal/engine`；Format v1 的 runtime、内部模块、命令和
测试已在 M6 删除。所有新设计与实现必须遵守“不增加 adapter、fallback、dual-write 或双格式分支”。

## 当前约束文档

1. [总体架构](v2-architecture.md)：分层、唯一所有者和 RecordLog 边界；
2. [公开 API 与一致性](v2-api-contract.md)：Stable ID、VersionToken 与上层职责；
3. [RecordLog](v2-recordlog-contract.md)：统一 Append、物理格式、batching 和 Segment 生命周期；
4. [Record Protocol](v2-record-protocol.md)：Put、CommitGroup、Reserve 与 Abort record；
5. [Manifest](v2-manifest-format.md)：唯一 Catalog schema 和原子安装；
6. [Mapping Format](v2-mapping-format.md)：immutable radix node 和 Mapping Segment；
7. [Delta Admission](v2-delta-admission.md)：内存 hard limit、reservation 和 Checkpoint 时序；
8. [Checkpoint Builder](v2-checkpoint-builder.md)：有界构建与 Root 发布；
9. [Recovery](v2-recovery-protocol.md)：权威状态、replay 和崩溃时间线；
10. [Maintenance Marker](v2-maintenance-format.md)：Data GC 不可逆边界和恢复；
11. [Fault Matrix](v2-fault-matrix.md)：当前故障注入覆盖；
12. [M6 Review](v2-m6-review.md)：公开切换、v1 删除边界与验证证据。

阶段 Review：

- [M1](v2-m1-review.md)：codec、物理格式与 Catalog；
- [M2](v2-m2-review.md)：RecordLog、rotation 与 Reader Pin；
- [M3](v2-m3-review.md)：Transaction、Coordinator 与最小 Engine；
- [M4](v2-m4-review.md)：Persistent Mapping、Replay 与 Checkpoint；
- [M5](v2-m5-review.md)：Relocation、退休证明与物理清理；
- [模块处置矩阵](v2-module-disposition.md)：M0-M6 的最终去留。

## 历史文档

未以 `v2-` 开头的设计及 `phase-*.md` 记录 Format v1 的历史决策和曾经验证过的性质。它们只用于提取
经验，不是当前 API、格式、模块名或生产能力的事实来源。尤其其中的 Revision、旧命令、旧 metrics、
Backup/Verify/Soak 实现均已删除，后续能力必须按 v2 原生重写。

## 当前完整性

v2 已具备 Create/Open、CRUD、原子 Batch、group commit、Checkpoint、Replay、CommitUnknown 查询、
有界 GC 候选选择、Relocation、安全退休和崩溃恢复。它仍不是 production-ready：状态保留上界、
ENOSPC/backpressure、v2 原生 Verify/Backup/Metrics、并发模型测试和长期 soak 是后续完整性工作。

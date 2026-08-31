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
13. [磁盘空间 Admission](v2-space-admission.md)：用户写入水位、控制面 headroom 与 ENOSPC 边界。
14. [v2 Offline Verify](v2-verify-design.md)：只读审计边界、证据分层与实现阶段。
15. [v2 Mapping GC](v2-mapping-gc-design.md)：Root 全量重建、文件集原子替换与崩溃恢复。
16. [v2 Backup / Restore](v2-backup-restore-design.md)：离线全量 artifact、精确文件集与原子发布协议。
17. [v2 Durable Benchmark Harness](benchmark.md)：可归档的 ridstore/raw-fsync 基线与跨引擎待办边界。
18. [v2 RecordRef 与实时 SegmentStats](v2-record-ref-live-stats.md)：Mapping 精确物理长度、实时死亡速度与多 Segment Compaction 基础。

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
Backup/Soak 的旧实现均已删除；Verify 与 Backup/Restore 已按 v2 格式独立重写，其他后续能力不得复用
v1 协议。`metrics/prometheus`、long-fuzz runner、soak harness 与只读 migration planner 已按 v2
runtime 重新实现；当前没有需要执行的历史数据迁移 step。

## 当前完整性

v2 已具备 Create/Open、CRUD、原子 Batch、group commit、Checkpoint、Replay、CommitUnknown 查询、
有界状态恢复、用户写入磁盘水位、Data/Mapping GC、崩溃恢复、原生只读 Verify，以及 Linux 上的离线全量
Backup/Restore、有界 Metrics/Prometheus adapter，以及 long-fuzz/nightly 与 soak harness。它仍不是
production-ready：未来格式的实际 Migration step、long-fuzz/72h soak 自然结束证据、真实 workload
基线和远端备份传输是后续完整性工作。

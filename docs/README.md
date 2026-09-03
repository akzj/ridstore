# ridstore v2 文档索引

仓库只保留当前 v2 架构、持久化协议和可执行的验证/运维文档。历史阶段报告、Format v1 契约及已经完成的迁移计划不再随主分支维护；需要追溯时使用 Git 历史。

## 架构与运行时

1. [总体架构](v2-architecture.md)：分层、组件所有权和端到端数据流。
2. [API 与一致性](v2-api-contract.md)：Stable ID、VersionToken、Batch 和一致性边界。
3. [并发模型](v2-concurrency-model.md)：锁、admission fence、reader pin 与后台并发规则。
4. [Maintenance Scheduler](v2-maintenance-scheduler.md)：typed worker、阶段转换、资源和依赖所有权。
5. [运行时配置](runtime-config.md)：内存、磁盘、Checkpoint、GC 和 backpressure 参数。
6. [Metrics Export](metrics-export.md)：运行时指标与 Prometheus 映射。

## 持久化协议

1. [RecordLog](v2-recordlog-contract.md)：物理追加、rotation、reader pin 和 durable writer 边界。
2. [Record Protocol](v2-record-protocol.md)：Put、CommitGroup、Reserve 与 Abort 编码。
3. [Manifest](v2-manifest-format.md)：唯一 durable Catalog 与 generation 安装规则。
4. [Mapping](v2-mapping-format.md)：immutable radix node、Delta 与 Root。
5. [Delta Admission](v2-delta-admission.md)：内存 hard limit、reservation 与 pressure generation。
6. [Checkpoint Builder](v2-checkpoint-builder.md)：有界 COW 构建与 Root 发布。
7. [RecordRef 与 SegmentStats](v2-record-ref-live-stats.md)：精确物理长度和 live accounting。
8. [Recovery](v2-recovery-protocol.md)：权威状态、replay 和崩溃收敛。
9. [Maintenance Marker](v2-maintenance-format.md)：Data GC 不可逆边界与恢复。

## GC 与容量

1. [Data GC](gc-protocol.md)：候选、copy/CAS、Checkpoint proof、retirement 和删除协议。
2. [Mapping GC](v2-mapping-gc-design.md)：Root 重建、generation 切换和旧文件回收。
3. [磁盘空间 Admission](v2-space-admission.md)：用户写入水位和维护 headroom。

## 验证与运维

1. [Durable Fault Matrix](v2-fault-matrix.md)：当前 writer 的 syscall/fault/crash 覆盖。
2. [Offline Verify](v2-verify-design.md)：只读审计边界和证据分层。
3. [Backup / Restore](v2-backup-restore-design.md)：离线全量备份、恢复和原子发布。
4. [Format Migration](format-migration.md)：只读 planner 与当前“不迁移旧格式”边界。
5. [Durable Benchmark](benchmark.md)：可归档 benchmark 入口及结论限制。
6. [Long Fuzz](long-fuzz.md)：nightly runner 与长期证据要求。
7. [Steady-State Soak](soak.md)：长期运行、收敛和结果判定。
8. [与 LSM/RocksDB 的定位边界](positioning-vs-lsm.md)：产品范围和非目标。

## 当前完整性

v2 已具备 Create/Open、CRUD、原子 Batch、group commit、Checkpoint、Replay、有限状态恢复、磁盘水位、Data/Mapping GC、崩溃恢复、离线 Verify、Backup/Restore、Metrics，以及 fuzz/soak/benchmark harness。

这不等于 production-ready：长期 soak/fuzz、设备 power-loss、异机恢复演练和真实 workload 性能门禁仍需独立证据。文档中的“已实现”只描述代码与自动化测试状态，不替代这些运行证据。

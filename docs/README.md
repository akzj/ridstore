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
11. [实施计划](implementation-plan.md)：模块依赖、迭代顺序和每阶段完成定义；
12. [Offline Verify/Scrub](verify-scrub.md)：只读完整性验证、报告与资源边界；
13. [Consistent Backup/Restore](backup-restore.md)：离线快照、artifact 与 UUID 策略；
14. [Metrics Export](metrics-export.md)：稳定 samples 与 Prometheus adapter；
15. [Format Upgrade/Migration](format-migration.md)：兼容规则、只读 plan 与 step 门禁；
16. [Format v1 Freeze Review](format-freeze-review.md)：格式冻结结论、代码映射和验证边界；
17. [Phase 5 全局审计](phase-5-audit.md)：requirement-to-evidence、当前 P0/P1 缺口和 production checklist。
18. [72h Soak](soak.md)：长时工作负载、JSONL 资源样本和自然结束判定。
19. [Durable Writer Syscall Fault Matrix](syscall-fault-matrix.md)：逐 writer 的 write/fsync/rename/dir-sync 覆盖与剩余缺口。

## 文档状态

| 文档 | 状态 | 作用 |
|---|---|---|
| `design.md` | Accepted architecture | 总体边界与不变量 |
| `positioning-vs-lsm.md` | Accepted boundary | 防止漂移成 RocksDB 替代品 |
| `api-contract.md` | Development contract v1 | 第一版 Library API |
| `on-disk-format.md` | Format v1 frozen | Phase 0 后的兼容性边界 |
| `commit-recovery-protocol.md` | Development contract v1 | 提交与恢复状态机 |
| `mapping-design.md` | Development contract v1 | 第一版和目标 Mapping 架构 |
| `gc-protocol.md` | Development contract v1 | GC 安全协议 |
| `segment-stats-design.md` | Development contract v1 | Segment 统计与恢复协议 |
| `runtime-config.md` | Development contract v1 | 配置、资源预算与 Backpressure |
| `verification-plan.md` | Acceptance contract v1 | 测试证据要求 |
| `implementation-plan.md` | Execution plan v1 | 开发推进顺序 |
| `format-freeze-review.md` | Frozen 2026-08-21 | Phase 0 全局 Review 与证据边界 |
| `phase-1-review.md` | Passed 2026-08-21 | 最小 durable Record Store Review |
| `phase-2-review.md` | Passed 2026-08-21 | 并发与 group commit Review |
| `phase-3-review.md` | Passed 2026-08-21 | Persistent Mapping Review |
| `phase-4-review.md` | Passed 2026-08-21 | Data GC Review 与剩余运维边界 |
| `verify-scrub.md` | Phase 5 implemented | 离线只读验证与 scrub report |
| `backup-restore.md` | Phase 5 protocol v1 | 离线一致备份、恢复发布与 UUID 策略 |
| `metrics-export.md` | Phase 5 implemented | bounded samples 与 Prometheus HTTP adapter |
| `format-migration.md` | Phase 5 skeleton | 只读 plan、registry 与 copy-on-write 门禁 |
| `phase-5-audit.md` | Phase 5 in progress | 全局证据矩阵与生产声明门禁 |
| `syscall-fault-matrix.md` | Phase 5 in progress | durable writer syscall 错误覆盖与缺口 |
| `soak.md` | Harness implemented | 72h steady-state 运行协议；自然结束证据仍未完成 |

磁盘格式已在 Phase 0 的 golden vectors、decoder fuzz 和初始化 crash harness 通过后冻结。后续非兼容修改必须提升 major version 并提供离线迁移。

## 当前完整性结论

Phase 0 格式与 Harness、Phase 1 最小 durable Record Store、Phase 2 并发与 group commit、Phase 3 Persistent Mapping、Phase 4 Data GC 已完成阶段 Review；Review 见 [phase-1-review.md](phase-1-review.md)、[phase-2-review.md](phase-2-review.md)、[phase-3-review.md](phase-3-review.md) 与 [phase-4-review.md](phase-4-review.md)。当前进入 **Phase 5 完整性与运维**；长期验证尚未完成，不能声明 production-ready。

仍可由实现基准选择而不改变契约的内容包括 Delta shard 数、Node Cache 的 CLOCK/SLRU 具体策略、I/O buffer 大小和后台调度权重。它们不得改变持久化格式、Batch 原子性、内存 hard limit、recovery 结果或 GC 删除门禁。

“格式已冻结”和 GC 主路径通过都不等于“可以生产”：Verify/Scrub、Backup/Restore、迁移与长期验证仍由 Phase 5 完成。

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

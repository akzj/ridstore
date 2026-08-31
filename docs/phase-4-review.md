# Phase 4 Data GC 全局 Review

状态：通过，可以进入 Phase 5 完整性与运维；不构成 production-ready 声明。

Review 日期：2026-08-21。

## 1. 完成范围

Phase 4 已闭合单 Segment Data GC 的真实删除主路径：

1. Checkpoint 生成 exact SegmentStats 和 replay-safe 候选边界；
2. Candidate 只负责排序，删除仍以 `Mapping[ID] == scanned VAddr` 为唯一 live 判定；
3. Reader pin、Cleaning、Retired 与 tombstone 阻止 lookup/acquire/delete 竞态，Open Batch ref 由复制重定向收敛；
4. live PutRecord 复制 Value 并保留 OriginBatchID/LogicalRevision；
5. Relocation Descriptor 与用户 Commit 共享 CommitSeq、fsync 和 deterministic expected-old CAS；
6. post-relocation Checkpoint 的 exact zero-live stats 证明源 Segment 已无 Mapping 引用；
7. GC-required Checkpoint 覆盖全部 Relocation，并把嵌套 Mapping rotation 记录进同一个 DataGC Journal；
8. Retire、pin drain、Manifest remove、rename-to-trash、unlink 和目录 fsync 按可恢复顺序执行；
9. Open 对 Journal Phase 1–3 安全放弃清理，对 Phase 4–7 验证并完成既定删除；
10. 两段磁盘空间 admission、Delta hard-limit reservation、有限 GC Batch、前台队列优先级和带宽 pacing 提供资源边界。

公开维护入口为 `CompactData(ctx)`，一次至多清理一个 sealed Segment。第一版没有隐藏后台 scheduler；上层可以依据 Metrics 和自身维护窗口显式调度。

## 2. 删除不变量与代码门禁

删除源文件前必须同时成立：

- RelocationSeal 已 durable，runtime/recovery CAS 结果一致；
- post-relocation Checkpoint 的 exact stats 对 source 为零；
- durable Manifest 的 Mapping Root、CoveredCommitSeq、Stats 与 ReplayStart 覆盖本轮 Relocation；
- Reader pin 已归零，Open Batch 已不再持有 source 地址；
- Manifest 已移除 source；
- source 已移入 trash，data/trash 目录已 fsync；
- unlink 后 trash 目录再次 fsync。

SegmentStats、dead ratio、Metrics 和文件年龄均不授权删除。任何校验失败都保留源 Segment；MappingCheckpointDurable 之后的错误使 Store fail closed，由 Open 继续 Journal，而不是伪装回滚。

## 3. 嵌套 Mapping rotation

此前 DataGC phase 3 的 Checkpoint 若写满 Active Mapping Segment，会与独立 Mapping rotation 争用 `MAINTENANCE` 并安全返回冲突，但大型 Checkpoint 无法完成。当前协议已经修复：

- Mapping old/new file refs 追加到父 DataGC Journal；
- Journal file-ref identity 为 `(Kind, FileID)`，状态只允许 Temporary→Active/Sealed/Trash、Active→Sealed/Trash、Sealed→Trash；
- Checkpoint 安装后重新加载 durable 父 Journal，再推进 DataGC phase，避免覆盖 rotation refs；
- Open 在构造 Mapping 前完成所有已记录的嵌套 rotation；
- crash 留下的 partial、尚未被 Manifest 引用的 destination active file 可按 Journal 身份安全重建。

普通 Mapping Checkpoint、Mapping GC 与 Data GC 仍由 `checkpointMu` 串行；Data Segment rotation 使用独立短 `ROTATION` journal。

## 4. 并发与资源结果

- 已排队用户 Commit 优先于下一批 GC Relocation；已选中的单个有限 Relocation Batch 保持原子，不被中途拆分；
- Relocation 与用户 Put 的两种 CommitSeq 顺序都由 expected-old CAS 决定，GC 不覆盖较新的用户值；
- GC 不持有 Store 全局 operation lock；Close 通过 lifecycle admission + active-operation drain 等待 GC 到可恢复完成点；
- Context/ENOSPC 在 MappingCheckpointDurable 前撤销 Cleaning、移除 Journal 并保留 source；之后 fault closed；
- Journal 前的 copy admission 与 checkpoint barrier 后按 frozen Delta entry 数计算的 COW admission 分离，避免漏算 GC 期间进入 cut 的用户 Commit；
- `GCBytesPerSecond` 在每个 durable Relocation Batch 后做 Context-aware pacing，累计 throttled nanos 可观测；
- 重复 overwrite + `CompactData` 集成测试证明 sealed Segment 数量下降且最新值与 LogicalRevision 保持正确。

## 5. 自动化证据

本阶段 Review 时以下命令通过：

```text
go test ./...
go test -race ./...
go vet ./...
git diff --check
```

SIGKILL subprocess matrix 覆盖：

- RelocationPart written、RelocationSeal written、Relocation fsync、Relocation Mapping published；
- DataGC Prepared、Copying、RelocationsDurable、MappingCheckpointDurable、Retired、Manifest removed、Trashed、Deleted；
- DataGC 内嵌 Mapping rotation 的 Prepared、OldSealed、NewCreated、ManifestInstalled。

其他自动化证据覆盖 reader pin wait、lookup/acquire/retire state machine、Open Batch ref、并发用户 Put、取消前后 durable boundary、空间 admission、ENOSPC 错误分类、Close/GC 协调、重复空间收敛、重启后数据与 Revision 一致。

## 6. 已知边界

以下不属于 Phase 4 正确性主路径已完成的声明，必须在 Phase 5/生产声明前继续处理或如实保留：

- 可用磁盘查询不是配额预留；通过 admission 后仍可能因其他写入得到真实 ENOSPC；
- 当前有 durable-boundary SIGKILL 与代表性错误注入，但尚未完成每一个 write/fsync/rename/permission syscall 的系统化 fault matrix；
- 尚无 metrics exporter adapter、offline verify/scrub、backup/restore 和 migration skeleton；
- 尚未执行长期 fuzz、72 小时 soak、同 durability RocksDB/Pebble benchmark；
- 当前只有显式 `CompactData`，没有自动水位 scheduler；这不影响内核协议，但部署层必须主动调度和告警；
- 生产写停止水位尚未实现；极端磁盘耗尽仍需上层 admission/运维预留，Phase 5 全局审计前不能宣称 production-ready。

## 7. Review 结论

Phase 4 的核心问题不是“能否复制文件”，而是“何时有资格删除旧文件”。当前实现已把资格收敛为可恢复的 Mapping、Checkpoint、Pin、Manifest 与目录持久化证据，并对嵌套 Mapping rotation、并发用户写和资源不足 fail closed。

因此可以进入 Phase 5，但生产就绪仍未证明。

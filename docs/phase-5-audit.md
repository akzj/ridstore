# Phase 5 全局审计与生产门禁

状态：进行中；禁止声明 production-ready

审计基线：2026-08-22，`dd4f577`

本文不是阶段通过报告，而是 Phase 5 的 requirement-to-evidence 台账。只有“完成”项可以作为当前结论；“局部证据”和“未完成”都不能被测试全绿替代。

## 1. Phase 5 交付矩阵

| 计划项 | 当前状态 | 当前证据 | 尚缺证据 |
|---|---|---|---|
| Offline Verify/Scrub | 完成 | `internal/verify`、`ridstore-tool verify`、corruption/lease/active-tail tests | 超大 Store 的 external-sort verifier 不在 v1；当前 O(live IDs) 内存限制已记录 |
| 一致 Backup | 完成 | 同一 source lease、hash metadata、payload Verify、SIGKILL matrix | 远端传输、压缩、加密不在 v1 |
| Restore/UUID 策略 | 完成 | 新目录发布、RESTORING marker、默认新 UUID、preserve 显式开关、SIGKILL matrix | 应用级异机演练尚未执行 |
| Metrics adapter | 完成 | 固定 bounded samples、Prometheus adapter tests | dashboard/告警属于部署层 |
| Migration skeleton | 完成 | 只读 planner、registry、非 v1 明确 `ErrUnsupported` | 没有可执行跨版本迁移；skeleton 不代表升级路径已存在 |
| Full crash/fault matrix | 局部证据 | Initialize、Commit、Reserve、Abort、Rotation、Checkpoint、Mapping/Data GC、Backup/Restore 的 process SIGKILL；Manifest/CURRENT syscall-error matrix | Journal、Segment、GC trash/delete 等全部 write/fsync/rename/dir-sync/permission/ENOSPC 尚未系统覆盖；没有 power-loss 设备证据 |
| Long fuzz/nightly | 未完成 | 每个 decoder 的 2s smoke gate | 长时 fuzz 未自然结束，也没有 nightly 产物 |
| 72h steady-state soak | 未完成 | 现有短收敛测试 | 尚无 72h harness 报告、资源时间序列或自然结束证据 |
| Same-durability benchmark | 未完成 | 单一 ridstore durable commit benchmark | 无 append baseline、Pebble/RocksDB 同 durability 稳定态对比和原始报告 |
| Known limits/checklist | 进行中 | 本文 | 下列 P0/P1 未关闭，最终 Review 尚未完成 |

## 2. 当前实现审计发现

### 已修复：Open recovery 全量扫描 Data Log

基线审计发现 `recovery.RecoverIntoScanners` 依次扫描所有 sealed Segment 和 Active Segment；即使 Frame 位于 `ReplayStart` 前，也先把每个 PutRecord 加入 `puts map[VAddr]putRecord`，再跳过 replay。这会导致：

- Open 时间与全部历史 Data bytes 相关，而不是 Root 后 replay window；
- 恢复瞬时内存与全部历史 PutRecord 数相关；
- 与 Phase 3“从 Persistent Root 启动、不全量加载”的验收结论不一致。

当前实现已改为 lazy sealed envelope open，只顺序扫描 ReplayStart 所在 Segment及其后；切点时 Open Batch 的 Commit Descriptor 或 Relocation 引用更早 PutRecord 时，通过受校验的随机 VAddr reader 读取完整 Frame。自动化测试证明 pre-replay Segment 的 `Scan` 次数为 0、被引用 Record 精确随机读取一次，offline Verify 仍严格全扫。恢复不再保存全历史 Put 元数据。

### 已修复：Batch Status retention 无界

基线审计发现运行时 `Store.statuses` 只增不减，Recovery 的 `Result.Statuses` 同样保存 ReplayStart 后全部终态；大量空 Batch 可以绕过 Delta bytes 限制持续增长内存。

当前实现增加 runtime `StatusRetention`（默认 65,536，且不小于 `MaxOpenBatches`）：resolved 状态按完成顺序有界保留，旧 ID 返回 `ErrStatusExpired`，CommitUnknown 钉住。每个 Open Batch/内部 Relocation 预留 terminal slot，75% 请求 Checkpoint，硬上限 backpressure；Checkpoint 使用 barrier 前保守计数，只在 Manifest durable 后释放覆盖容量。Recovery 以相同上限精确保留 terminal BatchID，超过返回 `ErrStatusCapacity`，重复 Commit/Abort 仍判 corruption；Descriptor Part 也改为单个连续且有界集合。测试覆盖缓存逐出、Unknown 钉住、容量推进 Checkpoint、GC 达限撤销后重试收敛、恢复上限、重复终态以及 Close 广播唤醒。

### P0：系统化 syscall fault coverage 未闭合

Manifest/CURRENT 已覆盖 write、file sync、rename、directory sync 的 `ENOSPC`/`EIO`/`EACCES` 注入，并在 publication outcome 不确定时要求 fresh Open。其他 durable writer 仍主要依赖操作完成后的 phase failpoint；它们证明 process-crash 恢复，但不等价于真实 syscall 返回错误的传播与清理。

Active Data 主写路径现已增加 append/sync、seal/footer write+sync、rename 和 data directory sync 注入点。单元矩阵证明每个 seal 边界都能经 fresh recovery 收敛为严格 immutable Segment；Store 级测试证明 descriptor write 错误确定未提交、Seal 后 sync 错误返回 CommitUnknown、两者均保留 `EIO` cause 并 fail closed。Rotation Journal 与 Maintenance Journal 的 install/remove write、sync、rename/remove、directory sync 也已覆盖三类错误；Open 会删除并 fsync 未发布 regular temp、拒绝 symlink，并幂等完成已发布 Journal。

Maintenance Journal 另按 Data GC phase 验证所有七次 directory-sync publication：phase 1–3 失败且 Manifest 尚未证明 GC checkpoint 时允许撤销；Mapping Checkpoint Manifest 一旦 durable，运行时立即 fail closed，即使 phase-4 Journal rename 尚未成功，fresh Open 也会用 `MaintenanceGeneration`、精确 SegmentStats 和 ReplayStart 的共同证据补写 phase 4 后继续删除。嵌套 Mapping rotation 虽可提前推进相同 MaintenanceGeneration，但旧 source 仍在 SegmentStats 时只能撤销，不能误判为 GC checkpoint。checkpoint 前 Journal cleanup 自身失败也会 fail closed，由 fresh Open 收敛。完整 writer 清单与剩余缺口见 `syscall-fault-matrix.md`；Mapping、Data GC trash/delete、Initialize 和 Backup/Restore 尚未闭合，因此本 P0 仍保持未完成。

### P1：磁盘耗尽停止水位

GC 有 copy/checkpoint admission 与真实 ENOSPC 传播，但没有独立的前台写停止水位或保留空间配额。极端磁盘耗尽仍依赖部署层预留和告警。

### P1：长时与对比证据

72h soak、长期 fuzz、power-loss、异机 restore drill 和同 durability benchmark 都必须自然完成并保存原始产物。当前本机短测试不能替代这些结论。

## 3. 当前可重复门禁

```text
make test
make test-race
make vet
make test-fuzz-smoke
make test-crash
make test-integration
make bench
make verify
```

`make verify` 聚合普通、race、vet、fuzz smoke、process-crash 和 integration。它不包含 benchmark、long fuzz、72h soak 或 power-loss，因此成功只能证明当前开发门禁通过。

## 4. Production checklist

- [x] Format v1 frozen，golden/decoder fuzz smoke 可重复；
- [x] Commit/Recovery、Checkpoint、Mapping/Data GC 主路径有阶段 Review；
- [x] Offline Verify、Backup/Restore、Metrics、Migration planner 已实现；
- [x] 本机 `make verify` 通过；
- [x] Open recovery 不再全量扫描/保存历史 PutRecord；
- [x] Status retention 与 recovery transient memory 有明确上界；
- [ ] 所有 durable writer 完成 syscall error matrix；
- [ ] write-stop/运维磁盘水位策略完成或由明确部署契约承接；
- [ ] long fuzz/nightly 自然结束且无未解释 failure；
- [ ] 72h steady-state soak 自然结束，空间/RSS/FD/goroutine 收敛；
- [ ] same-durability append/Pebble/RocksDB 对比保存原始结果；
- [ ] 独立目录及异机 Backup/Restore drill 完成；
- [ ] 支持的文件系统/设备完成 power-loss 验证，或产品声明明确排除；
- [ ] 最终全局锁顺序、格式兼容、取消清理和完整 diff Review 通过。

在所有勾选项关闭前，README、release note 和部署文档都不得使用 production-ready 表述。

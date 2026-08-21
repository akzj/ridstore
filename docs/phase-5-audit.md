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

### P0：Open recovery 仍全量扫描 Data Log

`recovery.RecoverIntoScanners` 当前依次扫描所有 sealed Segment 和 Active Segment；即使 Frame 位于 `ReplayStart` 前，也先把每个 PutRecord 加入 `puts map[VAddr]putRecord`，再跳过 replay。这会导致：

- Open 时间与全部历史 Data bytes 相关，而不是 Root 后 replay window；
- 恢复瞬时内存与全部历史 PutRecord 数相关；
- 与 Phase 3“从 Persistent Root 启动、不全量加载”的验收结论不一致。

修复方向必须保留旧 payload 引用语义：切点时 Open Batch 的 Commit Descriptor 可以在 ReplayStart 后引用切点前 PutRecord，Relocation 也可以引用旧 VAddr。因此不能简单丢弃旧 Segment；应跳过 replay segment 之前的顺序扫描，并通过受校验的随机 VAddr reader 按 Descriptor 引用读取 PutRecord。恢复只保存未完成 Descriptor 与有界近期 Status，不保存全历史 Put 元数据。

### P0：Batch Status retention 无界

运行时 `Store.statuses` 当前是只增不减的 map；`Batch.finish` 对每个终态写入，尚未实现设计文档要求的有界近期状态表。Recovery 的 `Result.Statuses` 同样会保存 ReplayStart 后全部终态。长时间运行或大量空 Batch 可以绕过 Delta bytes 限制，持续增长内存。

修复必须同时定义：近期状态容量、`ErrStatusExpired` 边界、CommitUnknown 不被提前逐出、Checkpoint cut 前后 Status 语义，以及 Recovery 重复终态检测不能因简单 eviction 被削弱。

### P0：系统化 syscall fault coverage 未闭合

Manifest/CURRENT 已覆盖 write、file sync、rename、directory sync 的 `ENOSPC`/`EIO`/`EACCES` 注入，并在 publication outcome 不确定时要求 fresh Open。其他 durable writer 仍主要依赖操作完成后的 phase failpoint；它们证明 process-crash 恢复，但不等价于真实 syscall 返回错误的传播与清理。

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
- [ ] Open recovery 不再全量扫描/保存历史 PutRecord；
- [ ] Status retention 与 recovery transient memory 有明确上界；
- [ ] 所有 durable writer 完成 syscall error matrix；
- [ ] write-stop/运维磁盘水位策略完成或由明确部署契约承接；
- [ ] long fuzz/nightly 自然结束且无未解释 failure；
- [ ] 72h steady-state soak 自然结束，空间/RSS/FD/goroutine 收敛；
- [ ] same-durability append/Pebble/RocksDB 对比保存原始结果；
- [ ] 独立目录及异机 Backup/Restore drill 完成；
- [ ] 支持的文件系统/设备完成 power-loss 验证，或产品声明明确排除；
- [ ] 最终全局锁顺序、格式兼容、取消清理和完整 diff Review 通过。

在所有勾选项关闭前，README、release note 和部署文档都不得使用 production-ready 表述。

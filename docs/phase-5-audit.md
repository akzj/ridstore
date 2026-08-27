# Phase 5 全局审计与生产门禁

状态：进行中；禁止声明 production-ready

审计基线起点：2026-08-22，`4e9700e`；本文随当前审计提交更新

本文不是阶段通过报告，而是 Phase 5 的 requirement-to-evidence 台账。只有“完成”项可以作为当前结论；“局部证据”和“未完成”都不能被测试全绿替代。

## 1. Phase 5 交付矩阵

| 计划项 | 当前状态 | 当前证据 | 尚缺证据 |
|---|---|---|---|
| Offline Verify/Scrub | 完成 | `internal/verifier`、根包 `Verify`、corruption/lease/active-tail tests | 超大 Store 的 external-sort verifier 不在 v2；当前 O(live IDs) 内存限制已记录 |
| 一致 Backup | 完成 | 同一 source lease、hash metadata、payload Verify、SIGKILL matrix | 远端传输、压缩、加密不在 v1 |
| Restore/UUID 策略 | 完成 | 新目录发布、RESTORING marker、默认新 UUID、preserve 显式开关、SIGKILL matrix | 应用级异机演练尚未执行 |
| Metrics adapter | 完成 | v2 Coordinator/Engine 原生固定 bounded samples、Prometheus adapter tests | dashboard/告警属于部署层 |
| Migration planner | 完成 | v2 双 Manifest 槽只读识别、严格 header/CRC、空 registry、当前格式 exact Verify、未知版本 fixture | 无 v1 数据迁移；未来格式尚无执行 step |
| Full crash/fault matrix | 局部证据 | 各主协议的 process SIGKILL；当前已识别 v2 durable writer 的 `EIO/ENOSPC/EACCES` syscall-error matrix | 没有设备 power-loss 证据；代码级 syscall matrix 不能证明 flush 硬件语义 |
| Long fuzz/nightly | Harness 完成，证据未完成 | 9-target v2 runner、每日/手动 workflow、原始日志/corpus/terminal marker、短 harness smoke | 尚无全部 target 自然结束的 long-fuzz artifact |
| 72h steady-state soak | Harness 完成，证据未完成 | v2 bounded-ID 模型、维护排空、exact offline Verify、资源收敛与终态 JSONL smoke | 尚无 72h 自然结束 artifact |
| Same-durability benchmark | Harness 局部完成 | v2 durable create/hot-overwrite、raw append+fsync lower bound、带环境元数据和终态 marker 的 report runner | 无 Pebble/RocksDB 同 durability 稳定态对比、完整 workload matrix 和可发布原始报告 |
| Known limits/checklist | 进行中 | 本文、前台 write-stop admission | 长时/环境证据及最终 Review 尚未完成 |

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

Active Data 主写路径现已增加 append/sync、seal/footer write+sync、rename 和 data directory sync 注入点。单元矩阵证明每个 seal 边界都能经 fresh recovery 收敛为严格 immutable Segment；Store 级测试证明 descriptor write 错误确定未提交、Seal 后 sync 错误返回 CommitUnknown、两者均保留 `EIO` cause 并 fail closed。新 Active 创建的 Header write、file/directory sync，以及 Open incomplete-tail truncate/sync 也完成三类错误矩阵。Rotation recovery 只在 Journal 明确授权且新 ID 尚未发布时删除短 regular file 或 corrupt Header 后重建；合法空文件重新 fsync，非空、symlink/non-regular entry 拒绝。tail truncate 后 sync 失败的下一次 Open 即使已看到 clean size 也补做 file sync。Rotation Journal 与 Maintenance Journal 的 install/remove write、sync、rename/remove、directory sync 也已覆盖三类错误；Open 会删除并 fsync 未发布 regular temp、拒绝 symlink，并幂等完成已发布 Journal。

Maintenance Journal 另按 Data GC phase 验证所有七次 directory-sync publication：phase 1–3 失败且 Manifest 尚未证明 GC checkpoint 时允许撤销；Mapping Checkpoint Manifest 一旦 durable，运行时立即 fail closed，即使 phase-4 Journal rename 尚未成功，fresh Open 也会用 `MaintenanceGeneration`、精确 SegmentStats 和 ReplayStart 的共同证据补写 phase 4 后继续删除。嵌套 Mapping rotation 虽可提前推进相同 MaintenanceGeneration，但旧 source 仍在 SegmentStats 时只能撤销，不能误判为 GC checkpoint。checkpoint 前 Journal cleanup 自身失败也会 fail closed，由 fresh Open 收敛。完整 writer 清单见 `syscall-fault-matrix.md`；代码级 syscall matrix 已闭合，但本 P0 仍因没有设备 power-loss 证据保持局部完成。

Active Mapping Checkpoint 的 Node append 与最终 file sync 现已覆盖相同三类 syscall 错误。底层 `nodeStore` 在任一失败后 poisoned，Store 保留原始 cause 并停止写；fresh Open 采用旧 Manifest Root、忽略/截断未发布 tail、从 Commit Log replay 后可重新 Checkpoint，offline Verify clean。

Active Mapping Open tail repair 也已覆盖 truncate/sync 的三类错误，失败 Open 释放 lease 后可重试。审计同时修复了“先 truncate、后验证 Root”的取证破坏窗口：现在只有完整遍历 durable Root 并证明它不触及 invalid tail 后才允许修复；引用损坏 Node 时文件保持不变并返回 corruption。

Mapping rotation 已覆盖旧 Active sync、Footer write/sync、rename/dir-sync 与新 Active Header write/sync/dir-sync；三类错误均验证，Store 级各边界均 fail closed 并由 fresh Open 恢复。普通 rotation 的独立 Maintenance Journal 和 Data GC nested 的父 Journal 所有权分别验证。恢复 writer 的 truncate、partial remove、Footer/Header、rename/dir-sync 也完成三类错误矩阵并证明失败可重试。审计还修复了恢复仅验证“文件当前可见”却未补齐 durability 的问题：合法 sealed/new Active 已存在时仍重新 file sync 与 mapping-dir sync。

Mapping GC 现已覆盖新 generation Header/Node/Footer write、file sync、temp/publish rename 与 directory sync，以及 checkpoint 前 cleanup、旧文件 trash、delete 的 remove/rename/directory-sync；每个边界注入 `EIO/ENOSPC/EACCES`，恢复路径自身失败后可再次 Open。多文件 Case 验证部分 rename/delete，Store 级代表性 Case 验证 fail closed、原始 cause、记录一致与 offline Verify。审计同时修复两处协议缺口：checkpoint 前 cleanup 错误不再被吞掉；Catalog mutation 一旦通过、Installer 可能已发布 CURRENT 后，运行时不再删除新 Mapping 文件。相反，Installer 前的 Mapping baseline conflict 仍完整回滚。旧 Root reader 清零门禁保持在首次 trash rename 之前。至此 Mapping writer 的当前 syscall matrix 闭合，但不能替代真实 power-loss 证据。

Data GC 的 source rename-to-trash、data/trash directory sync、trash delete 与 delete directory sync 现已覆盖 `EIO/ENOSPC/EACCES`。这些边界均位于 checkpoint 和 source-removal Manifest durable 之后，因此运行时不尝试回滚，而是立即 fail closed；fresh Open 从 phase-5/6 Journal 完成相同操作。恢复路径本身也传播 hook，任一恢复 syscall 或 Journal advance/remove 失败后，下一次 Open 可继续；测试验证记录 revision/value、Journal/trash 清空和 offline Verify。该结果只闭合 Data GC 删除 writer，不能替代其他 writer 或设备 power-loss 证据。

Initialize 现已覆盖 Marker temp 清理/write/file sync/rename/root sync、目录创建/root sync、初始 Data/Mapping Header write/file sync/directory sync、损坏的未发布文件清理，以及最终 Marker/temp remove/root sync 的 `EIO/ENOSPC/EACCES`。有效 Marker temp 在恢复时先补做 file sync，再 rename/root sync；损坏 temp 和 durable phase 前的损坏初始 Segment 可删除并重建，durable phase 后仍 fail closed。审计同时修复了最终 Marker remove 成功而 root sync 失败后的重试缺口：marker-free Open 在采用已发布 Manifest 后会重新 sync root，不能仅因 Marker 当前不可见就认定删除 durable。所有 Case 收敛到同一 UUID/HardLimits、generation 1，且不残留 Marker/temp。该结果仍不代表真实 power-loss 已覆盖。

Backup artifact publication 现已覆盖 root/子目录 create，INCOMPLETE、临时 Verify LOCK、payload 和 metadata 的 write/file sync，Verify cleanup/Marker remove，以及 prepared root、parent、各 payload child、metadata root、最终 publication root 和补偿路径的 directory sync；所有逻辑边界分别注入 `EIO/ENOSPC/EACCES`。root 创建前失败不产生目标，此后失败由 INCOMPLETE 明确拒绝 Inspect。最终 root sync 失败会补偿恢复 Marker；补偿 write/file sync/root sync 自身失败时，返回值同时保留 publication 与 compensation cause，源 Store 保持 clean。Backup 不使用 rename，且失败 artifact 不在原路径重试或隐式覆盖。该结果只证明 Backup writer；Restore 的独立证据如下。

Restore artifact publication 现已覆盖 root/`.payload`/子目录 create，RESTORING、LOCK、payload、Segment Header UUID rewrite、Manifest replacement 的 write/file sync/rename/cleanup，prepared/rewrite/publish 两侧 directory sync，八个 payload entry rename、`.payload` remove、Marker remove/final sync 与补偿路径；各逻辑边界分别注入 `EIO/ENOSPC/EACCES`。第二个 Header rewrite 及第二至第八个布局 rename 失败证明部分变换仍由 RESTORING fail closed，Open/public Verify 拒绝且源 artifact 保持可 Inspect。审计同时修复布局 rename 只 sync 目标目录的问题：现在在 `.payload` 尚存在时先 sync source，随后 remove 并 sync destination。Manifest cleanup 和 Marker 补偿失败保留双重 cause。至此当前已识别的 v2 durable writer 代码级 syscall matrix 闭合，但不能替代 power-loss 或异机恢复证据。

### 已修复：磁盘耗尽停止水位

当前实现增加 runtime `WriteStopFreeBytes` 与 `SpaceCheckInterval`：Put 在产生新的 payload append 前执行缓存式空间 admission，间隔内按获准物理字节保守扣减，并以共享 refresh gate 覆盖 admission 到 append 返回的窗口；低于水位返回 `ErrInsufficientSpace`，不 fault Store。已有 Batch 的 Commit/Abort、Get 与普通 Checkpoint 保持可运行，避免保护机制阻塞收敛路径；空间恢复后新写可重试。GC 使用同一 reservation 账本和独立较低水位执行 copy/checkpoint 两阶段 admission。

该水位是 admission signal 而非文件系统配额：其他进程以及门禁外的 Commit/Checkpoint/GC 可并发消耗空间，真实 write/fsync 仍可能 ENOSPC。部署层仍必须提供独立文件系统/配额、容量告警和基于最大并发 Batch 的余量；代码不把水位误称为绝对空间保证。

Mapping GC 也在创建 staging/marker 前按精确 live-record 数、八层 Dense Mapping Node 和完整输出
Segment 建立保守 admission；拒绝不改变旧 generation。Data GC 的复制速率可通过
`SetGCBytesPerSecond` 在运行时调整，新值从下一次 Compact 生效。按时段和容量触发维护仍由外部
scheduler 负责，不进入持久化协议。

### 已修复：v2 切换后 Metrics 实现缺失

公开 API 切换到 v2 Engine 时，旧 runtime metrics 实现随 v1 一并删除，但本文与 Metrics 契约仍错误标记为完成。
当前实现已从 v2 的真实所有者重新建立 bounded snapshot：Coordinator 记录用户 Commit queue/group 与分段耗时，
Batch 生命周期记录 committed/aborted/unknown，Persistent Mapping 和 space gate 提供即时 gauge，完整 Data GC
记录物理 copied/reclaimed bytes 与结果计数。根包导出固定 41 个样本（含 GC throttle/space admission、
后台 Checkpoint requested/completed/failed 和 Record metadata cache hit/miss/entries/evictions）以及无第三方依赖的 Prometheus adapter。

### P1：长时与对比证据

72h soak、长期 fuzz、power-loss、异机 restore drill 和同 durability benchmark 都必须自然完成并保存原始产物。仓库现已提供 soak、long-fuzz/nightly 和初步 durable benchmark harness，但当前本机短测试不能替代这些结论。

## 3. 当前可重复门禁

```text
make test
make test-race
make vet
make test-fuzz-smoke
make test-crash
make verify
```

`make verify` 聚合普通、race、vet、fuzz smoke、long-fuzz/soak harness smoke 和 process-crash。它不包含自然 long fuzz、benchmark、
72h soak、异机恢复或 power-loss，因此成功只能证明当前开发门禁通过。

## 4. Production checklist

- [x] v2 format/contracts frozen，golden/decoder fuzz smoke 可重复；
- [x] Commit/Recovery、Checkpoint、Mapping/Data GC 主路径有阶段 Review；
- [x] Offline Verify、Backup/Restore、Metrics 已实现；
- [x] 本机 `make verify` 通过；
- [x] Open recovery 不再全量扫描/保存历史 PutRecord；
- [x] Status retention 与 recovery transient memory 有明确上界；
- [x] 所有当前已识别的 v2 durable writer 完成 syscall error matrix；
- [x] write-stop 水位完成，并明确部署层配额/告警与并发余量契约；
- [ ] long fuzz/nightly 自然结束且无未解释 failure；
- [ ] 72h steady-state soak 自然结束，空间/RSS/FD/goroutine 收敛；
- [ ] same-durability append/Pebble/RocksDB 对比保存原始结果；
- [ ] 独立目录及异机 Backup/Restore drill 完成；
- [ ] 支持的文件系统/设备完成 power-loss 验证，或产品声明明确排除；
- [ ] 最终全局锁顺序、格式兼容、取消清理和完整 diff Review 通过。

在所有勾选项关闭前，README、release note 和部署文档都不得使用 production-ready 表述。

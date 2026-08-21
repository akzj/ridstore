# 验证与验收计划

状态：Acceptance contract v1

## 1. 原则

测试通过不是生产就绪的充分条件。ridstore 的证据分为：

```text
格式正确性
协议状态机
崩溃恢复
并发竞态
长期空间/资源收敛
同 durability 性能
```

每项结论必须说明验证边界。正常 Close 后重开不能替代 crash recovery；初始空库吞吐不能替代稳定 GC 状态。

## 2. 测试层次

### 2.1 Codec/Format

- golden bytes；
- encode/decode round trip；
- CRC 覆盖范围；
- Length/Count/Offset 溢出；
- padding 非零；
- unknown version/type/TLV；
- truncated Header/Payload/Footer；
- Store UUID/FileID 不匹配；
- full uint64 ID、边界 VAddr、ReplayStartLogPos 的 active-end/rotation 边界和类型不可互换；
- PutRecord 固定 64-byte Header、OriginBatchID revision、空 Value 和 Relocation revision preservation；
- SparseBitmap/Dense512 Node golden bytes、EntryCount/NodeSize/bitmap rank 边界；
- SegmentSeal/Footer 镜像字段、DescriptorCRC 精确拼接顺序和空 Batch vector；
- INITIALIZING/MAINTENANCE 各 OperationType/Phase 的 golden bytes、非法跳级和恢复后置条件；
- 所有 v1 Flags/CommitFlags/Reserved 非零均拒绝；
- SegmentStatsEntry 排序、重复、零项、covered seq 和 Root generation 一致性；
- fuzz 所有不可信 decoder。

### 2.2 模型属性测试

以简单内存参考模型：

```text
map[ID]{value, revision} + atomic conditional batch
```

随机生成 Allocate/Put/Delete/Commit/Abort/Get/Checkpoint/GC 序列，每步比较 ridstore 可见状态。

属性：

- Committed Batch 全部生效；
- Aborted Batch 零生效；
- 同 Batch 同 ID 最后操作生效；
- ID 不复用；
- Delete 幂等；
- GC 前后逻辑状态相同；
- Recovery 前后逻辑状态相同；
- Mapping Checkpoint 前后状态相同；
- 同一逻辑 Node 在 SparseBitmap/Dense512 编码下 Lookup 结果相同；
- 随机稀疏完整 uint64 ID 的 Mapping bytes 不退化为每个 Leaf 固定 4160 bytes；
- Blind Put 按 CommitSeq Last-Writer-Wins；
- ExpectRevision/ExpectAbsent 全部满足才提交；
- 任一条件冲突时所有 mutation 零生效且不产生 Seal；
- Relocation 不改变 LogicalRevision。

### 2.3 并发测试

- 多 goroutine 独立 Batch；
- 同 ID commit-order last-writer-wins；
- 同 ID ExpectRevision 竞争只有一个成功；
- 同组 ExpectAbsent 按 virtual Mapping 顺序只有一个成功；
- group commit 多 Batch 独立原子性；
- Lookup 与 Publish；
- Checkpoint cut 与 Commit；
- Reader pin 与 Retire；
- 用户 Put 与 GC CAS；
- 冷 Root 上 Relocation CAS 在 pre-Seal 阶段解析，publish 临界区零文件 I/O，runtime/recovery outcome 一致；
- Close 与 Commit/Checkpoint/GC；
- backpressure 和 Context cancel。

全部并发测试在 `-race` 下运行，并增加高重复次数和调度扰动。

## 3. Crash Harness

使用父进程驱动真实子进程和真实数据目录：

```text
parent
  -> start child workload
  -> wait for named failpoint
  -> SIGKILL / forced exit
  -> reopen with fresh process
  -> verify committed oracle
```

禁止：

- defer Close；
- 在 kill 前调用 Flush；
- 只 mock fsync 就宣称掉电安全；
- 复用进程内 Mapping 验证恢复。

Failpoint 必须位于真实 write/fsync/rename/dir sync/publish 操作两侧。

Manifest/CURRENT publication 另有 syscall-error matrix：在两个文件的 write、file sync、rename 和 directory sync 操作前分别注入 `ENOSPC`、`EIO` 或 `EACCES`，验证底层 cause 不丢失、CURRENT 只指向完整旧/新 generation、离线 Installer 可幂等重试。运行中 Checkpoint 一旦进入 Manifest Installer 后得到错误，不猜测 publication outcome，Store 立即 fail closed；fresh Open 清理未发布 generation 或采用已发布 CURRENT，并通过 offline Verify。该矩阵尚未代表其他 Journal、Segment 和 GC 文件操作均已覆盖。

Active Data 另覆盖 append write、commit sync、SegmentSeal write/sync、Footer write/sync、rename 和 data directory sync。每个边界分别注入 `EIO/ENOSPC/EACCES`；write/sync 失败必须 poison 当前 Active，rename/dir-sync 失败必须停止 Rotation。Store 级 descriptor write/sync Case 分别验证确定 Aborted 与 CommitUnknown，随后 fresh Open 按磁盘完整 Frame 判定。完整覆盖状态以 `syscall-fault-matrix.md` 为准。

Active Data rotation destination 另对 Header write、file sync、data directory sync 注入 `EIO/ENOSPC/EACCES`。这些错误发生时 Rotation Journal 仍以旧 Active 为权威，Store fail closed；fresh Open 对缺失 destination 重新创建，对短于 Header 或 Header corruption 的 unpublished regular file 执行 remove+directory-sync 后重建，对合法空文件重新 file/directory sync。大于 Header 的意外内容和 symlink/non-regular entry 必须拒绝，不能以恢复名义删除。recovery 的 remove、remove-dir-sync、create file/dir syscall 自身失败后可再次 Open。Active Data Open 对 incomplete tail 的 truncate 与 file sync 注入同三类错误；truncate 后 sync 失败时，下一次 clean Open 仍必须补做 file sync，不能因当前 size 已等于 valid end 而跳过 durability。恢复后 Commit 结果、Batch Status、记录内容及 offline Verify 必须一致。

Rotation Journal 对 install 的 temp-remove/write/file-sync/rename/dir-sync 和完成时的 final-remove/temp-remove/dir-sync 注入相同三类错误。rename 前失败时 fresh Open 必须删除 `.ROTATION.tmp` 并 fsync journal 目录；temp 不是 regular file 或为 symlink 时 fail closed。dir-sync 错误额外覆盖 Phase 1–5 每个已 rename Journal 状态；Open 必须完成 Active→Sealed→new Active→Manifest 转换或确认已安装 Manifest，恢复后 offline Verify 必须 clean。

Maintenance Journal 对同一组 install/remove syscall 注入 `EIO/ENOSPC/EACCES`，并额外覆盖 Data GC phase 1–7 的 directory-sync 失败。GC checkpoint Manifest durable 前允许取消 cleaning 并删除 Journal；该清理任一 syscall 失败时 Store 必须 fail closed。Manifest durable 后不得因 phase-4 Journal publication 失败回滚：fresh Open 以 MaintenanceGeneration、source 不在精确 SegmentStats、ReplayStart 越过 source 三项共同证据恢复 phase 4 并继续协议。只有 MaintenanceGeneration 而 source 仍为 live 的情况属于嵌套 Mapping rotation，必须撤销而不能删除 source。恢复后 `.MAINTENANCE.tmp` 不得残留且 offline Verify 必须 clean；非 regular/symlink temp 必须拒绝。

Active Mapping Checkpoint 对 Node `WriteAt` 和最终 Active file sync 注入 `EIO/ENOSPC/EACCES`。任一错误必须 poison `nodeStore`，后续 append/sync 返回 `ErrActivePoisoned`；Store 保留原始 cause 并 fail closed。fresh Open 只能采用旧 Manifest Root，扫描或截断未发布 tail，再由 Commit Log replay 重建 overlay；随后新 Checkpoint 和 offline Verify 必须成功。该矩阵不覆盖 Mapping rotation 或 Mapping GC writer。

Active Mapping Open tail repair 对 truncate 与随后的 file sync 注入相同三类错误，失败 Open 必须释放 writer lease并允许无 hook 重试。任何 truncate 前必须以扫描得到的 `validEnd` 完整遍历当前 Manifest Root；只有所有可达 Node 都在有效区域内才可截断。若 Root 或子 Node 指向损坏 tail，Open 返回 corruption 且保持文件大小不变，避免先破坏取证再失败。该矩阵仍不覆盖 Mapping rotation/GC。

Mapping rotation 对旧 Active 的 pre-journal sync、Footer write/sync、sealed rename、mapping directory sync，以及新 Active Header write/sync/directory sync 注入 `EIO/ENOSPC/EACCES`。普通 rotation 和 Data GC phase-3 nested rotation 使用同一文件矩阵，但前者拥有并最终删除独立 Maintenance Journal，后者只能扩展父 Journal。任一运行时错误经 Checkpoint 使 Store fail closed；fresh Open 必须完成或确认 file-set rotation、保留已提交记录并通过再次 Checkpoint/Verify。恢复 writer 自身的 truncate、partial-destination remove、Footer/Header write/sync、rename/dir-sync 也注入相同三类错误；失败恢复释放资源后再次 Open 必须幂等完成。若恢复看到已存在的合法 sealed 或新 Active 文件，也必须重新 file sync 和 mapping-dir sync，不能以“当前进程可见”替代 durability。

Mapping GC 对新 generation 的 Header、Node、Footer write，sealed/final Active file sync，temp directory sync、temp→final rename 和 publish directory sync 注入 `EIO/ENOSPC/EACCES`；checkpoint 前 cleanup remove/dir-sync、旧文件 rename-to-trash、mapping/trash directory sync、trash delete/delete-dir-sync 使用同一矩阵。运行时错误均保留底层 cause 并由 Store fail closed；fresh Open 在 Manifest 前删除所有 temp/final destination，Manifest 后验证新 Root 再完成 old-file trash/delete。恢复 writer 的 cleanup/trash/delete syscall 失败后再次 Open 必须幂等重试；多文件 Case 额外覆盖第一个 rename/delete 成功、第二个失败。Catalog 在进入 Installer 前返回 Mapping baseline conflict 时允许完整清理；一旦 mutation 校验成功并进入 Installer，即使返回错误也禁止删除新文件，因为 CURRENT 可能已发布，只能由 fresh Open 判定。Reader pin 仍须在首次 old-file rename 前清零，最终 offline Verify clean。

Data GC 在 checkpoint Manifest 已证明 source 无 live Mapping、reader pin 清零且第二个 Manifest 已移除 source 后，对 source rename-to-trash、data directory sync、trash publish directory sync、trash remove 和 delete directory sync 注入 `EIO/ENOSPC/EACCES`。任一点失败时 Store 保留 cause 并 fail closed；fresh Open 根据 phase-5/6 Journal 和当前 Manifest 幂等完成 rename 或 delete。恢复路径使用同一 hook，恢复 syscall 自身失败必须释放 Open 资源并允许下一次 Open 重试；rename 已完成但任一 directory sync 失败、delete 已完成但 directory sync 失败均须重新 fsync。恢复期 Journal phase advance/final remove 也继承同一 hook。成功后 source/trash 均不存在、Journal 清空、记录 revision/value 不变且 offline Verify clean。

## 4. Commit Crash Matrix

每个 Case 至少验证旧值、新值、NotFound 和 Batch Status。

| 阶段 | 允许恢复结果 |
|---|---|
| PutRecord 前/中/后，无 Seal | Batch 不可见 |
| CommitPart 中间 | Active 尾部截断且 Batch 不可见；若损坏位于 Sealed/中间位置则 corruption |
| CommitSeal 中间 | Active 尾部截断且 Batch 不可见；若损坏位于 Sealed/中间位置则 corruption |
| Seal write 后、fsync 前 | Committed 或未提交，调用者语义 Unknown |
| fsync 后、Publish 前 | 必须 Committed |
| Publish 部分后 | 重启后必须整批 Committed |
| Publish 后、Reply 前 | 必须 Committed，Status 可确认 |
| 条件检查前/中取消或冲突 | 确定 Aborted，无 CommitSeal，Mapping 零变化 |

Abort 独立覆盖 marker append 前、完整 write 后未 sync、API 已返回后三个 SIGKILL 边界。三者恢复后 Put 都不可见、Status 都为 Aborted，且旧 BatchID/Record ID 均不得再次发放；这证明正确性不依赖 best-effort Abort marker durable。

若硬件/文件系统不能模拟真实 power loss，报告只能称为 process-crash evidence。

初始化另有独立 crash matrix：INITIALIZING marker、子目录、两个初始 Segment Header、generation-1 Manifest、CURRENT 和 marker 删除的每个 write/fsync/rename 前后强制退出；恢复结果只能是可继续完成的同一 UUID Store 或明确错误，不能静默创建第二个空 Store。

初始化 syscall matrix 另在 Marker temp 清理/write/file sync/rename/root sync、子目录 create/root sync、初始 Data/Mapping Header write/file sync/directory sync、损坏的未发布初始文件清理，以及最终 Marker/temp remove/root sync 前分别注入 `EIO/ENOSPC/EACCES`。每次失败都必须保留底层 cause，随后由同一 UUID/HardLimits 恢复到 generation 1；只有最早期尚未形成有效 Marker 时允许返回 `ErrNotInitialized` 并由相同参数重新 Create。损坏的未发布 Marker temp 和初始 Segment 可以删除重建，phase 已声明 durable 后则禁止修复。Marker 已删除但 root sync 失败时，marker-free Open 必须重新 sync root 后才能成功。

## 5. ID 与 BatchID Reserve Crash Matrix

- Reserve frame 前崩溃：旧区间不变；
- write 后 fsync 前：恢复 high watermark 只能保持旧值或采用 durable 新值；
- fsync 后 publish 前：恢复必须采用新 high watermark；
- 已返回 ID 后崩溃：任何后续 Open 不得再次返回该 ID；
- 多次 reserve/rotation/checkpoint 后仍单调。

同一矩阵分别作用于 IDReserve 和 BatchIDReserve。额外验证：崩溃前已返回的 BatchID 不会在重启后重新分配；`Status` 不会把旧 BatchID 关联到新 Batch；uint64 边界返回 `ErrIDExhausted` 而不是回绕。

当前自动化 SIGKILL matrix 覆盖 reserve append 前、完整 write 后 sync 前、sync 后 allocator 内存 publish 前，以及 ID/BatchID 已返回四个边界。write 后 sync 前允许 fresh process 观察旧或新 durable high；sync 后只能采用新 high。另有多轮 ID/BatchID reserve 跨 Data Segment rotation、Checkpoint 和重新 Open 的单调性测试。该矩阵是 process-crash 证据，不替代设备忽略 flush 的 power-loss 验证。

## 6. Mapping Checkpoint Matrix

- captured/fresh Overlay 切换时并发 Commit；
- Mapping Node 写中断；
- Mapping file fsync 前后；
- Mapping Segment seal/footer/rename 前后；
- Manifest temp/write/fsync/rename；
- CURRENT temp/write/fsync/rename；
- Checkpoint 安装与 Data/Mapping Segment rotation、Data GC Manifest 安装并发；
- directory fsync 前后；
- 新 Root 发布后旧 Cache View 仍在读；
- Checkpoint 失败时 Delta 不丢失；
- Delete 后空 Leaf/Internal 路径剪枝；
- Sparse→Dense、Dense→Sparse 阈值两侧和 occupancy 1/503/504/512；
- 变长 Node 跨 Mapping Segment 尾部时先 rotation，不允许跨文件写 Node；
- Open 只加载 Root/上层，不全量加载；
- Delta hard limit backpressure。
- Root/Stats 在 Manifest 中同代安装：每个 write/fsync/rename 崩溃点只能得到旧 Root+旧 Stats 或新 Root+新 Stats；
- Stats Builder 对同一 ID 多次覆盖只计算 base→cut-final，Abort、条件冲突和 Relocation CAS failure 不计 live；
- Header 批读错误、Stats underflow 或 Segment 身份错误时新 Root/Stats 均不安装，frozen Delta 不丢失；
- cut 后并发 Commit、Checkpoint 安装和 Recovery replay 中，`exact Base + active/frozen additions` 始终不低于全量 Mapping 得到的精确 live；
- Stats 表为 0 或缺失 Segment 时仍不能绕过 GC 精确 Mapping 校验和删除门禁。

恢复只能选择完整旧 Root 或完整新 Root。

## 7. GC Matrix

采用 `gc-protocol.md` 第 16 节全部时序，并验证：

- 任意崩溃后旧 Record 或新 Record至少一个可读；
- Mapping 不指向不存在文件；
- CAS 失败不覆盖用户 Put；
- Reader pin 阻止提前删除；
- Journal Phase 可幂等继续；
- trash 文件不重新加入正式集合；
- 空间最终收敛；
- ENOSPC 不破坏旧数据。
- SegmentStats 只能改变候选顺序，不能改变任意 Record 的搬迁和删除结论。

## 8. Corruption 测试

主动翻转：

- Segment Header；
- Frame Header/Payload CRC；
- Commit Descriptor Entry；
- Segment Footer；
- Mapping Node Slot/CRC；
- Sparse bitmap、EntryCount、NodeSize、packed value 顺序和非法 Encoding；
- Manifest/CURRENT；
- INITIALIZING/ROTATION/Maintenance Journal。

期望：

- 不返回错误对象的数据；
- 不越界分配；
- 不 panic；
- 不静默修复 Sealed corruption；
- 返回可分类错误并 fail closed；
- Scrub 与 Open 对 corruption 的判断一致。

“一致”指两者对已读取字节使用同一格式/CRC 结论，不要求普通 Open 主动全扫所有历史 payload。Open 必须拒绝损坏的文件 envelope、Replay window 和被 Descriptor/Mapping 实际访问的 Record；离线 Scrub 必须扫描并报告未被普通启动路径触及的 sealed corruption。

## 9. 资源与收敛测试

长期循环：

```text
Put/overwrite/delete/abort
-> checkpoint
-> data GC
-> mapping GC
```

观测：

- Data active/sealed/retired/trash 数量；
- Mapping files 和 unreachable bytes；
- Delta bytes；
- Mapping Cache bytes；
- RSS；
- FD；
- goroutine 数；
- disk allocated/logical bytes；
- GC copied/reclaimed bytes；
- Checkpoint lag。
- exact/live-upper SegmentStats、StatsCoveredCommitSeq 和 overestimate bytes。

通过条件不是固定文件数，而是工作负载停止后维护过程最终稳定、trash 清空、FD/goroutine 回到基线附近、空间不继续增长。

仓库提供 `ridstore-soak` 与 `make soak-72h`。工具只接受不存在的新 Store/report
路径，输出 append-only JSONL sample；结束时逐 ID 校验模型、排空 Data GC、Compact
Mapping、Close 并 offline Verify。`make test-soak-smoke` 只验证 harness 状态机和报告
格式，不能作为 72h 证据。只有指定时长自然结束且最终 summary 同时包含
`completed_naturally=true`、`verified_clean=true`，再结合环境元数据，才能进入审计。

## 10. 性能对比

遵循 `positioning-vs-lsm.md` 的公平性约束。候选对照至少包括成熟 LSM 和一个简单 append baseline。

### 10.1 工作负载

- Value：128B、4KiB、64KiB、1MiB；
- Batch：1、10、100、1000；
- concurrency：1、4、16、64；
- create-only；
- hot overwrite；
- random overwrite；
- conditional overwrite：0%、10%、50% 冲突率；
- hot/cold ExpectRevision Header validation；
- delete-heavy；
- mixed 50/50；
- hot Mapping read；
- cold Mapping read，dataset > cache；
- 连续高 occupancy 与随机稀疏 ID 的 Mapping bytes/Lookup；
- 大量 Delete 后 Dense→Sparse 的空间收敛；
- foreground + checkpoint；
- foreground + GC；
- 不同 live ratio 稳定态。

### 10.2 指标

- ops/s 和 bytes/s；
- Commit p50/p99/p999；
- queue/write/fsync/publish 分段；
- condition validation latency、conflict rate、Header read bytes；
- read hot/cold latency；
- CPU、RSS、FD；
- user bytes / physical bytes；
- GC/compaction bytes；
- space amplification；
- recovery bytes/time；
- tail latency during maintenance。

所有结果保存原始 benchmark 输出、配置、Git commit、Go 版本、内核、文件系统和设备信息。

## 11. 工程命令门禁

项目形成代码后统一入口：

```text
make test
make test-race
make test-fuzz-smoke
make test-crash
make test-integration
make bench
make verify
```

`test-fuzz-smoke` 对每个 Format decoder fuzz target 分别运行短时 fuzz（默认 `FUZZ_TIME=2s`、`FUZZ_PARALLEL=4`，CI/nightly 可覆盖）；`test-integration` 执行 Create→Commit→Checkpoint→离线 Verify→Backup→新 UUID Restore→Open 的跨模块生命周期；`bench` 只生成原始 benchmark，不是正确性门禁；`verify` 聚合普通、race、vet、fuzz smoke、process-crash 和 integration，仍不包含长期 fuzz、72h soak、power-loss 或跨引擎性能结论。

Backup artifact syscall matrix 覆盖 root/子目录 create，INCOMPLETE、Verify LOCK、payload、metadata 的 write/file sync，Verify cleanup 与 Marker remove，以及 prepared root、parent、各 payload child、artifact root 和补偿路径的 directory sync；每个逻辑边界分别注入 `EIO/ENOSPC/EACCES`。root create 前失败不得留下目标，此后失败必须由 INCOMPLETE 使 Inspect 返回 `ErrRecoveryRequired`。最终 root sync 失败后的 Marker 补偿 write/file sync/root sync 也独立注入，要求原 publication cause 与补偿 cause 均可由 `errors.Is` 观察，且源 Store offline Verify 仍 clean。Restore 采用独立矩阵，不能用本项替代。

Restore artifact syscall matrix 覆盖 root/`.payload`/子目录 create，RESTORING、LOCK、payload、Segment Header、Manifest replacement 的 write/file sync/rename/cleanup，prepared directories、Manifest rewrite directory、布局 publication 的 source/destination directory sync，八个 payload entry rename、`.payload` remove、Marker remove/final sync 与补偿路径。每个逻辑边界分别注入 `EIO/ENOSPC/EACCES`；额外在第二至第八个 rename 以及第二个 Segment Header rewrite 前失败，证明部分发布/部分 UUID rewrite 仍由 RESTORING fail closed。Manifest temp cleanup 和最终 Marker 补偿的双重错误必须同时保留 cause。除 root create 前失败外，所有失败目标都必须拒绝 Open/public Verify，且源 artifact 仍通过 Inspect。

离线命令默认使用与运行时相同的 65,536 terminal replay 上限：`ridstore-tool verify --dir <dir> --status-limit <n>`。超过上限明确返回 `ErrStatusCapacity`；操作者可以给一次离线诊断提高预算，但零值和静默无界扫描均不允许。

最低合并门禁：

- `go test ./... -count=1`；
- `go test -race ./... -count=1`；
- `go vet ./...`；
- format golden tests；
- crash smoke suite；
- 无未解释数据竞争、panic、goroutine/FD 泄漏。

长时 fuzz、完整 crash matrix 和 soak 可以在独立 CI/nightly，但发布前必须完成。

## 12. Phase 验收

### Phase 0

- 格式 golden/fuzz；
- crash harness 可杀真实子进程；
- write/fsync/rename failpoint 可定位；
- Format Freeze Review 完成。

### Phase 1

- 单线程 Put/Delete/Get/Batch；
- GetRecord Revision、ExpectRevision/ExpectAbsent 与确定冲突；
- Commit/Abort/Unknown 矩阵；
- StatusRetention 逐出/`ErrStatusExpired`、CommitUnknown 钉住、replay terminal hard limit、Checkpoint 释放容量和重复终态 corruption；
- ID Reserve 不复用；
- 扫描恢复与参考模型一致。

### Phase 2

- group commit；
- group virtual Mapping 条件顺序与冲突隔离；
- 并发/取消/backpressure；
- race clean；
- 性能分段可解释。

### Phase 3

- Persistent Mapping、Cache、Checkpoint；
- 不全量加载；
- 内存有界；
- Root 安装 crash matrix。

### Phase 4

- Data/Mapping GC；
- Reader pin；
- Journal crash-resume；
- 空间和资源收敛。

### Phase 5

- Scrub/Verify、Backup/Restore；
- 完整 soak；
- 稳定态 RocksDB/Pebble 对比；
- 运维和格式升级文档。

## 13. 生产就绪声明

只有同时满足才可称为 production-ready：

- 所有 durable invariant 有自动化证据；
- 完整 crash matrix 通过；
- race/vet/fuzz 无未解决问题；
- soak 自然结束且报告有效；
- Data/Mapping GC 空间收敛；
- Scrub 验证通过；
- 备份恢复在独立目录验证；
- 格式版本和升级策略冻结；
- 已知限制明确；
- 性能报告区分 initial 与 steady state。

单次 benchmark、正常重启或单元测试全绿不能单独支持该声明。

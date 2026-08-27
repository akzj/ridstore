# 配置、预算与 Backpressure

状态：v2 runtime contract；字段以根包 `HardLimits` / `RuntimeConfig` 为准

## 1. 配置分类

配置分为两类：

- **FormatHardLimits**：决定已有字节是否可被安全解码，Create 时归一化后写入 Manifest，Open 后不可在线改变；
- **RuntimeBudgets**：只决定内存、调度和维护速度，可以在每次 Open 时调整，但必须通过跨字段校验。

零值表示采用默认值，不表示无限。所有 bytes 均为 binary bytes，所有加法、乘法和 int 转换使用 checked arithmetic。

## 2. 第一版 Config

公开配置分为 `CreateConfig{HardLimits, Runtime}` 与 `OpenConfig{Runtime}`。持久化字段使用 `uint64`；
runtime 包含 RecordLog queue/buffer、Commit group、Mapping/metadata cache、Checkpoint sort、Delta、Status、
前台空间水位，以及 `GCBatchBytes`、`GCBatchMutations`、`GCMinFreeBytes`、`GCBytesPerSecond`。

第一版默认值：

| Field | Default |
|---|---:|
| SegmentSize | 256 MiB |
| MaxValueSize | 64 MiB |
| MaxBatchBytes | 256 MiB |
| MaxBatchMutations | 1,000,000 |
| MaxBatchConditions | 1,000,000 |
| MaxOpenBatches | 1,024 |
| IDReserveSize | 1,048,576 |
| BatchIDReserveSize | 65,536 |
| MappingCacheBytes | 256 MiB |
| DeltaSoftLimitBytes | 256 MiB |
| DeltaHardLimitBytes | 512 MiB |
| CheckpointSortBytes | 256 MiB |
| RecordMetaCacheEntries | 65,536 |
| MaxSegmentStats | 65,536 |
| StatusRetention | 65,536 |
| MaxGroupPayload | 64 MiB |
| MaxGroupBatches | 64 |
| GCBatchBytes | 16 MiB |
| GCBatchMutations | 4,096 |
| GCMinFreeBytes | min(SegmentSize, WriteStopFreeBytes) |
| GCBytesPerSecond | 64 MiB/s |
| WriteStopFreeBytes | 512 MiB |
| SpaceCheckInterval | 100 ms |

这些是可运行的安全起点，不是性能承诺；基准可以改变后续默认值，但持久化 hard limits 的改变必须遵守 Open 兼容规则。

## 3. 归一化与跨字段校验

Create 先填充零值、再验证、最后把 8 个 FormatHardLimits 写入 INITIALIZING 与 Manifest。Open 以 Manifest 为权威；调用者对应字段为 0 时采用磁盘值，非 0 时必须完全相等，否则返回 `ErrConfigMismatch`。RuntimeBudgets 每次 Open 独立归一化。

必须满足：

- `4 KiB + largest aligned Frame/Descriptor + 4 KiB Footer <= SegmentSize <= 4 GiB`；
- `MaxValueSize <= MaxBatchBytes`，并且最大 PutRecord 可完整放入一个 Segment；
- 最大合法 Commit/Relocation Descriptor 可完整放入一个 Segment；
- count 字段能安全转换为实现使用的 `int` 和分配大小；
- `0 < DeltaSoftLimitBytes < DeltaHardLimitBytes`；
- `CheckpointSortBytes` 必须能覆盖 Delta hard limit 允许形成的最坏 frozen entry 数；
- `StatusRetention >= MaxOpenBatches`；该预算同时约束近期查询缓存与 Manifest replay cut 后的 terminal Batch 数；
- `MaxGroupPayload > 0` 并限制多请求 group buffer；若单 Descriptor 更大，则该请求单独成组，不能拒绝一个已由 HardLimits 允许的 Batch；
- `GCBatchBytes <= MaxBatchBytes` 且 `GCBatchMutations <= MaxBatchMutations`；
- `GCMinFreeBytes >= 0`；公开层零值采用不高于写停水位的一个 Segment；内部关闭 space gate 时不执行预检；
- `GCBytesPerSecond > 0`；零值采用 64 MiB/s；
- `WriteStopFreeBytes >= GCMinFreeBytes`；公开层零值采用 512 MiB；
- `SpaceCheckInterval > 0`；公开层零值采用 100ms；
- 任何 runtime budget 不能解释为允许绕过持久化 hard limit。

## 4. Delta 计费

Delta 占用按 Store 级总量计费：

```text
DeltaChargedBytes = active Delta
                  + all frozen Delta
                  + admitted-not-yet-published reservations
```

计费包含 entry、hash bucket/索引的摊销、tombstone、per-segment Stats additions 和对象固定开销；实现必须使用保守 charge，不能只统计 Value/VAddr 的理论字节。冻结 Overlay 不释放 charge，只有新 Root+Stats Manifest durable、immutable state 安装且旧 reader unpin 后才释放。

达到 soft limit：立即请求/提高 Checkpoint 优先级并告警。可能超过 hard limit 的用户 Commit 必须在进入不可穿插 Checkpoint 的条件/CAS 验证区间**之前**，按所有最终 mutation/Relocation 都成功的上界等待并预留预算；因此等待期间 Checkpoint barrier 仍可运行。Context 在此阶段取消是确定未提交。验证冲突或 CAS skip 后释放未使用 reservation。内部 Relocation 同样受预算约束，必要时暂停 GC。

Group admission 不能持有一部分 reservation 等待另一部分：按 queue order 尝试预留；若当前 group 已有请求而下一个请求无法预留，立即封闭并执行当前 group，把该请求留在队首；只有空 group 的队首请求无法预留时才等待 Checkpoint。单请求的保守 charge 大于 DeltaHardLimit 属于 `ErrInvalidConfig` 或 `ErrBatchTooLarge`，不能永久等待。

一旦预算已预留并允许 Descriptor 落盘，fsync 后的 Mapping Publish 和 Stats addition 绝不能因 hard limit 再阻塞或失败；成功 Publish 后把 reservation 转为 active Delta charge。这样不会形成“durable Commit 等待 Checkpoint，而 Checkpoint 又等待 Publish”的环。

若 Checkpoint 持续失败，新的 Commit 在 hard limit 前停止，Get、Abort、Recovery、Checkpoint retry 和 Close 仍可运行；错误与最长等待时间必须可观测。

Batch Status 使用独立预算。运行时最多保留 `StatusRetention` 个已解决近期状态；更老已分配 BatchID 返回 `ErrStatusExpired`。`CommitUnknown` 不进入逐出队列，Store 在重新 Open 前也不会越过它建立新 Checkpoint。replay window 中 terminal Commit/Relocation/Abort 达到 75% 时请求 Checkpoint；`Begin` 为所有 Open Batch 预留最坏一个 terminal slot，到达硬上限后 Context-aware 等待 Checkpoint。Data GC 的内部 Relocation 同样预留 slot；若它在持有维护串行锁时到达上限，则返回 `ErrStatusCapacity`、安全撤销本轮 cleaning journal 并请求 Checkpoint，调用者可重试，不允许等待自身形成死锁。

Checkpoint 在 log barrier **之前**捕获 terminal 完成计数。已计数的终态要么已完成所需 append，要么是无需 terminal Frame 的确定性失败，随后 barrier 会覆盖它的全部恢复影响；并发较晚完成的终态即使物理 Frame 落在 barrier 前，也被保守计入下一 replay window。Manifest 安装完成后只释放该快照覆盖的容量，因此 crash recovery 的 terminal 去重表始终受同一上限约束。

## 5. Checkpoint 有界构建

`CheckpointSortBytes` 是 Builder 的 mutation 排序预算，不包含共享 Node Cache 和已计费 Delta。当前实现要求它能容纳 Delta hard limit 允许形成的最坏 frozen entry 数，并在超限时保留旧 Root/Stats 与 frozen Delta。

Header validation 按 `(SegmentID, offset)` 排序并使用有界窗口；生成的 SegmentStats 可流式 merge。若单个合法操作仍无法在预算内完成，返回明确配置错误并保留旧 Root/Stats/frozen Delta，不能 OOM 或安装部分 checkpoint。

## 6. Group Commit 与 GC 调度

`MaxGroupPayload` 与 `MaxGroupBatches` 限制 Commit Coordinator 一次 durable CommitGroup 的
payload 和 Batch 数。RecordLog 自身再由 `MaxQueuedBytes`、`AppendQueueCapacity`、
`AppendBufferBytes` 与 `AppendBufferRecords` 约束。实现不主动 sleep 等待更多请求；并发到达与 fsync
自然形成 batching window。单个合法 Descriptor 可以超过聚合目标独立执行，但仍必须满足持久化
RecordLog payload 和 Segment 上限。

GC 受 `GCBatchBytes/GCBatchMutations`、`GCBytesPerSecond`、Delta reservation、前台队列优先级和可用磁盘共同限制。每个 durable Relocation Batch 后按本轮累计 copied bytes 对 wall clock 做 Context-aware pacing；已经排队的用户 Commit 优先于下一批 Relocation，已经选中的单个有限 Relocation Batch 不被拆开。Runtime budget 只能降低后台工作速度，不能改变 Relocation durability、CAS 或删除门禁。

`SetGCBytesPerSecond(rate)` 原子修改后续 Data Compact 的复制速率。每次 Compact 只在创建 pacer
时读取一次，新值不影响正在运行的 Compact；`rate == 0` 非法。暂停、时间窗、容量水位和调用频率
属于外部 maintenance scheduler，不进入 Store 的持久化状态。

Data GC 使用两段磁盘 admission。copy 前按 source 全部物理 bytes、最坏 Relocation Descriptor、两个 rotation Segment 与 `GCMinFreeBytes` 检查空间；该上界对显式指定 Segment 和自动候选路径都成立，不依赖可能继续下降的 live estimate。copy 完成后，GC-required Checkpoint barrier 先冻结其实际 Delta layers，再按冻结 entry 数乘以每个 entry 最坏八层 Dense Mapping COW、一个 rotation Segment与 `GCMinFreeBytes` 重新检查。第二段失败时 Relocation 已是可恢复的 durable garbage/Delta，但源 Segment 和旧 checkpoint 仍保留。这样前台 Commit 可以继续运行，又不会把只按源 live 数得到的估计误称为整个 Checkpoint 上界。任一检查低于上界均返回 `ErrInsufficientSpace`。可用空间检查只是 admission signal，并不保留磁盘配额；之后每个 write/fsync 的 `ENOSPC` 仍必须原样传播并遵守对应 crash-recovery phase。

Mapping GC 在创建 staging 目录前，以同一 Checkpoint 的精确 live-record 数计算完整替换 generation
上界：每条记录最多八层 Mapping Node，每个 Node 按 Dense 编码、每个输出 Segment 按完整
`SegmentSize` 计费。空间不足不创建 staging/marker、不改变旧 Mapping。该检查与 Data GC 共用
reservation 账本和 `GCMinFreeBytes`；真实 ENOSPC 仍遵守 Mapping GC 恢复协议。

## 7. 前台新写停止水位

`WriteStopFreeBytes` 是进程级前台 admission 水位。所有 Create/Put/CompareAndPut 在产生新的
Put payload append 前检查可用空间；低于“水位 + 本次精确物理 Record 大小”时返回
`ErrInsufficientSpace`。拒绝不把 Store 标记为 fault/read-only，释放空间后可以重试。

已有 Batch 的 Commit/Abort，以及 Get、Checkpoint、Data/Mapping GC 不受该门禁阻塞。
它们是确认已写 payload、释放资源或推进恢复边界所需的收敛通道；但各自真实 write/fsync
仍可能返回 ENOSPC 并遵守原协议。水位不是磁盘预留，也不能保证所有已打开 Batch 可以
同时提交，部署方必须按最大并发 Batch/Checkpoint 和外部同文件系统写入量设置余量。

为避免每个 Put 都执行 `statfs`，Guard 在 `SpaceCheckInterval` 内缓存一次观察值，并对
每个获准的新 payload 做保守扣减；间隔到期后重新读取文件系统。其他进程写入、
Commit/Checkpoint/GC 的并发增长仍可能使观察值过时，因此所有底层 ENOSPC 处理保持不变。
受控 payload/reserve 从 admission 到 append 返回期间持有共享 refresh gate，刷新不会覆盖
仍在途的保守扣减；不同前台 append 之间仍可并发。
导出最近估算 available bytes、水位、stopped gauge、拒绝次数和检查错误次数。

## 8. 可观测性与验收

当前导出 active/frozen/reserved Delta charge、soft/hard limit、group batch/wait/fsync、GC copied/reclaimed、
GC throttled time、GC space rejection，以及前台空间水位。Checkpoint working bytes 与独立 Delta
backpressure wait time 仍属于后续可观测性收口。

验收必须覆盖：所有 frozen layer 计费、reservation 取消归还、fsync 后 Publish 不被限额阻塞、Checkpoint 连续失败时内存不越 hard limit、极小/溢出/不相容配置拒绝、Open 的 hard-limit mismatch，以及低水位拒绝新 Put 但不阻塞已有 Batch Commit/Abort、Get 和 Checkpoint。

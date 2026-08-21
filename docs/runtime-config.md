# 配置、预算与 Backpressure

状态：Development contract v1

## 1. 配置分类

配置分为两类：

- **FormatHardLimits**：决定已有字节是否可被安全解码，Create 时归一化后写入 Manifest，Open 后不可在线改变；
- **RuntimeBudgets**：只决定内存、调度和维护速度，可以在每次 Open 时调整，但必须通过跨字段校验。

零值表示采用默认值，不表示无限。所有 bytes 均为 binary bytes，所有加法、乘法和 int 转换使用 checked arithmetic。

## 2. 第一版 Config

```go
type Config struct {
    Dir string

    // Persisted format hard limits.
    SegmentSize        int64
    MaxValueSize       int64
    MaxBatchBytes      int64
    MaxBatchMutations  int
    MaxBatchConditions int
    MaxOpenBatches     int
    IDReserveSize      uint64
    BatchIDReserveSize uint64

    // Runtime memory and scheduling budgets.
    MappingCacheBytes     int64
    DeltaSoftLimitBytes   int64
    DeltaHardLimitBytes   int64
    CheckpointMemoryBytes int64
    MaxGroupBytes         int64
    MaxGroupBatches       int
    MaxGroupDelay         time.Duration
    GCBatchBytes          int64
    GCBatchMutations      int
    GCMinFreeBytes        int64
    GCBytesPerSecond      int64
}
```

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
| CheckpointMemoryBytes | 256 MiB |
| MaxGroupBytes | 8 MiB |
| MaxGroupBatches | 64 |
| MaxGroupDelay | 0 |
| GCBatchBytes | 16 MiB |
| GCBatchMutations | 4,096 |
| GCMinFreeBytes | SegmentSize |
| GCBytesPerSecond | 64 MiB/s |

这些是可运行的安全起点，不是性能承诺；基准可以改变后续默认值，但持久化 hard limits 的改变必须遵守 Open 兼容规则。

## 3. 归一化与跨字段校验

Create 先填充零值、再验证、最后把 8 个 FormatHardLimits 写入 INITIALIZING 与 Manifest。Open 以 Manifest 为权威；调用者对应字段为 0 时采用磁盘值，非 0 时必须完全相等，否则返回 `ErrConfigMismatch`。RuntimeBudgets 每次 Open 独立归一化。

必须满足：

- `4 KiB + largest aligned Frame/Descriptor + 4 KiB Footer <= SegmentSize <= 4 GiB`；
- `MaxValueSize <= MaxBatchBytes`，并且最大 PutRecord 可完整放入一个 Segment；
- 最大合法 Commit/Relocation Descriptor 可完整放入一个 Segment；
- count 字段能安全转换为实现使用的 `int` 和分配大小；
- `0 < DeltaSoftLimitBytes < DeltaHardLimitBytes`；
- `CheckpointMemoryBytes` 至少容纳一个最大编码 Node、排序块和 Header 批读窗口，但不要求容纳整个 Delta；
- `MaxGroupBytes > 0` 并限制多请求 group buffer；若单 Descriptor 更大，则该请求单独成组，不能拒绝一个已由 FormatHardLimits 允许的 Batch；
- `GCBatchBytes <= MaxBatchBytes` 且 `GCBatchMutations <= MaxBatchMutations`；
- `GCMinFreeBytes >= 0`；零值采用一个 Segment 的保留空间，不能用零值关闭预检；
- `GCBytesPerSecond > 0`；零值采用 64 MiB/s；
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

## 5. Checkpoint 有界构建

`CheckpointMemoryBytes` 是 Builder 私有工作集上限，不包含共享 Node Cache 和已计费 Delta。Builder 必须使用外部/分块排序或有界 run merge，不能假设所有 dirty ID、Header 或 SegmentStats 同时装入内存。

Header validation 按 `(SegmentID, offset)` 排序并使用有界窗口；生成的 SegmentStats 可流式 merge。若单个合法操作仍无法在预算内完成，返回明确配置错误并保留旧 Root/Stats/frozen Delta，不能 OOM 或安装部分 checkpoint。

## 6. Group Commit 与 GC 调度

`MaxGroupBytes` 只计算待写 Commit/Relocation Descriptor 编码字节和 coordinator buffer，不重复计算 Put 阶段已 append 的 Value。`MaxGroupDelay=0` 表示不主动 sleep；大于 0 时从首个排队请求开始计时，Context deadline 更早时不得为了凑组延迟。

GC 受 `GCBatchBytes/GCBatchMutations`、`GCBytesPerSecond`、Delta reservation、前台队列优先级和可用磁盘共同限制。每个 durable Relocation Batch 后按本轮累计 copied bytes 对 wall clock 做 Context-aware pacing；已经排队的用户 Commit 优先于下一批 Relocation，已经选中的单个有限 Relocation Batch 不被拆开。Runtime budget 只能降低后台工作速度，不能改变 Relocation durability、CAS 或删除门禁。

Data GC 使用两段磁盘 admission。安装 Maintenance Journal 前按 exact live bytes、Relocation Descriptor、两个 rotation Segment 与 `GCMinFreeBytes` 检查 copy 阶段空间；copy 完成后，GC-required Checkpoint barrier 先冻结其实际 Delta layers，再按冻结 entry 数乘以每个 entry 最坏八层 Dense Mapping COW、一个 rotation Segment与 `GCMinFreeBytes` 重新检查。第二段失败时 Relocation 已是可恢复的 durable garbage/Delta，但源 Segment 和旧 checkpoint 仍保留，Journal 安全撤销。这样前台 Commit 可以继续运行，又不会把只按源 live 数得到的估计误称为整个 Checkpoint 上界。任一检查低于上界均返回 `ErrInsufficientSpace`。可用空间检查只是 admission signal，并不保留磁盘配额；之后每个 write/fsync 的 `ENOSPC` 仍必须原样传播并遵守对应 crash-recovery phase。

## 7. 可观测性与验收

至少导出：active/frozen/reserved Delta charge、soft/hard limit、backpressure waiters/time、Checkpoint working bytes、group batch/bytes/wait/fsync、GC throttled time。

验收必须覆盖：所有 frozen layer 计费、reservation 取消归还、fsync 后 Publish 不被限额阻塞、Checkpoint 连续失败时内存不越 hard limit、极小/溢出/不相容配置拒绝，以及 Open 的 hard-limit mismatch。

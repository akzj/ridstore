# ridstore v2 磁盘空间 Admission

状态：Implemented

## 1. 目标与边界

磁盘水位的目标不是证明文件系统永远不会返回 ENOSPC，而是在空间接近耗尽时尽早停止新的用户数据，
给已接收 Batch 的 Commit、allocator reserve、Checkpoint 和 GC relocation 保留自救空间。

RecordLog 仍是业务无关的顺序写入器，负责真实 write/fsync 错误并在不确定时 fail-closed。空间策略由
Engine 持有，不能进入 RecordLog 的格式或 writer 状态机。

## 2. 写入分类

```text
用户 Put Record             -> 经过 space admission
Abort/Reserve/CommitGroup   -> 使用保留 headroom
Checkpoint marker           -> 使用保留 headroom
GC relocation Put/Commit    -> 使用保留 headroom
Mapping/Catalog/marker I/O  -> 使用保留 headroom
```

Put 被拒绝时返回 `ErrInsufficientSpace`，Batch 仍可 Abort。已经接受 Put 后不能因为水位策略拒绝 Commit，
否则会把可完成的事务主动变成 orphan；底层真实 I/O 错误仍可能使结果成为 CommitUnknown。

## 3. 预算算法

Engine 按 RecordLog 的 physical record size 预留，而不是按 Value 长度估算。Segment rollover 的 header、
footer、rotation journal 和 Catalog 写入不计入单条 Put 的 debit，而是由保留 headroom 覆盖。一个缓存的
filesystem available 样本维护两类扣减：

- concurrent reserved：已通过 admission、尚未由 RecordLog 接收完成；
- successful bytes：样本之后已被 RecordLog 接收的物理字节。

正常的定时刷新只在没有 outstanding reservation 时发生，避免把尚未反映到 statfs 的并发写入重复计算。
append 失败会立即使样本失效；此时即使仍有 outstanding reservation，下一次 admission 也必须重新采样，
并继续从新样本中扣除全部 reservation。这可能保守地重复扣减已经反映到 statfs 的字节，但不会超额准入。
采样失败按 `ErrInsufficientSpace` 拒绝用户 Put。

## 4. 配置

- `WriteStopFreeBytes`：用户 Put 完成后必须留下的最小可用字节；公开层默认 512 MiB；
- `SpaceCheckInterval`：无并发 reservation 时刷新 statfs 样本的最长间隔；默认 100 ms。
- `GCBatchBytes`：一个 relocation batch 的 Value bytes 上限；默认 16 MiB，并受持久化
  `MaxBatchBytes` 上限约束。单个合法 Value 可以超过该运行时预算并独占一个 batch。
- `GCBatchMutations`：一个 relocation batch 的 mutation 上限；默认 4096，并受持久化
  `MaxBatchMutations` 与 Commit descriptor 上限共同约束。
- `GCMinFreeBytes`：GC copy 和其后必要 Checkpoint 必须保留的空间；默认不高于
  `WriteStopFreeBytes` 的一个 Segment。
- `GCBytesPerSecond`：按本轮累计 copied physical bytes 计算的 relocation 速率；默认 64 MiB/s。

内部 Engine 配置允许 `WriteStopFreeBytes == 0` 关闭该策略，便于纯内存/单元测试；公开 API 的零值会
应用默认值。

## 5. 明确限制

- 其他进程可以在检查后消耗同一文件系统空间，因此 admission 不是 quota 或磁盘预留；
- 文件系统 block、metadata、CoW 和 delayed allocation 可能使估算与真实占用不同；
- GC relocation 所需空间取决于 live bytes，配置水位必须结合 SegmentSize 和 workload；
- 真正的 ENOSPC、short write 或 fsync 失败仍由各 durable writer 按原协议 fail-closed。

因此该机制改善可恢复性和错误时机，但不替代独占 filesystem、quota、容量告警或故障测试。

## 6. GC 两阶段 admission

Data GC 与用户 Put 共用同一个进程内空间 reservation 账本，但使用独立的
`GCMinFreeBytes` 水位：

1. copy 前按 source 全部物理数据、最坏每 Record 一个 relocation descriptor，以及两个 Segment
   rotation 余量保守预留；这允许低层 `RelocateSegment` 在没有 fresh stats 时仍保持安全上界；
2. relocation durable 后、GC-required Checkpoint freeze 后，按 frozen entry 数乘以八层 Dense Mapping
   Node 上界，再加一个 Mapping Segment 余量重新准入。

第二阶段拒绝时，已复制 Record 与 relocation Delta 保留为可恢复状态，source 仍在 Catalog，调用者可在
空间恢复后重试。任何 admission 都只是保守信号；真实 write/fsync 错误仍按原 durable 协议处理。

Relocation 每次 durable batch 后根据本轮累计 copied physical bytes 做 context-aware pacing。等待期间不持有
Coordinator 队列锁，已排队的用户 Commit 可以继续执行；取消只终止后续 GC 工作，不回滚已经 durable
的 relocation。

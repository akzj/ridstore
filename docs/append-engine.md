# Append Engine

本文定义 ridstore 的物理追加路径、批量边界和故障语义。批量写不改变 Format v1
Frame、Commit Descriptor 或恢复协议。

## 1. 分层

```text
Commit Coordinator     CommitSeq、冲突验证、Mapping publish
        |
Append Log protocol    构造 Put/Reserve/Commit/Relocation Frame
        |
Append Sequencer       唯一拥有请求顺序和 FrameSeq
        |
Buffered append path   reserve -> write -> sync，维护三个水位
        |
ActiveData             编码连续 Frame，一次 WriteAt
```

物理追加路径不判断 Batch 是否提交，也不决定 Mapping 可见性。它只接受已经构造并排好序的
Frame，分配稳定的 `VAddr = SegmentID + Offset`，按要求推进 write/sync 水位。Commit
Coordinator 仍是唯一分配 CommitSeq 和发布 Mapping 的组件。

生产路径由 `Sequencer` 启用有界 buffer。依赖注入 Log failpoint 的 crash-test 路径仍逐
Frame 写入，以保留 `PutWritten`、`PartWritten` 与 `SealWritten` 的精确进程崩溃边界；这条
测试路径不用于性能结论。

## 2. 正常写路径

Put 不再立即执行系统调用：

```text
AppendPut
  -> 校验并计算完整 Frame 大小
  -> 必要时 flush 已满 buffer 或 rotate
  -> 复制 payload 到 appendlog 自己拥有的内存
  -> 分配 FrameSeq 与稳定 VAddr
  -> 推进 reservedPos
  -> 返回 receipt
```

Sequencer 从当前队列提取一个有界 append cycle。每个请求只负责校验、构造并 stage Frame；
`Reserve`、`CommitGroup` 和 `Relocation` 只声明自己需要 durable completion，不在各自方法中
调用 write/fsync。cycle 中任一请求要求 durability 时，Sequencer 在全部请求 stage 后统一：

```text
Put1 Commit1 Reserve Put2 Commit2 Relocation ...
  -> ActiveData.AppendBatch      // 一个连续 buffer、一次 WriteAt
  -> ActiveData.Sync             // 一次 fsync
  -> 逐请求完成 durable result
```

因此，即使每个 Batch 只修改一条 Record，同一 CommitGroup 的 Put 与 Descriptor 也能共享
一次 write 和一次 fsync；相邻的多个独立 durable request 也可以共享同一个 cycle fsync。
`applyRequest` 不完成物理持久化，它只产生 request result 和 completion 条件。超过 buffer
Frame/byte 预算时可提前 write，但不提前 sync；后续 durable request 的 Sync 覆盖此前
written 前缀。单个合法大 Frame 可以超过聚合预算独立存在，但仍受 Format HardLimit 和
Segment 容量约束。

是否需要 fsync 是请求上的显式 `requestDurability`，而不是 Sequencer 根据业务名称临时推断：
Put/Abort 在 reservation 后即可完成，Reserve/Commit/Relocation 要等 cycle durable。请求类型
仍负责构造对应 Frame，但物理引擎只消费 Frame、completion level 和 marker cut。

尚未落盘的 Put 需要参与 Commit 校验和 GC relocation 校验。RecordReader 先查询 pending
address index，未命中再读取 Segment Registry。pending 查询返回 payload 副本，不能把可变
buffer 暴露给上层。

## 3. 三个水位

Append Engine 对外暴露：

```text
durablePos <= writtenPos <= reservedPos

reservedPos : 已分配稳定地址、且 payload 已由 appendlog 持有的尾部
writtenPos  : 已完整完成 WriteAt、本进程可从 Segment 读取的尾部
durablePos  : 已完成 Sync 的尾部
```

Put receipt 只要求到达 `reservedPos`。Commit、Reserve、Relocation 和 Barrier 要求自己的范围
到达 `durablePos`。Abort 本身不承诺独立 fsync，但可与后续 durable request 一起落盘；Close
必须 flush/sync 尚存的 pending Frame，不能静默丢失已经成功返回的 reservation。

一次 write 可以覆盖多个逻辑请求，一次 sync 也可以覆盖此前多个 write。水位是
`LogPos(SegmentID, endOffset)`，而不是记录地址；它表示扫描边界。

## 4. Barrier 与 CheckpointCut

Barrier 是零 payload 的同步标记语义，不需要写额外 Frame：Sequencer 中的请求顺序本身确定
marker 所在位置。Barrier flush 当前 pending、执行 Sync，并返回当时的 `CheckpointCut`、
`LastFrameSeq` 和 `NextFrameSeq`。

`CheckpointCut` 与“稍后观察到的最新 DurablePos”是两个概念。若未来允许 Barrier 后的数据
与其一起物理 write/sync，Manifest 的 `ReplayStart` 仍必须使用 marker 自己的 cut；否则可能
跳过尚未进入 Mapping checkpoint 的 durable Commit。

Barrier 当前是 append cycle 的逻辑边界：它与 marker 之前已经进入 cycle 的请求共享一次
write/fsync，但不提取 marker 后面的请求。这样返回时不会有后续 Frame 越过 cut。更高层 checkpoint
还必须保证 allocator high、open batches 和 terminal status 等外部状态取自同一 cut。若未来
允许这些状态在 Barrier 后继续发布，必须增加 completion publication fence 或按 cut 版本化
快照，不能用最新值拼接旧的 `CheckpointCut`。

## 5. Segment 边界

- Frame 永远不跨 Segment，pending buffer 也不能跨 Segment。
- 可用空间按 `ActiveData.Remaining - pendingBytes - SegmentSealReserve` 计算。
- 空间不足时先 flush pending，随后 seal/sync 旧 Segment，再创建新 Active Segment。
- Rotation 的 SegmentSeal 消耗一个 FrameSeq；新 Segment 上的计划必须用新起点重新编码。
- 地址一旦返回就不能因 flush 或 rotation 移动。
- 配置 Rotator 后，空 Segment 仍放不下合法请求属于容量不变量破坏：Log fail-closed 并返回
  `ErrCorrupt`，不能把内部 `segment.ErrFull` 泄漏给用户。
- Segment 边界可能强制额外 write/sync；“一次 write、一次 fsync”只保证同一 Segment 内的
  正常 CommitGroup 主路径。

## 6. 错误与所有权

- Context 在 reservation 前取消，不消耗 FrameSeq 或地址。
- reservation 成功后，调用者可以立即复用原始 value buffer；appendlog 已持有副本。
- `ActiveData.AppendBatch` 在写前完成全组编码和容量检查。
- write 错误、短写、预分配地址不匹配或 Sync 错误都会 poison Active Segment 并 fault Log；
  当前进程不能移动/复用已返回地址，也不能继续发布结果，必须重新 Open 扫描完整 Frame 前缀。
- flush 时 `nextFrameSeq` 不再推进；序号已在 reservation 时分配。
- pending index 只包含尚未完成 write 的 Frame；flush 成功后立即移除，读取自然退回 Registry。
- 磁盘空间观测必须从 `statfs` 可用量中再扣除 `reservedPos-writtenPos`；pending bytes 尚未
  反映在文件系统中，若刷新 Guard 时忽略它们，会重复发放同一份空间额度。

## 7. 验证与观测

自动化测试必须覆盖：

- Put reservation 不触发 write/sync，且调用者修改原 value 不影响 pending payload；
- 多个 Put 与 CommitGroup 形成一次 append write、一次 sync；
- `durablePos <= writtenPos <= reservedPos`，Commit/Barrier 后三个水位收敛；
- buffer 满时提前 write 但不错误推进 durable 水位；
- rotation 后地址、FrameSeq 和水位保持单调；
- canceled Put 不分配序号；Close flush pending；
- write/short-write/sync failure 后所有等待者得到确定错误且 Log fail-closed；
- pending-first RecordReader 覆盖用户 Commit 与 GC relocation；
- race、crash matrix 和 recovery replay。

后续 metrics 应加入 reserved bytes、written-not-durable bytes、group frames/bytes、write/syscall
次数、sync 次数、queue wait、write duration 和 sync duration。等待时间与 write/sync 时间的
比例用于判断压力来自 CPU、排队、系统调用还是设备持久化延迟。

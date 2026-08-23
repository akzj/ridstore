# Append Engine

本文定义 ridstore 的物理追加路径、批量边界和故障语义。它描述的是 Format v1
之上的运行时实现；批量写不改变磁盘 Frame 格式、Commit 协议或恢复规则。

## 1. 目标与当前实现

ridstore 的主要结构性优势是 Value 与 Descriptor 都只追加。写路径应尽量把多个
小 Frame 合并为较大的顺序写，使瓶颈接近设备带宽，而不是每条记录一次系统调用。

当前第一阶段已经实现：

```text
concurrent Put callers
        |
        v
bounded Sequencer queue
        |
        +-- drain adjacent queued Put requests
        |   stop at Commit / Abort / Reserve / Barrier
        v
Log.AppendPutGroup
        |
        +-- assign consecutive FrameSeq
        +-- split at Data Segment boundary
        v
ActiveData.AppendBatch
        |
        +-- encode complete Frames into one contiguous buffer
        +-- one WriteAt for one segment-local group
```

这一阶段只合并已经并发排队的 Put。`AppendPut` 返回时，其完整 Frame 已经交给内核
写入，但尚未因为 Put 本身执行 `fsync`。同一调用者同步地逐条 Put 不会被主动延迟来
凑批，也不会获得 batching；这是第二阶段要解决的问题。

## 2. 不变量

- Sequencer 是 Frame 顺序的唯一拥有者；批量不能越过非 Put 请求。
- 每个成功 Put 获得唯一且连续的 `FrameSeq`，取消或编码失败的请求不消耗序号。
- 一个 Frame 永远不跨 Data Segment。空间不足时，Log 先完成 Rotation，再在新
  Segment 分配 Put 的 `FrameSeq`；一个输入 group 可以被拆成多个物理写。
- `ActiveData.AppendBatch` 在写前完成整组编码和容量检查。容量不足不写入任何字节。
- 一次批量写发生错误或短写后，Active Segment 与 Log 都进入 poisoned/faulted
  状态；该进程不能继续发布该组的任何地址，必须通过重新 Open 扫描完整 Frame 前缀。
- `PointPutWritten` 仍对每个完整写入的 Put 触发；Segment 的 append-write hook 对
  每个物理 group 只触发一次。
- Commit Coordinator 仍是唯一生成 CommitSeal、分配 CommitSeq、执行 durable sync
  并发布 Mapping 的组件。Put batching 不赋予 Value 可见性。
- 队列、Frame 数和聚合字节数均有界。单个允许的大 Value 即使超过 group 字节预算，
  也必须能够独立执行。

## 3. 顺序与边界

Sequencer 收到第一个 Put 后，只提取队列中紧邻的 Put。以下任一条件立即封闭 group：

- 达到 `MaxGroupBatches`（在 append 层作为最大 Put Frame 数）；
- 达到 `MaxGroupBytes`；
- 队首是 Commit、Relocation、Abort、Reserve 或 Barrier；
- 当前队列中已没有请求。

生产路径不会为了 Put batching 主动等待 `MaxGroupDelay`，避免在已有 Group Commit
等待窗口之外再次增加延迟。`MaxGroupDelay` 仍只控制 Commit Coordinator。

调用者在请求进入有界队列以前可以因 Context 取消而返回。一旦请求被队列接受，
调用者必须等待 Sequencer 给出结果，因此调用者的 `Value` 在此之前始终归 append
路径安全引用。执行前已经取消的请求返回 Context error，不占用 FrameSeq。

## 4. 三个物理边界

要让同一调用者的连续 Put 也能合并，后续 staging 设计必须把一个 `end` 拆成：

```text
reservedEnd <= writtenEnd <= durableEnd  (含义按区间边界表达时反向理解)

reservedEnd : 已在有界内存 staging 中保留的逻辑尾部
writtenEnd  : 已完成完整 WriteAt、可以从当前进程读取的尾部
durableEnd  : 已由 fsync/fdatasync 建立持久性的尾部
```

更准确的不变量是 `durableEnd <= writtenEnd <= reservedEnd`。三者相等时没有待刷数据。
Put 可以在复制进 staging 后返回，但 Commit、Abort、Barrier、Rotation 和 Close 必须
等待其依赖范围达到相应的 written/durable 边界。内存预算、唤醒、错误广播、Context
取消后的 buffer ownership 和进程崩溃恢复都必须明确后，才能启用该语义。

第二阶段尚未实现。当前 `ActiveData.End()` 同时表示本进程已经完成写系统调用的尾部，
durability 仍由 Commit/Reserve/Barrier 的显式 Sync 建立。

## 5. 验证与观测

自动化测试覆盖：连续地址与可读性、整组容量预检、错误后 poison、取消不分配序号，
以及多个已排队 Put 只触发一次 Segment append-write hook。基准测试应分别比较单 Frame
与不同 group size，并同时记录 bytes/op、allocs/op 和 append syscall 数；只有在真实
文件系统与目标设备上的结果才能用于选择默认预算。

后续加入 staging 时，需要新增 queue wait、staging wait、write syscall duration、
sync duration、group frames/bytes 以及各边界积压量。等待时间与 write/sync 时间的比例
是判断系统压力来自 CPU、队列、系统调用还是设备持久化延迟的核心证据。

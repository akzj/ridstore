# ridstore v2 总体架构

状态：已实现的 v2 当前架构；production audit 持续进行

范围：`v2` 分支的当前架构与持续审计约束。本文不承诺兼容 Format v1，也不允许通过兼容层延续旧实现。

## 1. 为什么建立 v2 分支

ridstore v2 只保留一条从业务协议到通用 RecordLog 的追加路径，避免两套物理顺序、两套 Segment
生命周期和长期兼容代码。

v2 分支采用重新组装而不是渐进兼容：保留已经证明正确的产品边界与安全不变量，从一个通用
RecordLog 向上构建 ridstore。旧代码只在契约完整吻合时原样复用，否则重写或删除。

## 2. 开发原则

模块处置只有三种结果：

- `Keep`：职责、所有权、错误语义和并发模型与 v2 完全一致，可以直接成为新主路径；
- `Rewrite`：保留经过验证的不变量和测试思想，重新生成干净实现；
- `Delete`：目标架构中不存在该职责，或已经由另一个唯一所有者承担。

禁止 `Adapt`：

- 不为旧接口增加长期 adapter；
- 不同时保留两条生产路径；
- 不通过 feature flag 在旧、新引擎之间切换；
- 不为了复用代码而保留错误的模块边界；
- 不把“已有大量测试”当成保留错误结构的理由，测试应迁移到新契约。

生成新代码的成本低于维护错误抽象。每次 Review 优先判断模型是否正确，而不是改动行数是否少。

## 3. 项目定位保持不变

ridstore v2 仍然是嵌入式、单机、单目录独占的 Stable-ID Log-Structured Record Store：

```text
uint64 ID -> variable-length bytes
```

它仍然不提供任意字节 Key、排序、范围查询、LSM Level Compaction、SQL、复制或分布式事务。
ID 永不复用，Record 不原地修改，Batch 以 durable Commit Record 为原子可见边界。

## 4. 目标分层

```text
Public Store / Batch API
           |
           v
Commit Coordinator
  condition validation / CommitSeq / group admission
           |
           v
Ridstore Record Protocol
  Put / Commit / Abort / Reserve / Relocation codec
           |
           v
RecordLog
  opaque []byte / VAddr / queue / buffer / write / fsync
           |
           v
Physical Segments

Mapping <---- durable Commit publication
   |
Checkpoint + Catalog <---- authoritative durable snapshot
   |
GC ---- liveness proof / relocation / segment retirement
```

`RecordLog` 是物理子系统，不是 ridstore。它不知道 ID、BatchID、CommitSeq、Mapping、GC 或
Checkpoint 的业务含义。上层协议把每个 ridstore 事件编码成不透明 payload。

## 5. 唯一所有者

### 5.1 物理顺序

RecordLog 的单 writer 是唯一物理顺序所有者。v2 不再保留旧 `appendlog.Sequencer`。

所有生产者通过有界 channel 提交请求。writer 按接收顺序分配 VAddr、复制 payload、合并
Record，并推进三个水位：

```text
durable <= written <= reserved
```

不得在 RecordLog 之上再建立第二个负责物理顺序或 FrameSeq 的 goroutine。

### 5.2 Commit 顺序

Commit Coordinator 是 CommitSeq 和 Mapping 发布顺序的唯一所有者。它可以把一个 group 的多个
Commit Record 连续提交给 RecordLog，再统一等待 durable completion。Put 可以在物理日志中穿插，
但 Commit Record 的 CommitSeq 必须与 Coordinator 提交顺序一致。

### 5.3 Manifest

Catalog 是全局 Manifest generation 和安装顺序的唯一所有者。RecordLog、Mapping Checkpoint 和
GC 只能提交受字段所有权限制的 Catalog mutation，不能各自维护第二份权威 Manifest。

Manifest v2 至少包含：

- Store identity 和冻结的 HardLimits；
- RecordLog active/sealed/retired 文件集；
- Mapping active/sealed 文件集和 Mapping Root；
- Checkpoint 覆盖的 CommitSeq 与 RecordLog cut；
- allocator durable high watermarks；
- GC/maintenance 的恢复状态。

### 5.4 Mapping

Mapping 是 `ID -> VAddr` 可见状态的唯一所有者。RecordLog 中存在 Record 不表示它可见；只有
durable Commit 被 Coordinator 发布或 Recovery 重放后，Mapping 才能改变。

Mapping 不保存、推导或查询业务 Revision。条件提交比较调用者先前观察到的 VAddr 与提交顺序中
当前的 VAddr；不存在条件以零地址表达。解析条件只访问 Mapping，不能为了恢复另一种版本语义读取
PutRecord Header。这样冷 Root 条件检查至多发生一次 Mapping path read，而不是 Mapping read 后再做
一次随机 Data Record read。

VAddr 是 ridstore 内部物理观察 token，不是业务版本：GC relocation 会在 Value 不变时更新 VAddr，
因此并发条件可能产生安全的伪冲突。上层可以重读并重试；若 B-link tree、page engine 或其他业务
结构需要跨 relocation 稳定的 page epoch、MVCC version 或锁版本，它必须把该字段编码在自己的 Value
中，并由自己的锁或验证协议解释。Ridstore 不把 OriginBatchID 暴露为 LogicalRevision。

## 6. RecordLog 契约

### 6.1 请求与完成

RecordLog 保持单一同步动作。是否需要持久化只是请求属性，不产生不同的控制路径：

```go
type RecordLog interface {
    Append(context.Context, []byte, bool) (AppendResult, error) // bool means sync
    Read(context.Context, VAddr) ([]byte, error)
    Inspect(context.Context, VAddr, prefixBytes) (RecordMetadata, []byte, error)
    Scan(context.Context, LogPos, func(VAddr, []byte) error) error
    Status() LogStatus
    Close() error
}
```

语义：

- `sync=false` 在地址已预留且 payload 已复制后返回；
- `sync=true` 在地址已预留、数据已 write 且覆盖它的 fsync 成功后返回；
- `AppendResult` 同时返回 Record 的 VAddr 和紧随其后的 LogPos；后者是精确 checkpoint cut；
- 两种请求在返回后都允许调用者立即复用原始 buffer；
- writer 可以把多个 `sync=true` 请求放入同一次 write/fsync；
- Context 在地址预留前取消不产生记录；地址预留后不能撤销已经占用的日志位置；
- 任何不确定 write/fsync 状态都使 RecordLog fail-closed。

Coordinator 把一个 group 的多个 Batch Descriptor 编码成一个 `CommitGroupRecord`，只执行一次
`Append(sync=true)`。RecordLog 仍可把它与队列中其他请求自然合并，但不需要为业务 group 增加
`AppendGroup` 或异步 Receipt API。

### 6.2 物理 Record

RecordLog 的 envelope 只包含物理字段：

```text
magic / format version / physical size / payload size / VAddr / CRC / padding
```

Ridstore 的 RecordType、BatchID、CommitSeq、ID 和 mutations 全部位于 opaque payload 内。

一个 ridstore `CommitGroupRecord` 包含一个或多个完整 Batch Descriptor，并编码为一个 RecordLog
Record。HardLimits 必须保证任一合法单 Batch Descriptor 能放入空 Segment；Coordinator 的 group
byte limit 保证整个 CommitGroupRecord 也能放入。v2 不继续保留 CommitPart/CommitSeal 邻接协议。
若未来确实需要超过单 Record 的 Commit，必须重新设计显式 indirection，不能悄悄恢复业务分片。

### 6.3 Segment 边界

RecordLog 负责物理 Segment Header、Record envelope、Footer、active tail 修复和顺序写。Record
不得跨 Segment。空间不足时 writer 在分配用户 Record 地址前完成 rotation，因此已返回 VAddr
永不移动。

Catalog 仍是文件集的权威所有者：RecordLog 可以创建、seal 和打开物理文件，但 active/sealed
成员关系只有 Catalog 安装成功后才成为 committed metadata。rotation 必须有可重放 journal，
不能仅靠目录扫描猜测发布状态。

### 6.4 读取边界

RecordLog 统一解析 VAddr：

- reserved 但尚未 write：从 pending index 返回副本；
- written active Record：从 active Segment 读取；
- sealed Record：经内部 Registry 和 Reader Pin 读取；
- retired Segment：拒绝新 pin，等待已有 pin 后才允许删除。

SegmentStats 优先使用进程内 `VAddr -> {RecordID, PhysicalSize}` cache；hit 时跳过物理读。miss 时
`Inspect` 只读取并校验物理 Record Header 与调用者要求的 payload prefix；它返回不含 checksum
字段的 `RecordMetadata`，避免让 buffered Record 假装已经拥有磁盘 Header CRC。Checkpoint 用它读取
32-byte Put protocol header，因此统计阶段不读取大 Value body，也不声称验证未读取的 payload CRC。

读取能力属于物理 RecordLog，但 Mapping 决定某个地址是否代表当前逻辑值。调用者不能绕过
Mapping，直接把任意可读 Record 当成已提交数据。

## 7. Ridstore Record Protocol

第一版 v2 payload 类型：

```text
PutRecord
CommitGroupRecord
AbortRecord
IDReserveRecord
BatchIDReserveRecord
RelocationRecord
CheckpointMarker
```

协议层负责版本、类型、业务字段和业务 CRC。RecordLog envelope CRC 保护物理边界；协议 CRC
保护 ridstore 语义。两层校验不能互相替代。

`PutRecord` 使用 `sync=false`，取得 VAddr 后写入 Batch 的最终 mutation。`CommitGroupRecord` 使用
`sync=true`；其成功完成证明此前引用的 PutRecord 和整个 Commit group 均已持久化。

## 8. 正常提交

```text
1. Batch Put 向 RecordLog 提交 PutRecord，等待 reserved VAddr
2. Batch 只保存最终 ID -> VAddr/Delete mutation
3. Coordinator 用 ExpectedVAddr 串行验证条件并形成 virtual Mapping
4. Coordinator 按顺序分配 CommitSeq，编码一个 CommitGroupRecord
5. Append(CommitGroupRecord, sync=true)
6. RecordLog 合并 write 并执行 fsync
7. Coordinator 按 CommitSeq 发布 group 内各 Batch 的 Mapping
8. 最后向调用者返回 Committed
```

durable Commit 前不能发布 Mapping。fsync 结果不确定时返回 CommitUnknown 并使 Store fail-closed。

## 9. Checkpoint 与 GC

Checkpoint 取得一个明确的 RecordLog durable cut，构造新的 Mapping Root，然后通过 Catalog 原子
安装二者。不能把“稍后观测到的最新 durable position”拼接到较旧的 Mapping Root。

SegmentStats 仍是可重建派生状态，不进入 Commit 热路径。GC 候选只能由统计筛选；真正搬迁和
删除前仍必须验证：

```text
Mapping[RecordID] == scanned VAddr
```

Relocation Commit durable 并进入 Mapping Checkpoint 后，RecordLog Registry 先阻止新 reader 并
等待已有 pin；Catalog 随后移除旧 Segment，最后关闭文件并删除。Catalog 安装失败时撤销内存
retire gate，不能留下 Manifest 仍引用但当前进程不可读的 Segment。

## 10. VAddr 与 size tag

v2 可以采用已验证的低三位 size tag：

```text
uint32 SegmentID | aligned byte offset 的高 29 bit | 3-bit size tag
```

它是整个 v2 的唯一 VAddr 定义：

- Mapping 保存完整 tagged VAddr；
- Protocol Descriptor 保存完整 tagged VAddr；
- RecordLog 使用屏蔽标签后的 offset 定位；
- Scan 和 Recovery 根据 PhysicalSize 重建相同标签；
- LogPos 不带 size tag，不能与 VAddr 共用编码器；
- tag 与 Record Header PhysicalSize 不一致视为损坏。

Format v1 不兼容不是 v2 分支的约束。v2 Open 必须明确拒绝 v1 数据目录，后续如有需要再开发
独立离线迁移工具，生产路径不包含双格式兼容分支。

## 11. 不做的事情

- 不在 v2 分支继续维护旧 appendlog；
- 不用 adapter 让新 Coordinator 同时支持旧、新 Log；
- 不保留 FrameSeq，除非 Recovery 证明 VAddr 顺序无法表达必要约束；
- 不把 Mapping、事务、GC 或业务 codec 放入 RecordLog；
- 不以代码复用率作为架构质量指标；
- 不在总体协议冻结前开始批量搬运旧代码。

## 12. 持续 Review 门禁

每次扩展实现或声明里程碑完成前必须重新回答：

1. 单 Record Commit Descriptor 的最大尺寸约束是否可接受；
2. Append 的取消、关闭和错误完成是否无悬挂；
3. rotation 与 Catalog 安装失败时，哪个文件集合是权威状态；
4. Checkpoint cut 是否能和 Mapping Root 构成同一快照；
5. GC 删除顺序是否在每个崩溃点都可恢复；
6. 是否还存在第二个顺序、Manifest 或 Segment 生命周期所有者；
7. size-tag 是否在所有 VAddr 生产和消费路径上一致。

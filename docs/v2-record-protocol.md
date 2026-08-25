# ridstore v2 Record Protocol

状态：Draft for Review

Ridstore Protocol 位于 Public API/Coordinator 与通用 RecordLog 之间。每个协议消息是一个完整的
RecordLog payload；RecordLog 不解析本协议。

## 1. 共同编码规则

- Little Endian；
- protocol version = 2；
- 所有保留字段写零、读时要求为零；
- 所有可变数组有显式 count，并用 checked arithmetic 验证总长度；
- 未知 required type/flag 返回 Unsupported；
- RecordLog envelope 的 CRC32C 覆盖整个 protocol payload，因此协议层不重复存储 CRC；
- 解码器必须先验证长度上限，再分配内存。

每个 payload 以 16-byte 公共头开始：

| Offset | Size | Field |
|---:|---:|---|
| 0 | 4 | magic `RSP2` |
| 4 | 2 | protocol version = 2 |
| 6 | 1 | RecordType |
| 7 | 1 | flags |
| 8 | 2 | type header size |
| 10 | 2 | reserved |
| 12 | 4 | protocol payload size |

`protocol payload size` 必须等于外层 RecordLog Header 的 PayloadSize。

## 2. RecordType

| Value | Type | Durability |
|---:|---|---|
| 1 | PutRecord | reserved |
| 2 | CommitGroupRecord | durable |
| 3 | AbortRecord | reserved |
| 4 | IDReserveRecord | durable |
| 5 | BatchIDReserveRecord | durable |
| 6 | CheckpointMarker | durable |

Relocation 不增加新的顶层类型；它是 CommitGroupRecord 中的内部 Descriptor kind，并与用户 Commit
共享 CommitSeq 和 Mapping 发布顺序。

## 3. PutRecord

Header 固定 32 bytes：

| Offset | Size | Field |
|---:|---:|---|
| 0 | 16 | common header |
| 16 | 8 | OriginBatchID |
| 24 | 8 | RecordID |
| 32 | N | value bytes |

规则：

- OriginBatchID 和 RecordID 非零；
- value 可以为空；
- `N <= MaxValueSize`；
- LogicalRevision 等于 OriginBatchID，GC relocation 必须保持它不变；
- PutRecord 出现在日志中不代表可见，CommitGroupRecord 才发布它。

## 4. CommitGroupRecord

Group Header 固定 32 bytes：

| Offset | Size | Field |
|---:|---:|---|
| 0 | 16 | common header |
| 16 | 4 | DescriptorCount |
| 20 | 4 | TotalMutationCount |
| 24 | 8 | FirstCommitSeq |

随后是 `DescriptorCount` 个完整 Descriptor。Descriptor Header 固定 40 bytes：

| Relative Offset | Size | Field |
|---:|---:|---|
| 0 | 1 | DescriptorKind: UserCommit=1, Relocation=2 |
| 1 | 1 | flags |
| 2 | 2 | header size = 40 |
| 4 | 4 | MutationCount |
| 8 | 8 | BatchID |
| 16 | 8 | CommitSeq |
| 24 | 8 | LogicalPayloadBytes |
| 32 | 4 | DescriptorSize |
| 36 | 4 | reserved |

每个 Mutation 固定 32 bytes：

| Relative Offset | Size | Field |
|---:|---:|---|
| 0 | 8 | RecordID |
| 8 | 8 | NewVAddr，Delete 时为 0 |
| 16 | 8 | ExpectedOldVAddr，仅 Relocation 使用 |
| 24 | 1 | Operation: Put=1, Delete=2, Relocate=3 |
| 25 | 7 | reserved |

UserCommit 规则：

- BatchID 非零；
- mutation 按 RecordID 严格递增；
- Put 的 NewVAddr 非零、ExpectedOldVAddr 为零；
- Delete 的两个地址均为零；
- 条件检查结果不写日志，只有验证通过的最终 mutation 被编码；
- 空 Batch 合法，MutationCount 为零。

Relocation 规则：

- 使用非零内部 BatchID；
- 每项 Operation=Relocate；
- NewVAddr 和 ExpectedOldVAddr 均非零；
- Recovery 按 CommitSeq 重放相同 CAS，不能覆盖并发用户更新；
- LogicalPayloadBytes 表示搬迁的用户 value bytes，不包含协议 envelope。

Group 规则：

- DescriptorCount 非零；
- 第一个 Descriptor.CommitSeq 等于 FirstCommitSeq；
- 后续 CommitSeq 严格连续；
- TotalMutationCount 等于各 Descriptor MutationCount 之和；
- DescriptorSize 等于 `40 + 32 * MutationCount`；
- Group payload 大小等于 `32 + sum(DescriptorSize)`；
- UserCommit 和 Relocation 可以共享物理 group，但 Coordinator 必须已经确定全局顺序和 resolved plan。

## 5. AbortRecord

Header 固定 32 bytes：

| Offset | Size | Field |
|---:|---:|---|
| 0 | 16 | common header |
| 16 | 8 | BatchID |
| 24 | 4 | reason code |
| 28 | 4 | reserved |

AbortRecord 不撤销 PutRecord，不修改 Mapping。它用于有限期状态查询和恢复诊断，使用
`sync=false`；后续 durable Record 或 Close 可以使它持久化。缺失 AbortRecord 也不能使没有 Commit
的 Batch 变为可见。

## 6. Reserve Records

`IDReserveRecord` 和 `BatchIDReserveRecord` 均为固定 32 bytes：

| Offset | Size | Field |
|---:|---:|---|
| 0 | 16 | common header |
| 16 | 8 | HighExclusive |
| 24 | 8 | reserved |

HighExclusive 非零且只能增加。Append 必须使用 `sync=true`；成功之前不能发放新区间。

## 7. CheckpointMarker

固定 32 bytes：

| Offset | Size | Field |
|---:|---:|---|
| 0 | 16 | common header |
| 16 | 8 | CoveredCommitSeq |
| 24 | 8 | reserved |

Coordinator 建立 publication fence 后追加 Marker，使用 `sync=true`。AppendResult.End 是精确
checkpoint cut。Marker 本身不发布 Mapping Root；只有之后成功安装的 Manifest 同时引用 Root、
CoveredCommitSeq 和 cut 时，Checkpoint 才生效。

## 8. 尺寸约束

定义：

```text
PutPayloadSize             = 32 + ValueBytes
DescriptorSize             = 40 + 32 * MutationCount
CommitGroupPayloadSize     = 32 + sum(DescriptorSize)
PhysicalRecordSize(payload)= align8(32 + payload)
```

创建 Store 时必须验证：

```text
PhysicalRecordSize(32 + MaxValueSize) <= SegmentContentCapacity
PhysicalRecordSize(32 + 40 + 32 * MaxBatchMutations) <= SegmentContentCapacity
MaxCommitGroupPayload <= MaxRecordLogPayload
MaxRecordLogPayload <= uint32 max
```

`MaxGroupBytes` 是运行时聚合目标，不得低于一个已经接纳的单 Batch Descriptor 的实际大小。若队首
单 Batch 超过目标但仍满足持久化 HardLimits，应独立形成一个 CommitGroupRecord；不能永久等待。

以现有默认值估算，`MaxBatchMutations=1,000,000` 的单 Batch Descriptor 约 30.52 MiB，小于
256 MiB Segment；`MaxValueSize=64 MiB` 的 PutRecord 仍是更大的单 Record。v2 配置必须从这些上层
限制推导 `MaxRecordLogPayload`，不能继续使用原型独立的 16 MiB 默认值。

## 9. 解码顺序

```text
RecordLog 校验 envelope/size/VAddr/CRC
  -> Protocol 校验 common header/type/flags/总长度
  -> 校验 type-specific header
  -> checked arithmetic 验证 count 和 descriptor boundaries
  -> 校验排序、唯一性、CommitSeq 和 VAddr
  -> Recovery 或运行时消费
```

解析器不能接受尾随字节、非零 padding/reserved、未知 operation 或部分合法的 group。

## 10. 不保留的 Format v1 概念

- FrameSeq；
- CommitPart 和 CommitSeal；
- SegmentSeal 业务 Frame；
- RelocationPart 和 RelocationSeal；
- RecordLog envelope 中的 BatchID、RecordID、FrameType；
- 依赖相邻多个物理 Frame 才能判断一个 Commit 是否完整。

物理 Segment Footer 是 RecordLog 自己的格式，不是 ridstore Protocol Record。

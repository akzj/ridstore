# RecordLog v2 Contract

状态：Draft for Review

RecordLog 是 ridstore v2 唯一的物理追加和 Segment I/O 子系统。它只存储不透明字节，不认识
ridstore 的 ID、Batch、Commit、Mapping 或 GC 语义。

## 1. API

```go
type VAddr uint64

type LogPos struct {
    SegmentID uint32
    Offset    uint32
}

type AppendResult struct {
    Addr VAddr
    End  LogPos
}

type Log interface {
    Append(ctx context.Context, payload []byte, sync bool) (AppendResult, error)
    Read(ctx context.Context, addr VAddr) ([]byte, error)
    Scan(ctx context.Context, from LogPos, visit func(AppendResult, []byte) error) error
    Status() Status
    Close() error
}
```

`Append` 是唯一写动作。不存在 AppendPut、AppendCommit、Barrier、Flush request 或业务 Batch API。
Checkpoint 通过追加一个普通的固定大小 Marker 并设置 `sync=true` 获得精确 durable `End`。

`Read` 和 `Scan` 是引擎内部物理能力，不能直接暴露为 ridstore 用户 API。Mapping 决定 Record
是否代表当前逻辑值。

## 2. Append 线性化与所有权

调用过程：

```text
validate size
  -> acquire queued-byte budget
  -> enqueue request
  -> writer assigns Addr and End
  -> writer encodes final envelope and copies payload
  -> sync=false: complete after reservation
  -> write a bounded buffer
  -> optional fsync
  -> sync=true: complete after durable watermark covers End
```

不变量：

- 调用者在 `Append` 返回前必须保持 payload 不变；返回后可以立即修改或释放；
- writer 将 payload 直接复制到包含最终 VAddr 的 encoded Record，避免先复制 payload、写入时再复制；
- 成功返回的 `Addr` 在当前 Log incarnation 中唯一且永不移动；
- `End` 是该 Record 后的第一个字节位置，不是下一个 Record 的 VAddr；
- `sync=false` 成功只保证 reserved，崩溃后允许消失；
- `sync=true` 成功保证 `End` 及之前的日志前缀 durable；
- 上层只能在引用该地址的 durable Commit 成功后把它发布到持久化 Mapping；
- writer 发生不确定写入后不得重新分配已返回地址。

## 3. Context

Context 只允许在地址 reservation 前取消请求。因为 `Append` 返回前调用者仍拥有且保持原 payload，
writer 可以安全地在队列中读取它：

- byte budget 或 enqueue 阶段取消：没有地址、没有 Record，也没有复制；
- writer 尚未 reservation 且观察到取消：没有地址、没有 Record；
- reservation 完成后取消：请求继续达到其声明的 completion level，结果通道必须被完成；
- 调用者可以停止等待，但不能撤销已经进入日志顺序的 Record。

实现不得因为 Context 取消而遗留无人释放的 byte budget、阻塞 writer 或复用已分配位置。

## 4. 自然 batching

writer 不使用 MaxWriteDelay 或 MaxSyncDelay。它只依靠真实排队压力形成 batching：

```text
dequeue first request
drain currently available requests until record/byte/segment limit
reserve all valid records in order
encode one contiguous buffer
write buffer
if any covered request has sync=true: fsync once
complete requests at their required watermark
```

`sync=true` 是本次请求的完成条件，不是停止 drain 的边界。即使每个请求都要求 sync，队列中已经
存在的请求仍可以共享一次 write 和一次 fsync。队列暂时为空时立即处理，不主动 sleep 猜测未来负载。

buffer 满时允许先 write 而不 fsync。后续 `sync=true` 的 fsync 覆盖此前 written 前缀。

## 5. 三个水位

```text
durable <= written <= reserved
```

水位使用按 `(SegmentID, Offset)` 排序的 `LogPos`：

- reserved：已分配地址且 RecordLog 已拥有 payload；
- written：完整 Record 已成功 write；
- durable：fsync 成功覆盖的前缀。

水位不能使用 tagged VAddr，因为它们表示边界而不是 Record 起点。rotation 后位置按 SegmentID
递增形成全序，旧 Segment 的终点小于新 Segment 的内容起点。

## 6. VAddr

```text
63                         32 31                    3 2       0
+----------------------------+-----------------------+---------+
|         SegmentID          | aligned byte offset   | sizeTag |
+----------------------------+-----------------------+---------+
```

- SegmentID 0、VAddr 0 和 tag 7 非法；
- 实际 offset 为 `low32 &^ 7`；
- tag 0..6 分别提示首次读取 64、128、256、512、1024、2048、4096 bytes；
- tag 6 同时覆盖 PhysicalSize 大于 4096 的 Record；
- tag 根据包含 envelope 和 padding 的 PhysicalSize 计算；
- Record Header 中的 Addr、真实 offset、PhysicalSize 与 tag 必须全部一致。

VAddr 的数值顺序可以用于比较 Record 起点，但不能通过 `addr2-addr1` 计算物理距离。所有 offset
运算必须先解析 VAddr。

## 7. 物理格式

v2 沿用原型已经验证的基本布局：

```text
Segment Header   64 bytes
Record Header    32 bytes
Payload          N bytes
Zero Padding     align to 8 bytes
Segment Footer   64 bytes
```

Record Header：

| Offset | Size | Field |
|---:|---:|---|
| 0 | 4 | magic `R2RC` |
| 4 | 2 | format version |
| 6 | 2 | header size = 32 |
| 8 | 4 | PhysicalSize |
| 12 | 4 | PayloadSize |
| 16 | 8 | tagged VAddr |
| 24 | 4 | payload CRC32C |
| 28 | 4 | header CRC32C |

所有整数 Little Endian。单个 Record 不跨 Segment，PhysicalSize 不超过 uint32，SegmentSize
不超过 `math.MaxUint32`。

Segment Header：

| Offset | Size | Field |
|---:|---:|---|
| 0 | 8 | magic `RIDAPV2H` |
| 8 | 2 | format version = 2 |
| 10 | 2 | header size = 64 |
| 12 | 4 | SegmentID |
| 16 | 4 | configured SegmentSize |
| 20 | 4 | PreviousSegmentID，首段为 0 |
| 24 | 16 | RecordLogID |
| 40 | 4 | first content offset = 64 |
| 44 | 16 | reserved |
| 60 | 4 | header CRC32C |

Segment Footer：

| Offset | Size | Field |
|---:|---:|---|
| 0 | 8 | magic `RIDAPV2F` |
| 8 | 2 | format version = 2 |
| 10 | 2 | footer size = 64 |
| 12 | 4 | SegmentID |
| 16 | 4 | DataEnd |
| 20 | 4 | reserved |
| 24 | 8 | FirstAddr，空 Segment 为 0 |
| 32 | 8 | LastAddr，空 Segment 为 0 |
| 40 | 8 | RecordCount |
| 48 | 12 | reserved |
| 60 | 4 | footer CRC32C |

## 8. Segment 生命周期

RecordLog 内部可以拆分 writer、segment、registry、rotation，但对外只有一个物理子系统：

```text
Creating -> Active -> Sealed -> Cleaning -> Retired -> Trash -> Deleted
```

- 任意时刻只有一个 Active Segment；
- SegmentID 单调递增且永不复用；
- rotation 在 writer 顺序内发生，待追加请求在新 Segment 创建后才获得地址；
- Catalog 是 Active/Sealed 成员关系的 durable 权威；
- Registry 是进程内 fd、reader pin 和状态切换的所有者；
- RecordLog 不决定 Segment 是否逻辑可删除，GC 提供已经满足的删除授权；
- Catalog 移除成功前不能 retire/delete 文件。

## 9. 读取

`Read` 首先查询 pending index，未命中再通过 Registry 定位 Segment。磁盘读取按 size tag：

- PhysicalSize 不大于 hint：一次 `ReadAt` 完成；
- tag 6 且 PhysicalSize 大于 4096：先读 4096，校验 Header，再读剩余部分；
- 不重复读取首块；
- 返回 payload 的独立所有权，不能暴露复用 buffer。

`Scan` 先从 writer 取得一个稳定 snapshot：扫描上界、最后地址和 pending 副本。它只访问该 snapshot
覆盖的前缀，不能追逐并发 append。恢复扫描只接受最大完整前缀；普通 sealed 扫描遇到损坏立即失败。

## 10. Rotation 与 Catalog 接口

RecordLog 不维护第二份 Manifest。rotation 在 writer 内串行执行，但通过 Catalog 安装文件集：

```go
type SegmentSet struct {
    Active        SegmentSummary
    Sealed        []SegmentSummary
    NextSegmentID uint32
}

type CatalogPort interface {
    SnapshotRecordLog() (generation uint64, set SegmentSet)
    InstallRotation(expectGeneration uint64, oldSealed, newActive SegmentSummary) error
}
```

正式实现可以调整 Go 类型，但必须保持：一个 Catalog generation、compare-and-install、字段所有权
校验、安装前文件 durable、安装后 Registry publication。目录扫描只能发现文件，不能发布成员关系。

## 11. GC 维护接口

热路径 `Log` 与维护接口分开：

```go
type Maintenance interface {
    ScanSegment(ctx context.Context, id uint32, visit func(AppendResult, []byte) error) error
    RetireSegment(ctx context.Context, id uint32, expectGeneration uint64) error
}
```

`ScanSegment` 只接受 sealed Segment，并在整个扫描期间持有内部 Reader Pin；callback 得到通过 RecordLog
envelope CRC 校验的完整 payload。RecordLog 不解析 Put、Commit 或 GC 语义。

这些方法只执行物理生命周期，不判断 liveness。GC 必须先完成 Mapping CAS、Checkpoint 和精确
liveness 证明，才可调用 `RetireSegment`。后者内部执行 retire gate、等待 Reader Pin、Catalog 移除、
detach、close、trash rename 和删除。Catalog 移除失败时撤销 retire gate。

## 12. Close 与错误

Close 顺序：

```text
stop accepting -> wait accepted submitters -> drain queue
-> write pending -> fsync -> complete waiters -> close files -> release lock
```

Close 会持久化所有已经成功返回 `sync=false` 的 Record；这些 Record 即使没有 Commit，也只是
可回收 orphan。重复 Close 返回同一结果。

write、short write、fsync、rotation 或 Catalog 安装造成状态不确定时：

- terminal error 只设置一次；
- 所有已接纳等待者都必须完成；
- 新 Append 立即返回 poisoned error；
- Read 可以继续服务已知安全前缀，是否允许由 Store 的 fail-closed 状态统一决定；
- 只能重新 Open 扫描和恢复，当前进程不得继续写。

## 13. 配置约束

持久化硬限制：SegmentSize、MaxRecordLogPayload、format version。运行时限制：buffer bytes/records、
channel capacity、queued bytes 和读缓存预算。

必须满足：

```text
EncodedRecord(MaxRecordLogPayload)
    <= SegmentSize - SegmentHeaderSize - SegmentFooterSize
```

RecordLog 不独立持久化一份配置；这些硬限制来自唯一 Manifest。

## 14. 当前代码映射

- `internal/recordlog/types.go`：VAddr、LogPos、AppendResult；
- `internal/recordlog/format.go`：Segment/Record 二进制 codec；
- `internal/recordlog/segment.go`：Active/Sealed Segment、tail recovery、size-hint read 和 scan；
- `internal/recordlog/registry.go`：active/sealed lookup、Reader Pin、Retiring 和 detach；
- `internal/recordlog/budget.go`：有界 queued-byte admission；
- `internal/recordlog/writer.go`：唯一 writer、自然 batching、write/fsync 和 poison；
- `internal/recordlog/log.go`：Append/Read/Scan/Status/Close；
- `internal/recordlog/catalog.go`：RecordLog 所需的窄 Catalog port；
- `internal/recordlog/rotation_journal.go`、`open.go`：rotation journal、Open 和崩溃恢复；
- `internal/recordlog/retire.go`：Reader drain、Catalog remove、trash/unlink；
- `internal/storecatalog/catalog.go`：直接实现 RecordLog Catalog port，不存在兼容 adapter；
- `internal/recordlog/*_test.go`：golden、边界、corruption、Reader Pin、group commit、process crash 和 fuzz。

M2 已实现本契约的物理主路径，但尚未接入 v2 Store/Coordinator。旧 Format v1 runtime 仍然编译，
不调用上述 v2 实现。

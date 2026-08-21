# 嵌入式 Library 与 API 契约

状态：Development contract v1

## 1. 交付形态

ridstore 第一阶段是嵌入式 Go Library，不是独立进程，也不是分布式系统。

```text
Application Process
  └── ridstore Library
        ├── Append/Commit Coordinator
        ├── Mapping
        ├── Recovery
        └── Local Data Directory
```

第一版进程模型：

- 一个 Store 对应一个本地数据目录；
- 一个进程以 writer 身份独占目录；
- 同一 Store 支持多个 goroutine 并发调用；
- 不提供 RPC、鉴权、多租户和跨进程共享；
- 不提供主备、共识、分布式 ID 或网络分区语义；
- 未来 `ridstored` 只能作为 Library 的包装层，不能改变底层提交协议；
- 复制层必须在单机协议稳定后独立设计。

选择 Library 的原因：避免 RPC 和二次序列化，使 Mapping Lookup、Segment Pin、mmap/pread 和 Batch Commit 保持在同一进程内。

## 2. 数据目录所有权

`Open` 必须获得独占目录锁。锁在 `Close` 完成后释放；进程崩溃由操作系统释放。

约束：

- 同一路径不能同时存在两个 writer；
- 目录锁失败返回 `ErrLocked`，不能降级为只读并继续；
- Library 不读取或写入配置目录之外的文件；
- 不跟随数据目录内部指向目录外部的符号链接；
- Restore、Scrub 等离线工具同样必须持有目录锁。

## 3. 第一版公开类型

以下是语义契约，最终 Go 命名可以在实现 Review 中做小幅调整。

```go
package ridstore

type ID uint64
type BatchID uint64
type CommitSeq uint64
type Revision uint64

type Record struct {
    Value    []byte
    Revision Revision
}

type BatchState uint8

const (
    BatchStateOpen BatchState = iota + 1
    BatchStateCommitting
    BatchStateCommitted
    BatchStateAborted
    BatchStateCommitUnknown
)

type BatchStatus struct {
    BatchID   BatchID
    State     BatchState
    CommitSeq CommitSeq // non-zero only when State == BatchStateCommitted
}

type Config struct {
    Dir                string
    SegmentSize        int64
    MaxValueSize       int64
    MaxBatchBytes      int64
    MaxBatchMutations  int
    MaxBatchConditions int
    MaxOpenBatches     int
    MappingCacheBytes  int64
    DeltaSoftLimitBytes int64
    DeltaHardLimitBytes int64
    CheckpointMemoryBytes int64
    MaxGroupBytes      int64
    MaxGroupBatches    int
    MaxGroupDelay      time.Duration
    GCBatchBytes       int64
    GCBatchMutations   int
    IDReserveSize      uint64
    BatchIDReserveSize uint64
}

func Create(cfg Config) (*Store, error)
func Open(cfg Config) (*Store, error)

type Store struct { /* private */ }

func (s *Store) Get(ctx context.Context, id ID) ([]byte, error)
func (s *Store) GetRecord(ctx context.Context, id ID) (Record, error)
func (s *Store) Begin(ctx context.Context) (*Batch, error)
func (s *Store) Status(ctx context.Context, id BatchID) (BatchStatus, error)
func (s *Store) Checkpoint(ctx context.Context) error
func (s *Store) Close() error

type Batch struct { /* private */ }

func (b *Batch) ID() BatchID
func (b *Batch) Allocate(ctx context.Context) (ID, error)
func (b *Batch) Put(ctx context.Context, id ID, value []byte) error
func (b *Batch) Delete(ctx context.Context, id ID) error
func (b *Batch) ExpectRevision(id ID, revision Revision) error
func (b *Batch) ExpectAbsent(id ID) error
func (b *Batch) Commit(ctx context.Context) (CommitResult, error)
func (b *Batch) Abort(ctx context.Context) error

type CommitResult struct {
    BatchID   BatchID
    CommitSeq CommitSeq
}
```

字段分类、零值默认、持久化硬限制、跨字段校验和 Delta admission 语义以 [runtime-config.md](runtime-config.md) 为准。配置不接受“0 表示无限”。

第一版不暴露任意 `[]byte` Key、Iterator、Range Scan、Snapshot、Column Family 或 TransactionDB API。

`Create` 只接受不存在或除 ridstore 初始化临时状态外为空的目录，并执行可恢复初始化；已有有效 Store 返回 `ErrAlreadyExists`。`Open` 只打开已有 Store，不隐式创建；缺少有效 CURRENT/Manifest 且没有可恢复 INITIALIZING marker 时返回 `ErrNotInitialized`。这样路径拼写错误不会静默创建一个新空库。

## 4. ID 语义

- `ID(0)` 永远无效；
- `Allocate` 返回全局唯一、单调递增、永不复用的 ID；
- ID 只有在成功 `Put` 并 Commit 后才对应可读取 Record；
- Allocate 后 Abort、进程崩溃或从未 Put 都会形成永久空洞；
- Delete 后 ID 不可重新绑定成另一个逻辑对象，但允许对同一逻辑 ID 再次 Put；
- Library 不解释 ID 的业务含义。

为了避免每次 Allocate 都执行 fsync，Store 按区间持久化预留 ID。预留成功后才向调用者发放该区间中的 ID。恢复后从已持久化 reserved high watermark 继续，不使用未确认的低值。

BatchID 同样通过独立的 durable reserve range 发放。`Begin` 只有在对应 BatchID 区间已经持久化后才能返回；进程崩溃、Abort 和状态过期都不会导致 BatchID 复用。这样 `Status(BatchID)` 不会把旧调用误关联到恢复后的新 Batch。

## 5. Batch 语义

Batch 状态：

```text
Open -> Committing -> Committed
Open -> Aborted
Committing -> Aborted (condition conflict or definite pre-Seal failure)
Committing -> CommitUnknown
```

规则：

- Batch 不是 goroutine-local；可以传递，但同一 Batch 的方法不能并发调用；
- Store 的不同 Batch 可以并发；
- 同一 Batch 多次修改同一 ID，最后一次操作决定提交状态；
- 早先 append 的 Record 不撤销，成为 GC 候选；
- Commit 成功表示整批已 durable 且已在当前进程发布；
- Abort 成功只表示该 Batch 永远不会被发布，不表示物理字节已删除；
- Commit 开始后调用者不能 Abort；coordinator 仍可因条件冲突或确定的 pre-Seal 错误将其置为 Aborted；
- CommitUnknown 后调用者只能查询 Status 或重新 Open，不能假设成功或失败；
- Close 遇到 Open Batch 时主动 Abort；遇到 Committing Batch 时等待确定结果或返回错误。

Batch 不提供跨 Store 原子性。

## 6. Put 与数据所有权

`Put` 在返回前消费 `value` 的全部内容。返回后调用者可以安全修改或释放原 slice。

第一版 Put 会把 Record append 到 Active Segment，但不会执行 durable fsync，也不会发布 Mapping。这样大 Batch 不需要把所有 payload 留在内存中。

用户 Put 产生的 LogicalRevision 等于该 Batch 的 BatchID，并保存在 PutRecord Header 已有的 BatchID 字段中；该字段对 PutRecord 定义为 OriginBatchID。同一 Batch 对同一 ID 的多次 Put 只有最终值可见，因此共享 Revision 不产生歧义。Revision 只在同一 ID 内作为相等性 token；不同 ID 即使 Revision 相同也没有版本关系。调用者不能解释其数值、排序或将其当作物理地址。GC Relocation 保留 OriginBatchID，所以不会制造新 Revision。

限制：

- `len(value) <= MaxValueSize`；
- Batch 已写入的逻辑 payload 总量不得超过 `MaxBatchBytes`；
- Batch 最终不同 ID 数量不得超过 `MaxBatchMutations`；
- 超限在写入前返回 `ErrValueTooLarge` 或 `ErrBatchTooLarge`；
- 第一版不提供部分 Put；一个 Put 要么产生完整可校验 Frame，要么返回错误并使 Batch Failed。

`PutReader`、零拷贝 View 和 `ReadAt` 延后到第一版格式稳定后设计；它们不能改变 Commit 协议。

## 7. Get 语义

第一版 `Get` 返回独立复制的 `[]byte`；`GetRecord` 同时返回 Value 和当前 LogicalRevision：

- 返回后不持有 Segment pin；
- 调用者拥有返回 slice；
- `GetRecord.Value` 同样是独立复制，成功时 Revision 必须非 0；
- Get 不提供跨多次调用的一致 Snapshot；
- 单次 Get 观察某一个完整 committed Mapping 状态；
- 并发 Commit 前后，Get 可以返回旧值或新值，但不能返回部分 payload、Abort 值或错误 ID 的值；
- Delete 已发布后返回 `ErrNotFound`；
- CRC 错误返回 `ErrCorrupt` 并使 Store 进入故障状态。

`Get` 等价于丢弃 `GetRecord` 的 Revision。Revision 在 GC Relocation 前后保持不变；只有用户 Put 改变 Revision。

未来需要 B-Tree 多 Record 一致遍历时，单独设计 `ReadView`；不能把多个独立 Get 默认为 Snapshot。

### 7.1 Mapping 维护

`Checkpoint(ctx)` 将当前 durable Data Log cut、Persistent Mapping Root、allocator high watermark 与精确 SegmentStats 同代安装。`CompactMapping(ctx)` 先推进 Checkpoint，再把可达 Mapping Node 复制到全新的文件 generation，安装新 Root，等待旧 Root reader 退出后通过 trash 协议回收全部旧 Mapping 文件。两者都与 Close 协调，但 Mapping copy 期间用户 Commit 可以继续进入新 active Delta。

`MappingSpaceUsage(ctx)` 显式遍历 durable Root，返回 Mapping Node 的 Total/Reachable/Unreachable encoded bytes；它是可能执行冷 I/O 的维护查询，不属于廉价 Metrics snapshot。

## 8. 并发覆盖

默认 Blind Put 对同一 ID 采用 commit-order last-writer-wins：

```text
CommitSeq 较大的 Batch 成为最新值
```

需要防止丢失更新时，Batch 可以增加条件：

```text
ExpectRevision(ID, Revision)  // 当前 Record 必须仍是该逻辑版本
ExpectAbsent(ID)              // 当前必须为 NotFound
```

条件可以约束本 Batch 未修改的 ID，用于验证跨 Record 的上层结构。Commit Coordinator 在全局提交顺序中的确定位置一次性验证全部条件；全部满足才允许生成 CommitSeal，任一失败则整个 Batch 进入 Aborted、返回 `ErrConflict`，已 append 的 PutRecord 成为垃圾。条件失败是确定未提交，不是 CommitUnknown。

条件始终针对应用本 Batch mutation 之前的 committed/virtual 状态，与调用 Expect/Put/Delete 的先后顺序无关；验证通过后才原子应用本 Batch 的全部最终 mutation。

同一 ID 最多有一个条件；重复的相同条件幂等，冲突条件使 Batch Failed。Revision 0 无效。条件只提供乐观并发控制，不提供 Snapshot、读集自动跟踪、Serializable 隔离、自动重试或自动 merge。

条件数量受 `MaxBatchConditions` 限制；条件 ID 在提交前排序，验证所需内存和冷读次数必须有界。

没有条件的 Blind Batch 不执行 Revision Mapping/Header 读取；LogicalRevision 复用已有 BatchID 字段，因此普通 Put 不增加 Record Header 空间。冲突检测成本只由显式使用条件的 Batch 承担。

GC Relocation 使用内部 expected-old-VAddr CAS，不改变用户并发语义。

## 9. Context 与取消

- Begin/Allocate/Put 在进入不可撤销内部动作前响应取消；
- Put 已成功 append 后收到取消，该 Record 仍是当前 Open Batch 的一部分；调用者应 Abort；
- Commit 一旦写入 Commit Seal 就不能因 Context 取消而宣告 Abort；
- Conditional Commit 在生成任何 CommitPart/Seal 前取消，属于确定未提交并进入 Aborted；
- Commit 在 Delta budget admission 等任何 pre-Seal 阶段取消，属于确定未提交并进入 Aborted；
- CommitSeal 已交给 append/write 后等待期间 Context 取消可能返回 `ErrCommitUnknown`；
- 后台 fsync 和恢复不能因单个调用者取消而停止到不一致状态；
- Close 使用调用者显式超时策略，不在内部静默放弃持久化任务。

## 10. 错误模型

稳定错误类别：

```go
var (
    ErrInvalidID       = errors.New("ridstore: invalid id")
    ErrNotFound        = errors.New("ridstore: not found")
    ErrLocked          = errors.New("ridstore: directory locked")
    ErrAlreadyExists   = errors.New("ridstore: store already exists")
    ErrNotInitialized  = errors.New("ridstore: store is not initialized")
    ErrInvalidConfig   = errors.New("ridstore: invalid configuration")
    ErrConfigMismatch  = errors.New("ridstore: configuration does not match store format")
    ErrClosed          = errors.New("ridstore: closed")
    ErrReadOnly        = errors.New("ridstore: read only after storage fault")
    ErrBatchClosed     = errors.New("ridstore: batch closed")
    ErrBatchFailed     = errors.New("ridstore: batch failed")
    ErrBatchTooLarge   = errors.New("ridstore: batch too large")
    ErrValueTooLarge    = errors.New("ridstore: value too large")
    ErrInvalidRevision  = errors.New("ridstore: invalid revision")
    ErrConflict         = errors.New("ridstore: optimistic conflict")
    ErrIDExhausted      = errors.New("ridstore: id space exhausted")
    ErrAddressExhausted = errors.New("ridstore: physical address space exhausted")
    ErrGenerationExhausted = errors.New("ridstore: metadata generation exhausted")
    ErrCommitUnknown   = errors.New("ridstore: commit outcome unknown")
    ErrStatusExpired   = errors.New("ridstore: batch status expired")
    ErrCorrupt         = errors.New("ridstore: corruption detected")
    ErrUnsupported     = errors.New("ridstore: unsupported format")
)
```

I/O 错误必须保留底层 cause。发生以下错误后 Store 进入只读故障状态：

- fsync、rename 或 directory sync 无法确定；
- committed Record CRC 错误；
- Manifest 与文件集合冲突；
- Mapping 指向不存在的 Segment；
- Append 出现 short write 且无法证明尾部状态。

只读故障状态允许 Status、Get 已验证数据和 Close；是否允许 Get 由具体故障范围决定，默认 fail closed。

## 11. Close 语义

Close：

1. 阻止新的 Begin/Get；
2. Abort 尚未 Commit 的 Open Batch；
3. 等待已进入 Commit 的 Batch 得到确定结果；
4. 停止新的 Checkpoint/GC；
5. 等待维护任务到可恢复边界；
6. fsync 必需元数据；
7. 关闭 Mapping Cache、Segment 和目录锁。

Close 返回成功不是恢复正确性的前提。所有 crash test 必须通过强制终止进程验证。

重复 Close 返回 `ErrClosed`，不 panic。

## 12. 配置默认值与硬上限

第一版建议默认值：

| 配置 | 默认值 | 硬约束 |
|---|---:|---:|
| SegmentSize | 256 MiB | `<= 4 GiB` |
| MaxValueSize | 64 MiB | `< SegmentSize - metadata` |
| MaxBatchBytes | 256 MiB | 可配置但受磁盘空间和恢复预算限制 |
| MaxBatchMutations | 1,000,000 | Commit Descriptor 必须可分帧 |
| MaxBatchConditions | 1,000,000 | 条件集合必须可在内存中排序和验证 |
| MaxOpenBatches | 1024 | 超限 backpressure |
| MappingCacheBytes | 256 MiB | 最小值覆盖 Root/上层节点 |
| IDReserveSize | 1,048,576 | 必须为正数 |
| BatchIDReserveSize | 65,536 | 必须为正数 |

默认值是工程起点，不是磁盘格式常量。打开已有 Store 时，格式硬约束来自 Manifest。

## 13. 明确不兼容的未来能力

以下能力不能通过悄悄扩展第一版 API 实现：

- 任意 Key 和范围查询；
- 多进程共享 writer；
- 跨 Store 事务；
- 分布式 ID/复制；
- 将 BatchID 当作业务幂等 Key；
- 使用 SyncNone 结果冒充 durable commit。

独立进程服务若出现，应使用不同包或 `cmd/ridstored`，并保留这里的 Library 语义。

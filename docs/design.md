# ridstore 总体设计

状态：Draft v0.1，等待 Review

范围：单机存储内核

当前阶段：设计，不承诺磁盘格式兼容性

项目与 LSM/RocksDB 的能力边界和性能判定标准见 [positioning-vs-lsm.md](positioning-vs-lsm.md)。该文档属于项目定位约束，而不是可随实现便利改变的优化建议。

## 1. 项目定位

ridstore 是一个 Stable-ID Log-Structured Record Store。它将可变逻辑记录转换为不可变物理记录：

```text
Logical ID ──Mapping──> Physical VAddr ──> Immutable Record
```

外部模型只有：

```text
uint64 ID -> variable-length bytes
```

底层通过顺序 append、Batch 合并和 group fsync 避免小块随机覆盖写。更新和删除不撤销既有物理写入；不可见记录由 GC 回收。

### 1.1 内核负责

- 分配稳定、单调递增且永不复用的 ID；
- Get、Put、Delete；
- 多记录 Batch 原子提交；
- append-only Record Segment；
- ID 到最新物理位置的 Mapping；
- 崩溃恢复、Mapping Checkpoint；
- 旧版本、Abort 数据和无效副本的 GC；
- 读操作与 Segment 回收之间的生命周期保护；
- 明确区分 Committed、Aborted 和 CommitUnknown。

### 1.2 内核不负责

- 任意字节 Key、Key 排序和范围查询；
- B-Tree Page、Blob、Stream、消息、时序数据等上层模型；
- TTL、保留周期和业务分块；
- SQL、事务隔离级别和业务索引；
- 压缩算法、冷热分层和复制协议的第一版实现；
- 将所有 Mapping 常驻内存。

Page、Blob 或 Stream 可以使用 ridstore 的 ID 表达自己的对象，但不能反向进入 ridstore 的核心语义。

ridstore 明确不以替代 RocksDB、LevelDB 或其他通用有序 KV 引擎为目标。任意字节 Key、Key 排序、范围查询、Prefix Scan、Sorted Run 和 Level Compaction 不属于 ridstore 的自然演进方向。若未来需求需要这些能力，应优先把它们实现为上层数据结构或直接选择成熟 LSM，而不是扩张 ridstore 内核。

## 2. 核心不变量

以下不变量优先于性能优化和具体数据结构。

### I1：ID 稳定且永不复用

- ID 0 保留为无效值；
- ID 单调递增；
- Abort 或崩溃可以产生 ID 空洞；
- Delete 后 ID 不会代表另一个对象；
- 分配器可以按区间预留，避免每次 Allocate 都 fsync。

ID 永不复用消除 ABA：旧引用最多得到 NotFound，不能读到另一个对象。

### I2：物理记录不可原地更新

Put 总是 append 新 Record。Mapping 从旧 VAddr 切换到新 VAddr 后，旧 Record 变为 GC 候选。

### I3：Commit 成功前不可发布 Mapping

Batch 的所有 Record 和 Commit Marker 持久化成功后，才能使新的 Mapping 对读者可见。

### I4：Mapping 不能指向未持久化数据

Commit Marker、Record payload 和校验信息必须处于同一个有序 append 持久化域，或者由协议证明 payload 先于 Marker 持久化。第一版优先使用同一 Record Log，避免 metadata-only WAL 指向未落盘 payload。

### I5：Batch 只有全可见或全不可见

Put/Delete 的物理写入可以逐条发生，但逻辑可见性以 Batch 为单位发布。读者不能观察到半个已发布 Batch。

### I6：Mapping 是可重建状态，不是唯一真相

Committed Record Log 是权威历史。Mapping Checkpoint 只缩短启动恢复时间，损坏或丢失时必须能够从受支持的 Log 历史恢复。

### I7：GC 不能改变逻辑内容

GC 只允许改变 Record 的物理 VAddr。任何并发更新都必须使过期的 GC 搬迁 CAS 失败。

### I8：没有全量 Mapping 加载前提

系统能否 Open 和运行不能取决于所有有效 ID 的 Mapping 是否能放入内存。内存只决定缓存命中率，不决定可寻址容量。

## 3. 概念模型

### 3.1 ID

```go
type ID uint64
```

完整 ID 空间为 `1..2^64-1`。任何 Mapping 实现都不能引入低于 uint64 的固定寻址上限。

按每秒分配 10 万 ID 计算，uint64 仍可使用约 584.9 万年。真正的容量约束是当前有效 Mapping 和物理数据规模，而不是 ID 消耗速度。

### 3.2 VAddr

VAddr 是内部物理地址，不暴露给使用者。概念上至少包含：

```text
Segment identity + byte offset
```

Record Length 保存在 Record Header 中，因此 Mapping 可以只保存紧凑 VAddr。VAddr 的最终位布局属于磁盘格式决策，目前保持开放。

### 3.3 Record

概念格式：

```text
RecordHeader {
    format_version
    record_type       // Put, Delete, BatchBegin, BatchCommit, BatchAbort...
    batch_id
    id
    payload_length
    header_crc
}
payload
record_crc
```

要求：

- Record 有明确边界；
- Header 能在不信任 Length 的情况下进行上限校验；
- CRC 覆盖影响解析和语义的所有字段；
- 尾部 torn write 可被确定识别；
- 未知 format version 必须拒绝，不能猜测解析；
- 具体二进制布局在实现前通过独立格式文档确定。

### 3.4 Mapping Entry

逻辑状态只有：

```text
ID -> VAddr
ID -> NotFound
```

Length、CRC 和类型从 Record Header 获取。Delete 被 Mapping Checkpoint 吸收后不保留永久 Tombstone；Mapping 空间应与当前有效 ID 相关，而不是与历史最大 ID 相关。

## 4. 外部 API 草案

API 表达语义，不提前绑定 Go 的最终接口细节。

```go
type Store interface {
    Get(ctx context.Context, id ID) (Record, error)
    Begin(ctx context.Context) (Batch, error)
    Close() error
}

type Batch interface {
    Allocate() (ID, error)
    Put(id ID, value []byte) error
    Delete(id ID) error
    Commit(ctx context.Context) error
    Abort() error
}

type Record interface {
    ID() ID
    Bytes() []byte
    Close() error
}
```

`Record.Close` 为 Segment pin、mmap view 或 buffer 生命周期预留。第一版可以返回复制后的 `[]byte`，但接口不能阻止未来的零拷贝或流式读取。

大 Value 接口保持开放。可能增加：

```go
PutReader(id ID, size uint64, src io.Reader) error
Open(id ID) (io.ReadCloser, error)
ReadAt(id ID, offset int64, dst []byte) error
```

这些接口不能改变 Batch 原子性。

### 4.1 Allocate 的语义

Allocate 只产生 ID，不立即写入空内容。常见使用方式：

```text
Begin
  id = Allocate
  Put(id, value)
Commit
```

只 Allocate 而未 Put 的 ID 在 Commit 后是否成为“存在的空 Record”目前不做隐式推断。第一版倾向要求显式 Put；未绑定内容的 ID 永久形成空洞。

### 4.2 同一 Batch 重复修改同一 ID

Batch 中可以多次 Put/Delete 同一 ID。提交后的最终 Mapping 只采用该 Batch 对 ID 的最后一个操作；此前物理 Record 自动成为 GC 候选。

## 5. Batch 原子提交

### 5.1 状态机

```text
Open
 ├── Abort                         -> Aborted
 ├── Commit + durable fsync        -> Committed
 └── Commit durability uncertain   -> CommitUnknown
```

进入 Commit 后不再接受 Put/Delete/Abort。

### 5.2 正常提交

```text
1. 为所有 Put/Delete 生成完整 Record Frame
2. append BatchCommit(batch_id, mutation_count, batch_crc)
3. 将一个或多个待提交 Batch 合并 write
4. 执行一次 fsync/fdatasync
5. 设置 Mapping publish barrier
6. 应用本批全部 ID -> VAddr / NotFound
7. 结束 publish barrier
8. 返回 Commit 成功
```

多个 Batch 可以共享一次 fsync，但各自保留独立 BatchID、CRC 和原子可见性。

### 5.3 Abort

大 Value 可以在 Commit 前已经 append。Abort 不执行物理撤销：

```text
append BatchAbort（如果后续解析需要）
不发布 Mapping
返回 Aborted
```

所有已写 Record 因没有可见 Mapping 而成为 dead record。GC 的基本存活判断是：

```text
Mapping[record.ID] == record.VAddr
```

### 5.4 CommitUnknown

`fsync` 返回错误不能证明 Commit Marker 未落盘。此时不能谎报 Abort：

```text
Commit Marker 可能 durable
调用者没有收到确定结果
=> CommitUnknown
```

发生 CommitUnknown 后，第一版应将 Store 置为只读或故障状态，通过重新 Open 和恢复确认 BatchID。BatchID 必须支持查询或幂等重试；具体接口待定。

### 5.5 大 Batch 的写入调度是开放决策

需要在实现前比较：

1. Batch 连续写入：恢复最简单，但一个大 Batch 可能阻塞其他写者；
2. 多 Batch Record 交错：吞吐和流式写更好，但恢复需要维护 pending batch；
3. 内存/临时文件 staging：提交日志连续，但可能产生内存压力或双写。

无论采用哪一种，都必须满足：Commit Marker 排在该 Batch 所有 Record 之后，且 Marker durable 意味着此前 Record durable。

## 6. Mapping 架构

Mapping 是 ridstore 最关键的性能路径，但第一版设计不能把某一种实现写成永久限制。

### 6.1 必须满足的能力

```go
type Mapping interface {
    Lookup(id ID) (VAddr, bool, error)
    Publish(batchID uint64, changes []MappingChange) error
    CompareAndSwap(id ID, old, new VAddr) (bool, error)
    Checkpoint(logPosition LogPosition) (MappingRoot, error)
}
```

要求：

- 覆盖完整 uint64 ID；
- 不按历史 high-watermark 全量分配；
- 不要求启动时全量加载；
- 内存缓存有明确上限；
- 空 Mapping Leaf 可以删除并向上剪枝；
- Delete Checkpoint 后不保留永久 Tombstone；
- 支持 Batch 原子发布；
- 支持 GC 的 expected-old-VAddr CAS；
- Checkpoint Root 和对应 Log Position 必须一致。

### 6.2 目标形态

```text
Committed Delta Overlay
          ↓ miss
Persistent Sparse Mapping
          ↓
Bounded Mapping Page Cache
```

启动时只加载 Manifest/Root。冷 Mapping Page 按需从磁盘读取，热点受容量限制地缓存在内存。

Persistent Mapping 候选包括 Adaptive Radix、Sparse Radix、B+Tree 或其他分页索引。实现前用独立原型比较以下指标，不在总体设计中提前指定赢家：

- 热 Lookup 延迟；
- 冷 Lookup I/O 次数；
- 每个有效 ID 的磁盘字节数；
- Delete 后空间收敛；
- 随机 Update 的 Checkpoint 写放大；
- 启动恢复时间；
- Cache 命中率与内存预算；
- 增量 Checkpoint 吞吐是否追得上提交速率。

Leaf 可以根据实际密度选择 Dense、Bitmap 或 Sparse 编码；这是数据分布自适应，不属于业务语义。

### 6.3 信息下界

如果 N 个有效 ID 可以独立指向任意 VAddr，精确随机 Lookup 至少需要 O(N) 位置信息。ridstore 能做到的是把冷 Mapping 放在磁盘并限制内存 Cache，而不是承诺 Mapping 与有效 ID 数量无关。

### 6.4 第一版允许的简化

为验证 append、Batch、Recovery 和 GC，第一版可以实现内存 Chunk/Hash Mapping，但必须：

- 通过 Mapping 接口隔离；
- 磁盘 Record 格式不依赖内存布局；
- 测试完整 uint64 边界；
- 文档明确它不是最终容量方案；
- 不把全量加载写进 Store.Open 契约。

### 6.5 Batch 可见性

持久化原子性由 Commit Marker + CRC + fsync 保证；运行时可见性需要独立 publish barrier。

第一版可用短临界区：

```text
append/fsync：不持有 Mapping publish lock

fsync 成功后：
  lock
  应用整个 Mapping batch
  unlock
```

后续可以用 epoch/seqlock、COW Root 或不可变 Delta Set 优化，但必须提供相同的全批可见语义。跨多个 Get 的一致读视图是否进入内核 API，属于待决问题；上层数据结构不能默认多个独立 Get 自动形成 Snapshot。

## 7. Segment 与 Manifest

### 7.1 Segment

Segment 是顺序追加的物理文件。状态：

```text
Active -> Sealed -> Retired -> Deleted
```

原则：

- 同一时刻每个 writer domain 只有受控的 Active append 顺序；
- Segment 达到阈值或显式要求时 Seal；
- Seal Footer 包含格式版本、有效长度、记录统计和 CRC；
- Active 尾部允许 torn record；Sealed Segment 不允许静默截断；
- 文件名不能作为唯一权威状态；Manifest 必须能检测缺失、重复和未知文件。

### 7.2 Manifest

Manifest 至少记录：

- store format version；
- Store UUID；
- 当前 Active/Sealed Segment 集合；
- 下一 Segment identity；
- 当前 Mapping Root；
- Mapping Root 覆盖的 Log Position；
- 最近确定完成的维护状态。

Manifest 采用 temp write、file fsync、atomic rename、directory fsync 发布。任何无法证明安全的目录状态都应阻止读写 Open，而不是自动猜测修复。

### 7.3 目录所有权

Store Open 时必须持有独占目录锁。两个进程不能同时以 writer 身份打开同一数据目录。

## 8. Recovery

启动恢复顺序：

```text
1. 获取数据目录独占锁
2. 校验 Store UUID、格式版本和 Manifest
3. 加载最新有效 Mapping Root
4. 确认 Root 对应的 checkpoint_log_position
5. 从该位置顺序扫描 Record Log
6. 校验 Record 边界、CRC、Batch 状态和 Commit 顺序
7. 只应用完整 committed batch
8. 忽略 aborted batch
9. 丢弃或截断最后一个不完整物理尾部
10. 重建 Delta Overlay、ID allocator 和 Active Segment 状态
11. 完成一致性检查后才开放读写
```

恢复不能依赖 Close 被调用。正常进程退出只是优化，不是正确性前提。

### 8.1 Batch 恢复

- 有完整 Commit Marker 且 CRC 正确：Committed；
- 有 Abort Marker：Aborted；
- 没有终止 Marker：Uncommitted，Mapping 不应用；
- Commit Marker 或 Record CRC 错误：Corruption，不得把后续不确定字节当成合法记录；
- BatchID 重复：只有协议允许的幂等重试可以接受，否则视为冲突。

## 9. GC 与空间回收

### 9.1 Record 存活判定

用户 Record 在扫描时存活，当且仅当：

```text
current Mapping[ID] == scanned Record VAddr
```

因此以下记录天然为垃圾：

- Abort Batch 中的 Record；
- 同一 Batch 内被后续操作覆盖的 Record；
- 被后续 Put 覆盖的旧版本；
- 已 Delete 的 Record；
- CAS 失败的 GC 副本。

### 9.2 搬迁协议

```text
1. 选择候选 Sealed Segment
2. 扫描 Record
3. 对当前仍存活的 Record append 副本
4. append GC Relocation Commit，包含 ID、OldVAddr、NewVAddr
5. fsync 新 Record 和 Relocation Commit
6. CAS Mapping(ID, OldVAddr, NewVAddr)
7. 建立覆盖 Relocation 的 Mapping Checkpoint
8. 将旧 Segment 标记 Retired
9. 等待读者 pin 清零
10. 删除文件并 fsync 目录
```

恢复重放 Relocation 时也必须执行 expected-old-VAddr 条件，避免旧 GC 操作覆盖更晚的用户 Put。

### 9.3 Reader Pin

读取不能只拿到 VAddr 后直接访问文件，否则 GC 可能在中间删除 Segment。概念协议：

```text
Lookup VAddr
Acquire/Pin Segment
必要时重新验证 Mapping/Segment 状态
读取 immutable Record
Release/Unpin Segment
```

Retired Segment 只有在 pin count 为 0 且恢复不再依赖时才能物理删除。

### 9.4 Log 与数据是同一个 Segment 时的回收约束

Segment 既保存 payload，也保存恢复所需的 Commit 历史。删除前必须同时满足：

- 没有 Mapping 指向其中的 Record；
- Reader pin 为 0；
- 当前 durable Mapping Checkpoint 已覆盖其中所有需要的 Commit/Relocation；
- Manifest 已持久化新的安全边界。

## 10. 并发模型

第一版优先选择可证明的简单模型：

- 单一 append sequencer 决定物理日志顺序；
- 多个调用者并发准备 Record；
- commit coordinator 合并 write 和 fsync；
- Mapping publish 以 Batch 为原子单位；
- Get 不参与 append 锁；
- GC 通过 VAddr CAS 与用户 Put 竞争；
- Segment Registry 管理 pin、retire 和 delete。

需要明确锁顺序并在实现文档中固定。建议所有模块遵循：

```text
Store lifecycle -> Append sequencer -> Mapping publish -> Segment registry
```

实际实现不得在持有 Mapping publish lock 时执行磁盘 fsync 或长时间数据复制。

## 11. 关键失败时序

### 11.1 大 Put 后 Abort

```text
payload 已 append
Abort
Mapping 未发布
=> 记录不可见，GC 回收
```

### 11.2 Commit Marker 写入后、fsync 返回前崩溃

恢复以实际磁盘内容和 CRC 为准。调用者若未得到结果，状态为 CommitUnknown。

### 11.3 fsync 成功后、Mapping 发布前崩溃

恢复看到完整 Commit，重新发布 Mapping。Commit 不丢失。

### 11.4 Mapping 发布到一半时进程崩溃

内存部分状态丢失。恢复只根据完整 Commit 重建整批 Mapping，不能把运行时发布进度当作持久化状态。

### 11.5 GC 复制完成但 Relocation 未提交

新副本没有 Mapping 指向，是垃圾；旧 Record 仍有效。

### 11.6 Relocation 已提交但旧 Segment 尚未删除

存在重复物理数据但逻辑 Mapping 唯一；后续 GC 继续回收。

### 11.7 删除旧 Segment 时崩溃

Manifest、Mapping Checkpoint、retire journal 和目录 fsync 顺序必须保证恢复能判定删除应继续、已完成或必须停止。具体维护 journal 格式在 GC 设计文档中确定。

## 12. 性能模型

ridstore 的主要收益来自：

```text
随机覆盖写
→ 顺序 append
→ 合并 write
→ group fsync
```

前台 Put 成本近似：

```text
序列化 + append copy + 摊销 fsync + Mapping publish
```

Get 成本近似：

```text
Mapping Lookup + Segment Pin + Record Header/Payload Read
```

性能不能通过削弱 durability 获得后仍标记为相同语义。基准必须分别报告：

- durable single Put；
- durable concurrent Put/group commit；
- Batch Put；
- Abort large Put；
- hot/cold Mapping Lookup；
- recovery replay；
- GC 与前台并发；
- Mapping Checkpoint 收敛速度；
- fsync 延迟分布和 queue wait/fsync 占比。

## 13. 可观测性

至少暴露：

- allocated ID high watermark；
- committed/aborted/unknown Batch 数量；
- append bytes、dead bytes、live bytes；
- group commit batch size；
- commit queue wait、write、fsync、publish latency；
- Active/Sealed/Retired Segment 数量；
- GC copied/reclaimed bytes 和 CAS failures；
- Mapping Overlay 大小；
- Mapping Cache bytes、hit/miss、cold-load latency；
- checkpoint position、duration、dirty mapping count；
- recovery scanned bytes、batches 和 duration；
- pinned reader/segment 数量。

指标不参与正确性决策，不能因为指标丢失改变提交结果。

## 14. 验证策略

### 14.1 单元和属性测试

- Record 编解码、长度边界、CRC；
- ID allocator 永不重复；
- Batch 最终操作折叠；
- Mapping Publish 全有或全无；
- GC CAS；
- Sparse Mapping 删除和节点剪枝；
- 格式版本拒绝；
- 随机操作模型与参考 map 对比。

### 14.2 崩溃测试

在每个持久化边界注入崩溃：

- Record Header/Payload/CRC 中间；
- Commit Marker 前后；
- write 后 fsync 前；
- fsync 后 publish 前；
- Mapping Checkpoint temp/rename/dir sync；
- GC copy/relocation/retire/delete 各阶段；
- Manifest 更新各阶段。

测试必须使用子进程强制终止并重新 Open，不能用正常 Close 模拟崩溃。

### 14.3 工程门禁

- `go test ./...`；
- `go test -race ./...`；
- `go vet ./...`；
- fuzz Record/Manifest/Checkpoint decoder；
- 静态锁顺序审计；
- 文件描述符、内存、磁盘空间收敛测试；
- 长时间 soak 只能在自然结束并检查报告后宣称通过。

## 15. 分阶段研发

### Phase 0：格式和故障模型

- 确定 Record、Batch、Segment Footer、Manifest 的版本化格式；
- 写出 fsync/rename/directory sync 顺序；
- 建立 crash-test harness；
- 不实现 GC 和最终 Mapping。

### Phase 1：最小正确主路径

- 单 Active Segment；
- ID allocator；
- Put/Delete/Get；
- 单 Batch Commit/Abort；
- 简单内存 Mapping；
- 重启扫描恢复；
- CommitUnknown 处理。

验收：所有定义的 Commit 崩溃点均满足原子性。

### Phase 2：并发与 group commit

- append sequencer；
- commit coordinator；
- 多 Batch 合并 write/fsync；
- Mapping Batch publish；
- 大 Value 写入调度；
- queue wait 和 fsync 压力指标。

验收：并发不改变单 Batch 语义，吞吐随并发增加且延迟可解释。

### Phase 3：Mapping 持久化与有界内存

- Delta Overlay；
- Mapping Root/Checkpoint；
- 按需 Mapping Page Cache；
- 完整 uint64、稀疏 ID、大量 Delete 测试；
- 比较候选 Persistent Mapping 原型后再选择格式。

验收：Open 不全量加载 Mapping，内存受配置上限约束。

### Phase 4：Segment GC

- live ratio 统计；
- relocation commit；
- Mapping CAS；
- Segment pin/retire/delete；
- crash-resumable maintenance journal。

验收：GC 与 Put/Get 并发时无错误数据、无提前删除，空间最终收敛。

### Phase 5：运维完整性

- Scrub/Verify 工具；
- Backup/Restore 格式；
- 资源限制和 backpressure；
- 长时间 soak；
- 格式兼容和升级策略。

复制、主备和分布式 ID 不属于单机内核第一阶段；只有单机恢复语义稳定后再单独设计。

## 16. 待 Review 的关键决策

以下问题保持开放，不能在实现中偷偷决定：

1. Batch Record 是否允许物理交错；
2. 大 Value 采用直接 Record、分帧 Record，还是 staging；
3. CommitUnknown 的查询和幂等重试 API；
4. 单个 Value 的尺寸上限；
5. Get 返回复制数据、pin view 还是流式 Reader；
6. Mapping 第一版和最终实现之间的替换边界；
7. Persistent Mapping 选择 Adaptive Radix、B+Tree 还是其他分页结构；
8. Batch 发布是否只保证单次 Get，还是提供多 Get ReadView；
9. GC Relocation 与普通 Batch 是否使用相同 Record 协议；
10. Segment identity、offset 和 VAddr 的位布局；
11. Segment 最大尺寸和 Seal 策略；
12. ID 区间预留大小和 high-watermark 持久化方式。

这些决策需要通过失败时序、复杂度和实测数据共同确定，而不是仅凭局部性能直觉选择。

## 17. 第一版明确不做的事

- 不实现 Page Store 或 Blob Store；
- 不实现任意 Key 的 KV API；
- 不实现 LSM Mapping，除非 Mapping 原型比较证明需要；
- 不实现 SQL、TTL、Stream 和业务索引；
- 不把外部数据库作为权威 Mapping；
- 不通过正常 Close 测试替代 crash test；
- 不为了基准数字跳过 fsync 后仍声称 durable；
- 不在单机协议稳定前设计主备复制。

## 18. 设计判定标准

设计成功不是“代码功能很多”，而是以下问题都有可验证答案：

1. Commit 返回成功后，任意崩溃点是否都能恢复该 Batch？
2. Abort 后是否永远不可见且最终可回收？
3. Mapping 是否可能指向未持久化或已删除的 Record？
4. GC 与用户更新竞争时，谁能覆盖谁，依据是什么？
5. Mapping 超出内存时，系统是否仍能 Open 和提供服务？
6. 删除 Segment 前，恢复、读者和 Mapping Checkpoint 是否都已脱离它？
7. fsync 返回不确定错误时，是否会谎报提交或回滚？
8. 每一项性能收益是否都在相同 durability 语义下测量？

只有这些约束稳定后，ridstore 才适合作为 Page、Blob、索引或其他上层存储结构的基础。

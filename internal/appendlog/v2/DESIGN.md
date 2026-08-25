# Append Log v2

状态：独立原型开发中，尚未接入 ridstore。本文和 v2 代码都不改变当前 Format v1 与运行路径。

## 1. 定位

Append Log v2 是一个业务无关的顺序追加引擎。它只接受不透明的 `[]byte`，为每次追加
分配稳定 `VAddr`，并根据调用者的 `sync` 标志决定返回时是否已经持久化。

```text
Upper-layer protocol
  Put / Commit / Abort / Checkpoint / Message / Event / ...
                         |
                         | opaque []byte + sync flag
                         v
Append Log v2
  VAddr / buffer / write / fsync / segment / recovery
                         |
                         v
                    Segment files
```

v2 提供机制，上层定义含义。它不是事务管理器、不是 ridstore WAL，也不是现有 Format v1
的兼容层。它首先作为独立组件开发、测试和压测；是否接入 ridstore 必须另行设计和 Review。

## 2. 最小契约

v2 的核心 API 只有一个动作：

```go
type VAddr uint64

type Log interface {
    Append(ctx context.Context, data []byte, sync bool) (VAddr, error)
    Read(ctx context.Context, addr VAddr) ([]byte, error)
    Scan(ctx context.Context, from VAddr, fn func(VAddr, []byte) error) error
    Status() Status
    Close() error
}
```

### 2.1 Append

每次调用恰好追加一个 Record，并始终返回该 Record 的稳定地址：

```go
addr, err := log.Append(ctx, data, false)
```

成功表示：

- v2 已复制并拥有 `data`；
- `addr` 已经分配且永不移动、复用或重新解释；
- Record 可能仍在内存 buffer 中；
- 不承诺掉电后仍然存在。

这里的“不复用”限于当前成功 Open 的 Log 实例。崩溃恢复会截掉未持久化的 active tail，随后
可以重新使用该物理 offset；因此上层不能在 durable Commit 之前把 `sync=false` 返回的地址发布
到日志之外。若跨崩溃仍需区分被丢弃的临时地址，必须把上层事务代际纳入引用，不能假设 VAddr
本身提供该保证。

```go
addr, err := log.Append(ctx, data, true)
```

成功表示：

- 当前 Record 已持久化；
- 日志顺序中位于它之前的所有 Record 也已经持久化；
- 返回的 `addr` 可以作为该次持久化前缀的业务 cut。

`sync` 只决定 completion 条件，不改变 Record 格式。所有 Record 在物理层完全一致。

### 2.2 上层自主解释

Append Log 不知道 payload 是什么。ridstore 可以自行编码：

```text
Put(recordID, value)             -> Append(payload, false) -> putAddr
Commit(batchID, putAddr, hash)   -> Append(payload, true)  -> commitAddr
Checkpoint(commitAddr, root)     -> Append(payload, true)  -> checkpointAddr
```

消息系统也可以用完全不同的 payload：

```text
Message(topic, body)             -> Append(payload, true)
```

如果上层不需要地址，可以忽略返回值。appendlog 不因地址是否被使用而改变行为。

### 2.3 B-link-tree / COW Page 示例

`sync=false` 的核心价值不是简单的“异步写”，而是在物理写入前取得稳定 VAddr。上层可以用
这些地址继续构造更高层的数据结构：

```text
addrA = Append(PageA, false)
addrB = Append(PageB{child: addrA}, false)
addrC = Append(PageC{child: addrB}, false)
commitAddr = Append(Commit{newRoot: addrC, pages: [addrA, addrB, addrC]}, true)
```

在 buffer 容量允许时，writer 可以执行：

```text
PageA + PageB + PageC + Commit
               |
               v
          one write + one fsync
```

若事务大于 buffer，允许提前 write，但不需要提前 fsync：

```text
PageA + PageB  -> write
PageC + Commit -> write + fsync
```

Commit 的 fsync 覆盖整个先前 written 前缀，所以 PageA、PageB 也达到 durable。Segment 边界
可能产生额外 write 或物理收尾同步，因此“一次 write”是常见性能结果而不是协议保证；真正的
保证是 Commit 成功返回后，它和之前引用的所有 VAddr 都已持久化。

Append Log 不发布新的 tree root。上层必须遵守：

```text
Commit durable 前：新 Page 不可见；崩溃后是可回收 orphan
Commit durable 后：才允许发布新的 root / mapping
Recovery：只接受完整合法的 Commit，忽略没有 Commit 的 Page
```

因此同一个追加机制既能服务 B-link-tree Page COW，也能服务普通 Record Store，而不引入
Page、Tree、Root 或 Transaction 类型。

### 2.4 不需要特殊 Barrier

Checkpoint marker 是上层协议的一种普通 Record：

```go
cut, err := log.Append(ctx, encodedCheckpointMarker, true)
```

成功返回后，marker 和它之前的日志都已持久化，`cut` 就是上层需要的 checkpoint cut。

v2 不提供特殊 `Barrier` request，不维护 marker 类型，也不理解 Checkpoint。若纯物理用户只想
强制同步而没有业务 payload，可以追加一个零长度 Record；零长度 payload 仍具有正常 envelope
和 VAddr，不是控制旁路。

## 3. v2 负责与不负责的边界

### 3.1 唯一负责

- 复制并拥有调用者提交的 payload；
- 按唯一日志顺序分配稳定 VAddr；
- 将 payload 包装成通用物理 Record；
- 合并多个请求，减少 write 和 fsync；
- 根据 buffer、rotation 和 `sync` 自动执行 write；
- 根据 `sync=true` 自动执行 fsync；
- 管理 Segment 创建、封口和 rotation；
- 支持 pending 和 disk Record 的统一读取；
- 扫描崩溃后最大完整物理前缀；
- 出现不确定物理状态后 fail closed。

### 3.2 明确不负责

- 不认识 Put、Delete、Commit、Abort、Checkpoint 或 Relocation；
- 不认识 BatchID、RecordID、CommitSeq 或 FrameType；
- 不判断多个 Record 是否构成完整事务；
- 不提供 AppendGroup 或物理事务；
- 不发布 Mapping，不决定 GC；
- 不序列化 Go 对象，不依赖业务 codec；
- 不为上层维护幂等键；
- 不允许多个 Log 实例同时写同一个目录。

上层业务原子性必须由上层协议完成。例如 ridstore 可以使用 Commit Record 中的
count/hash/引用地址判断一批 Put 是否完整。appendlog 只保证每个物理 Record 自身完整。

## 4. 从旧 WAL 继承的思想

`page-server-rs/src/wal/mod.rs` 已包含这一模型的原型：channel 汇聚、单后台 writer、批量
写、Segment rotation、CRC，以及零数据 flush 请求。

v2 继承：

- 所有调用者只向 channel 提交数据；
- 单 writer 决定真实日志顺序；
- 内存 buffer 合并多个调用；
- 一次 write 覆盖多个 Record；
- 一次 fsync 覆盖多个同步请求；
- Segment 和恢复属于 appendlog 自身。

v2 修正：

| 旧 WAL 行为 | v2 决定 |
| --- | --- |
| 生产者通过 Atomic 提前取得 sequence | VAddr 只能由单 writer 按实际顺序分配 |
| `BufWriter::flush` 作为持久化确认 | `sync=true` 必须完成真实 fsync/fdatasync |
| 根据典型 payload 大小估算 rotation | 依据精确 envelope 大小规划 |
| `recv_many` 消费已经排队的请求 | 保留自然聚合思想，但使用精确 byte/record 上限，不增加时间窗口 |
| 特殊 `seq=0 + None` 表示 flush | 所有请求都是普通 Record；上层自己编码 marker |
| writer 错误只记录日志并退出 | poison Log，并完成所有等待者 |
| writer 内序列化泛型对象 | 上层编码，v2 只复制 `[]byte` |
| close 只调用 flush | 停止接纳、持久化前缀、等待 writer 退出并关闭文件 |

## 5. VAddr

`VAddr` 同时是稳定定位信息和日志顺序信息。公开 API 不再返回独立 Sequence。

FormatVersion 2 把它编码为固定宽度的逻辑地址，并复用 8-byte 对齐天然为空的最低 3 bit：

```text
63                         32 31                    3 2       0
+----------------------------+-----------------------+---------+
|         SegmentID          |    aligned offset     | sizeTag |
+----------------------------+-----------------------+---------+
```

`sizeTag` 是物理 Record 大小的读取提示：

| tag | 首次读取上限 |
| --- | ---: |
| 0 | 64 B |
| 1 | 128 B |
| 2 | 256 B |
| 3 | 512 B |
| 4 | 1 KiB |
| 5 | 2 KiB |
| 6 | 4 KiB；同时表示更大的 Record |
| 7 | 保留，出现即非法 |

真实 offset 为 `low32 &^ 0b111`。标签根据包含 envelope 和 padding 的 `PhysicalSize` 计算，
不是根据 payload 大小计算。它不缩小约 4 GiB 的 Segment 字节寻址范围，也不改变地址顺序；
对上层而言 VAddr 仍是 opaque value，业务不能自行解释 bit layout。

### 5.1 不变量

- 每次成功 Append 得到唯一 VAddr；
- 后追加 Record 的 VAddr 在日志顺序上严格更大；
- VAddr 指向通用 Record envelope 起点；
- VAddr 的 sizeTag 必须与 Record Header 的 PhysicalSize 相符，否则恢复和随机读取都拒绝；
- Record 永不跨 Segment；
- Segment Header/Footer 和对齐空间不产生用户 VAddr；
- rotation 可以在相邻 Record 地址之间留下物理间隙，但不能逆序；
- 返回后，buffer flush 和 rotation 都不能改变地址；
- 已返回地址不能在同一个 Log incarnation 中复用。

独立 Sequence 只有在它表达 VAddr 无法表达的约束时才应重新引入。当前恢复可以通过 VAddr、
Record 长度、Segment 链和 CRC 发现空洞、重叠与损坏，因此 v2 暂不维护第二套顺序编号。

## 6. 三个内部水位

虽然公开 API 简化了，内部仍需要三个物理水位：

```text
durablePos <= writtenPos <= reservedPos
```

- `reservedPos`：地址已分配，payload 已归 v2 所有；
- `writtenPos`：完整 Record 已执行 write；
- `durablePos`：覆盖该前缀的 fsync 已成功；
- 水位是 Record 边界之间的位置，不是某条 Record 的 VAddr；
- 水位只能单调前进，不能跨过半条 Record。

`Append(sync=false)` 等待 Record 到达 reservedPos；`Append(sync=true)` 等待 Record 尾部到达
durablePos。Status 可以暴露三个水位用于诊断，但上层协议不需要操纵它们。

## 7. Writer 模型

只有 writer goroutine 可以修改：

- VAddr 分配位置；
- active Segment；
- pending Record 顺序；
- 三个水位；
- rotation 状态；
- running/poisoned/closing/closed 状态。

生产者不会通过 Atomic 提前分配 VAddr。channel 中被 writer 接纳的顺序就是日志顺序。

### 7.1 请求生命周期

```text
caller
  -> 取得 queue byte budget
  -> 临时借用 payload 并送入有界 channel
writer
  -> 校验精确 Record 大小并在返回前复制 payload
  -> 分配 VAddr
  -> stage 到 pending buffer
  -> sync=false: 返回 VAddr
  -> 持续 drain 当前已经排队的 request
  -> channel 暂时为空或达到物理边界
  -> 合并编码并 write
  -> 若本批次曾出现 sync=true，执行一次 fsync
  -> sync=true: 返回 VAddr
```

这使以下流程只需要一次 write 和一次 fsync：

```text
Append(Put, false)
  -> 返回 putAddr
Append(Commit{putAddr}, true)
  -> writer 合并 Put + Commit
  -> write
  -> fsync
```

### 7.2 channel drain 决定批次边界

writer 阻塞读取至少一个 request，完成 reservation，然后非阻塞地持续 drain 当前 channel。
物理批次只在以下边界结束：

- channel 当前暂时为空；
- pending bytes 达到 `MaxBufferBytes`；
- pending records 达到 `MaxBufferRecords`；
- active Segment 空间不足，需要 rotation；
- Close；
- 故障测试显式切断批次。

`sync=true` 不是批次边界，不能让 writer 停止 drain。writer 对本批次中的 flag 只执行：

```go
needSync = needSync || request.sync
```

即使当前 channel 中每个 request 都是 `sync=true`，writer 仍然继续读取，直到 channel 暂时
为空或者达到物理容量边界，然后才统一 write 和 fsync。

### 7.3 channel 为空时的行为

一次 drain 结束后：

- `needSync=true`：write 全部 pending Record，执行一次 fsync，完成所有同步等待者；
- `needSync=false` 且 buffer 未满：不 write，保留 pending，重新阻塞等待下一个 request；
- buffer 已满：write；只有本批次 `needSync=true` 才 fsync；
- rotation：先完成旧 Segment 的物理收尾，再继续处理尚未 reservation 的 request；
- Close：write 并 fsync 已接纳的完整前缀。

低流量 `sync=false` Record 可以一直留在有界 pending buffer 中。这符合它只要求稳定地址和
进程内可读、不要求持久化的契约。定时 write 或 sync 都不能解决新的正确性问题，因此 v2
不设置 `MaxWriteDelay` 或 `MaxSyncDelay`。

### 7.4 自然 group commit

请求在 writer 执行 write/fsync 期间自然积累到 channel；writer 完成 I/O 后，下一轮 drain
会自动形成更大的批次。低压力时 channel 很快变空，`sync=true` 无需承受人为等待；高压力时
I/O 本身产生排队和聚合，不需要预测未来是否还会到达请求。

因此相邻请求自然共享系统调用：

```text
append(false), append(true), append(false), append(true)
                         |
                         v
              one or few writes + one fsync
```

这不是对每个批次严格承诺一次 write；Segment 边界和合法超大 Record 可以产生额外 write。
目标是让系统调用数量接近物理边界和 durability cut 的理论下限。

## 8. Context、背压和所有权

### 8.1 Context

Context 只控制请求被接纳之前的等待：

- channel admission 前取消：不分配 VAddr；若 payload 已复制则立即释放其 byte budget；
- request 已进入有序队列：v2 必须继续处理并返回确定结果；
- 已接纳后不返回结果不明的 `context.Canceled`；
- 第一版不引入异步 Handle、idempotency key 或 outcome-unknown 协议。

换句话说，Append 一旦被接纳，就不能撤销。上层取消等待不能在物理日志中制造无法解释的
地址空洞。

### 8.2 背压

必须同时限制：

- queue request 数；
- queue payload bytes；
- pending bytes/records；
- 单 payload 大小。

byte budget 在复制前取得，完成、拒绝或关闭时归还。只限制 channel item 数无法抵御少量超大
payload 耗尽内存。

### 8.3 payload 所有权

- 调用期间 v2 临时借用调用者 payload；writer 在返回前把它复制进自身批次 buffer，因此调用者
  不得在 Append 返回前并发修改该 slice；
- writer 把 Record envelope 和 payload 直接编码进可复用的连续批次 buffer，不再先生成每条
  encoded Record、随后复制成整批；
- 普通批次 buffer 的容量严格受 `MaxBufferBytes` 约束，超过该上限的合法单 Record 使用临时
  大 buffer，并在 write 后释放，避免一次大请求永久抬高常驻内存；
- Append 返回后调用者可以立即修改或复用原 slice；
- pending Read 返回新副本；
- writer 和 reader 不能把可变内部 buffer 暴露给上层。

## 9. Read 与 Scan

### 9.1 Read

Read 不解释 payload：

- VAddr 命中 pending index：从内存读取；
- VAddr 已 written：从对应 Segment 读取；
- 两条路径都校验地址和 Record envelope；
- disk 路径校验 CRC；
- disk 路径先按 VAddr sizeTag 读取 64 B 到 4 KiB；小 Record 一次 `ReadAt` 完成，大 Record
  解析 Header 后只读取剩余部分，不重复读取前 4 KiB；
- 返回 payload 副本；
- 指向 Record 中间、未知 Segment 或损坏数据均返回确定错误。

因此上层可以在 `Append(sync=false)` 返回后立即通过 VAddr 读取，而不需要强迫 buffer write。

### 9.2 Scan

Scan 按日志顺序返回 `(VAddr, payload)`，不返回业务类型或 Sequence。它用于恢复和离线检查。

Open 只保留每个 Segment 的恢复摘要，不保存扫描 payload。运行期 Scan 通过 writer 捕获
written position、最后一个 VAddr 和 pending 副本；磁盘部分流式扫描到 written position，随后
按地址输出 pending 副本。它不构造全日志 map，不追赶快照后的新追加，额外内存上限为单条
最大 Record 加 pending buffer。

## 10. 通用磁盘格式

v2 使用独立格式，不复用 ridstore Format v1。当前物理格式版本为 2；版本 2 引入 VAddr
sizeTag，旧的无标签版本会被显式拒绝。所有多字节整数使用 Little Endian；第一版校验
算法固定为 CRC32C。当前原型采用 64-byte Header、32-byte Record Envelope、64-byte Footer
和 8-byte Record 对齐。编码器已有固定 golden vectors，三个 decoder 也有独立 fuzz 入口；最终
format freeze 前仍允许通过显式版本变更调整布局。

```text
Segment
  Fixed Header
  Record Envelope + opaque payload + padding
  Record Envelope + opaque payload + padding
  ...
  Fixed Footer (sealed Segment only)
```

### 10.1 Segment Header

至少包含：

- Magic 和 FormatVersion；
- LogID/incarnation；
- SegmentID；
- SegmentCapacity；
- PreviousSegmentID 和前一 Segment 尾部信息；
- Header CRC。

新 Segment 中任何 `sync=true` Record 被确认前，Header 和必要的目录项必须持久化。

### 10.2 Record Envelope

至少包含：

- Magic 和 Version；
- HeaderSize；
- PhysicalSize；
- PayloadSize；
- VAddr；
- Payload CRC；
- Header CRC。

envelope 不含业务类型。`PhysicalSize` 包含 padding，使扫描器能够精确找到下一条 Record；CRC
不覆盖未定义 padding。

### 10.3 Segment Footer

sealed Segment 的 Footer 至少包含：

- SegmentID；
- DataEnd；
- First/LastVAddr；
- RecordCount；
- Footer CRC。

Footer 不属于用户 Record，也不获得 VAddr。active Segment 没有 Footer，恢复时扫描完整 Record
前缀确定尾部。

### 10.4 精确容量

- reservation 前计算 envelope、payload 和 padding 的精确总长度；
- 单条 Record 必须能放进空 Segment 数据区；
- 合法大 Record 可以超过普通聚合预算，但不能超过 Segment HardLimit；
- 不使用平均或典型 payload 大小估算 rotation；
- Record 永不跨 Segment。

## 11. Segment rotation

下一条 Record 放不下时：

1. write 旧 Segment 的 pending Record；
2. 按需要 fsync 尚未 durable 的前缀；
3. 写并持久化 Footer；
4. 关闭旧 Segment；
5. 以 `.creating` 名称创建新 Segment并写入、fsync Header；
6. rename 为 `.active`，随后 fsync 目录发布；
7. 为当前 Record 分配新 Segment 中的 VAddr。

`.creating` 从不接收用户 Record，也不返回 VAddr。Open 持有目录独占锁后可以安全删除遗留的
合法 `.creating` 文件并重新创建；`.active` 则表示 Header 发布完成，不能按临时文件处理。

rotation 可能把此前 `sync=false` 数据一并持久化，这是允许的：`sync=false` 表示调用者不要求
持久化确认，不表示禁止底层持久化。

第一版不在核心 Log API 中提供删除 Segment。保留策略由上层 Checkpoint/GC 决定，未来只能
通过明确的维护接口删除指定、已关闭且没有 reader pin 的 Segment。

## 12. 恢复

Open 按 SegmentID 顺序验证：

- LogID/incarnation 一致；
- Segment 链连续；
- Header 合法；
- Record 中保存的 VAddr 与实际位置相符且严格递增；
- PhysicalSize 正确，Record 不重叠、不越界；
- sealed Segment Footer 与实际内容一致；
- 除最后 active Segment 外，不允许缺 Footer或截断；
- 只有最后 active Segment 的 torn tail 可以截断到最大完整 Record 边界；
- 中间损坏、CRC 错误、地址倒退和 Segment 断链一律拒绝 Open，不能跳过继续。

恢复只报告完整的物理 `(VAddr, payload)`。它不会判断 Commit、Checkpoint 或未完成事务。上层
自行解释 replay。

恢复可以包含崩溃前已经 write、但对应 `sync=true` 尚未成功返回的额外 Record。上层协议必须
通过 Commit/Seal 等规则容忍这些记录，不能把“扫描可见”直接等同于“业务已提交”。

## 13. 故障状态机

```text
Open -> Running -> Closing -> Closed
             |
             +------> Poisoned -> Closed
```

- 参数错误只拒绝当前请求，不 poison；
- VAddr 分配前发现 payload 不合法，不消耗地址；
- VAddr 已返回后发生 write、short-write、fsync 或 rotation 不确定性，必须 poison；
- poisoned 后拒绝新请求，并以同一根因完成全部等待者；
- 当前进程不得覆盖、移动或复用不确定尾部；
- 必须 Close 后重新 Open，通过严格扫描决定恢复前缀；
- writer panic 必须转换为 poisoned，不能让调用者永久等待；
- panic 后 writer 继续拒绝已排队请求，直到收到 Close，保证所有调用者得到确定结果；
- Close 幂等，停止接纳新请求并等待 writer 和文件句柄退出。

## 14. 并发模型

- 多个 goroutine 可以并发 Append；
- writer 是 mutable log state 的唯一所有者；
- Read 可以并发，通过 immutable pending entry 和 Segment reader pin 实现；
- Status 一次发布三个水位的一致快照，不能分别读取 Atomic 后拼接；
- Close 开始后不再接纳新 Append；
- Open 必须持有目录独占文件锁，禁止双 writer。

## 15. 可观测性

Status 和 metrics 至少提供：

- queue requests/bytes；
- pending records/bytes；
- reserved/written/durable lag；
- cycle records/bytes；
- write calls/bytes/duration；
- fsync calls/duration；
- queue、reservation、durable wait；
- rotation count/duration；
- poisoned 状态和根因；
- recovery scanned bytes/records、tail truncation bytes。

核心压力关系是：

```text
queue wait / (write time + fsync time)
```

结合每 cycle 的 Record、write 和 fsync 数量，判断瓶颈来自排队、复制、系统调用还是设备。

## 16. 独立开发结构

v2 只允许依赖 Go 标准库和本目录代码：

```text
internal/appendlog/v2/
  DESIGN.md
  log.go              API、Open、Close、Status
  writer.go           queue、VAddr reservation、cycle、completion
  buffer.go           pending payload 和地址索引
  format.go           通用 Header/Record/Footer
  segment.go          精确 append/read/sync
  rotation.go         seal/create/directory durability
  recovery.go         严格扫描和 active tail repair
  metrics.go          状态快照
  *_test.go
```

禁止依赖：

```text
internal/base
internal/batch
internal/commit
internal/format
internal/mapping
internal/recovery
```

测试使用独立临时目录、物理边界 fault hook 和内部 File backend，不调用当前 ridstore Runtime。
生产 backend 直接代理 `os.File`；测试 backend 可以从真实文件调用返回 short-write、partial-write、
sync、rename 和目录同步错误，因此既保留真实文件状态，也能覆盖 syscall 失败后的恢复路径。该
注入接口保持在包内，不扩张公开 Append API。v2 的正确性必须来自自身契约，而不是现有系统替
它补足语义。

## 17. 验证计划

### 17.1 模型测试

- 多生产者随机 Append，验证 VAddr 唯一、递增且无重叠；
- 每次状态变化验证 `durable <= written <= reserved`；
- `sync=false` 不发生不必要 fsync；
- 多个 `sync=true` 请求共享 fsync；
- 上层普通 marker 的 VAddr 精确界定持久化前缀；
- 随机 rotation 后地址稳定，Record 不跨 Segment；
- pending 和 disk Read 返回一致 payload；
- Scan 的顺序和冻结 end position 正确。

### 17.2 故障矩阵

在以下边界注入 error、short write 和进程崩溃：

- Segment Header 写入前后；
- Record envelope/payload/padding 中间；
- write 完成但 fsync 之前；
- fsync 返回错误；
- Footer 写入中间；
- Footer durable 后、新 Segment 创建之前；
- 新 Segment Header 或目录 fsync 中间；
- `sync=true` 和 Close 等待期间。

必须证明：成功返回的同步前缀不丢失；最多恢复额外未确认的完整 Record；VAddr 不会错误复用；
中间损坏不能被当作合法尾部跳过。

测试分为两层：in-process fault hook 验证错误传播、poison 和等待者完成；subprocess harness 在
Header 发布、append/sync、rotation 和 tail repair 边界直接 `os.Exit`，再由新进程 Open 并继续
追加。后者证明进程崩溃留下的可见文件状态可以恢复，但不模拟断电后设备缓存或内核 page cache
丢失，因此不能单独证明 power-loss durability。真实断电语义仍需在可控虚拟机、块设备故障注入
或硬件测试环境验证，并与目标文件系统的 fsync/rename/目录 fsync 契约对应。

### 17.3 性能测试

- 单 Record/Request 的高并发同步 workload；
- Put `sync=false` 后紧接 Commit `sync=true`；
- payload 从 0、128 B 到接近 Segment HardLimit；
- 对比逐条 write/fsync、批 write/逐条 fsync、批 write/批 fsync；
- 验证性能提升来自 write/fsync 数量下降，而不是弱化持久性。

## 18. 未来接入 ridstore 的唯一边界

现在不实现连接，未来只允许通过以下关系接入：

```text
DataLog.Encode(...) -> []byte
AppendLogV2.Append(data, sync) -> VAddr
DataLog.Decode([]byte)
```

接入前必须单独 Review：

1. ridstore DataLog payload v2 格式；
2. Commit Record 如何使用 VAddr、count 和 hash 表达完整性；
3. Checkpoint marker 如何引用 Mapping root 和 replay cut；
4. 当前 VAddr 与 v2 VAddr 是否一致；
5. Format v1 是放弃、离线迁移还是双读；
6. 现有 appendlog 的删除范围和切换策略。

这些问题冻结前，v2 不 import 业务类型，也不进入 ridstore 构造、恢复或测试主路径。

## 19. Review 前仍需冻结的决定

1. 是否冻结 32-bit SegmentID + 8-byte aligned Offset + 3-bit sizeTag 的 VAddr 布局及耗尽行为；
2. 是否把已有 golden vectors 对应的 64/32/64-byte Header/Record/Footer 固定为 FormatVersion 2；
3. `fsync` 或 `fdatasync`，以及目录 fsync 的平台契约；
4. channel、buffer byte/record 上限的默认值；
5. active tail 是自动截断，还是先返回 repair plan；
6. LogID/incarnation 的生成和目录重建规则；
7. 是否预分配 Segment，以及零填充区的恢复识别；
8. CRC32C 是否足够，是否增加仅供离线校验的强 hash。

完成这些 Review 后再冻结 API 和格式，随后开始独立实现。

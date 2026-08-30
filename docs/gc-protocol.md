# ridstore v2 GC 与空间回收协议

状态：M5 implementation contract

## 1. 目标与边界

Data GC 回收：

- Abort Batch 的 PutRecord；
- 同一 Batch 中被最终操作覆盖的 PutRecord；
- 被后续 Put 覆盖的旧 Record；
- Delete 后不再映射的 Record；
- GC CAS 失败产生的副本；
- Mapping Checkpoint 已覆盖的旧 Commit/系统 Record。

GC 不理解 Page、Blob、TTL、Stream 或业务生命周期。唯一逻辑存活依据是当前 Mapping。

Mapping 文件的不可达 Node 回收由 Mapping GC 负责，见 `mapping-design.md`。

## 2. Segment 生命周期

```text
Active -> Sealed -> Cleaning -> Retired -> Trash -> Deleted
```

- Active：RecordLog writer 正在写；
- Sealed：Footer durable，可读但不可写；
- Cleaning：GC 正在扫描/复制；
- Retired：没有 Mapping 应继续指向它，等待 reader/open-batch refs；
- Trash：已从正式 Manifest 和读路径移除；
- Deleted：文件删除且目录 fsync 完成。

状态变化由 Maintenance Journal 和 Manifest 共同证明，不能只依赖内存 flag 或文件扩展名。

Data GC、Mapping GC、Mapping Checkpoint 和 Segment rotation 对 Manifest 的修改由同一个安装串行器协调。
Data/Mapping GC 和 rotation 使用 generation CAS；Mapping Checkpoint 携带完整 base Manifest，只有并发变化是
可证明连续的纯 Data rotation 时才 rebase 到最新 Data 文件集合，其余变化一律冲突。任何操作都不能覆盖
其他操作刚安装的文件集合或 Root。

## 3. GC 候选条件

Segment 同时满足以下条件才可进入 Cleaning：

- 状态为 Sealed；
- Segment end position 不晚于当前 durable Mapping Checkpoint 的安全扫描边界；
- 没有 Open Batch 引用其中的 PutRecord；
- 没有其他 GC/Backup/Scrub 持有 maintenance pin；
- 不包含恢复仍需读取且未被 Checkpoint 覆盖的 Commit Descriptor；
- 不是当前故障诊断保留文件；
- 预估 dead ratio 达到阈值或磁盘压力要求强制清理。

候选估算使用 `segment-stats-design.md` 定义的 `LiveUpperBytes`：

```text
reclaimableLowerBound = max(0, reclaimablePhysicalBytes - LiveUpperBytes)
```

上界可能因 cut 后多次覆盖而高估 live，只会漏选一时值得清理的 Segment，不会把 live 空间误报为可回收。即使 `LiveUpperBytes == 0`，删除前仍必须完成本协议的精确 Mapping 扫描、Relocation 后复查、Checkpoint、Pin、Manifest 和 Trash 全部门禁；SegmentStats 永不授权删除。

v2 的 `CompactNextSegment` 在选择前主动创建一次 Checkpoint，并且只考虑 `segment.end < ReplayStart`
的 sealed Segment。这样的 Segment 不再接受新 append；同一时刻又只有一个 maintenance 操作，因此 cut
后的用户提交只能减少、不能增加指向该 source 的 Mapping。于是该 Checkpoint 的 exact live bytes 对选择
时刻自然成为 live upper bound，不需要在热路径维护第二套全局计数。

选择器单遍扫描 Manifest，额外空间只与被 open Batch 引用的 Segment 集合有关，并且一次最多返回一个
候选。排序依次采用 reclaimable bytes 降序、live bytes 升序、SegmentID 升序。调用方可以同时设置最小
可回收字节数和最小回收比例（basis points）；两个非零门槛都必须满足。被 open Batch 最终 Put 引用、
或尚未严格位于 ReplayStart 之前的 Segment 不进入候选集。等于 ReplayStart 的边界 Segment 仍视为
unknown：它可能在 Stats 构建后、Manifest 安装前才完成 rotation，必须由下一次 Checkpoint 补齐。

## 4. 存活判定

扫描 PutRecord，得到 `(ID, OldVAddr)`：

```text
current = Mapping.Lookup(ID)
live iff current == OldVAddr
```

其他 Record：

- Commit/Abort/IDReserve 在 Mapping Checkpoint 覆盖后不作为用户 live data 复制；
- 未覆盖的系统 Record 所在 Segment不能成为候选；
- Segment Header/Footer 不复制；
- 无法解析或 CRC 错误立即停止 GC 并标记 corruption。

第一次扫描得到的 live 只是候选；复制后发布时仍必须 CAS，因为用户 Put 可能并发改变 Mapping。

## 5. Relocation Batch

GC 为候选 live Record 创建 Relocation Batch：

```text
copy payload to new PutRecord
record {ID, ExpectedOldVAddr, NewVAddr}
```

Relocation Descriptor 使用共享 durable allocator 发放的内部 BatchID；copied PutRecord 必须保留旧
PutRecord Header 的 OriginBatchID 并复制用户 Value。OriginBatchID 只用于来源追踪，不是业务版本；
GC 不能改变逻辑内容。

Relocation 可以分多个有限 Batch，避免单次占用过多内存或阻塞 group commit。

同一 CommitGroup 内，Coordinator 稳定地让 UserCommit 先于 Relocation；两类请求内部仍保持 FIFO。该语义优先级只减少同组中由 VAddr 变化引起的用户条件伪冲突，不跨 group，也不替代 Relocation copy I/O 的限速和外部时段调度。

Relocation 不增加顶层 Record 类型。它是 `CommitGroupRecord` 中的 Descriptor kind，与 UserCommit 进入
同一个 Coordinator queue，共享分组、RecordLog append/fsync、CommitSeq 和 Mapping publish 顺序。
Coordinator 在 durable append 前的 virtual Mapping 顺序中对每条 mutation 解析 CAS：

```text
if Mapping[ID] == ExpectedOldVAddr:
    Mapping[ID] = NewVAddr
else:
    skip
```

Coordinator 将 apply/skip plan 传给 fsync 后的 Mapping Publisher，Publisher 在短内存临界区执行该计划，
不在 Mapping 锁内做冷 Lookup。Recovery 按 Descriptor/CommitSeq 重算相同 CAS。Relocation CAS 成功和
失败数量必须返回；失败副本不重试覆盖，它是新垃圾。

## 6. 持久化顺序

单个 Relocation Batch：

```text
append copied PutRecords
-> append one Relocation Descriptor in CommitGroupRecord
-> fsync
-> publish resolved CAS plan to Mapping
```

因此 Mapping 永不指向尚未 durable 的复制数据。

恢复重放 Relocation 时执行相同 CAS。用户 Commit 和 Relocation 共享全局 CommitSeq，顺序确定：

- Relocation 先、用户 Put 后：最终用户 Put 覆盖；
- 用户 Put 先、过期 Relocation 后：ExpectedOldVAddr 不匹配，CAS 失败。

## 7. 完成一个源 Segment 的搬迁

GC 扫描完源 Segment 并处理所有候选 Record 后，不能立即删除源文件。

必须完成：

```text
1. 所有 Relocation Batch 得到确定结果
2. 再扫描/校验 Mapping，不再有 VAddr 指向源 Segment
3. 创建覆盖所有 Relocation CommitSeq 的 Mapping Checkpoint
4. 确认新 Mapping Root/Manifest durable
5. 将源 Segment 标记 Retired
6. 等待所有 pin/ref 清零
7. 从正式 Manifest 移除并移动到 trash
8. fsync data/trash/root directory
9. 删除 trash 文件
10. fsync trash directory
```

步骤 2 若发现 Mapping 仍指向源 Segment，说明并发变化或遗漏，源 Segment 保留并重新调度，不允许部分删除。

## 8. Reader Pin

Get 的安全读取协议：

```text
loop:
  vaddr = Mapping.Lookup(ID)
  if vaddr == 0: NotFound
  segment = SegmentRegistry.Acquire(vaddr.segmentID)
  if acquire failed because Retired: retry Mapping
  verify Mapping.Lookup(ID) still equals vaddr
  read and verify Record
  Release(segment)
```

Acquire 与 Retire 在同一个 Segment Registry 锁/原子状态机中协调，消除：

```text
Reader 取得 VAddr
GC 删除文件
Reader 再 Acquire
```

的竞态。

第一版 Get 在复制 payload 完成后立即 Release；返回 slice 不引用 mmap。

## 9. Open Batch Ref

PutRecord 在 Commit 前可能位于后来 Sealed 的 Segment。Batch 为每个包含其最终或历史 PutRecord 的 Segment持有 open-batch ref，直到 Commit/Abort/Failed 清理。

GC 不选择 open-batch ref > 0 的 Segment。Batch 只需按 Segment 去重持有 ref，避免每个 Record 一个引用。

即使 Commit Descriptor 自包含最终 VAddr，也不能在 Commit 前删除 payload Segment。

## 10. Maintenance Marker

Relocation、Checkpoint 和 retirement proof 都发生在不可逆操作之前；失败只会留下由 Mapping 决定
是否可达的 Record，不需要为每个内存步骤持久化 Phase。唯一不可逆边界是 Catalog 移除 source。

v2 因此使用一个固定格式的全局 `MAINTENANCE.v2` marker：

```text
proof complete
-> install marker and fsync journal directory
-> Registry retire gate / wait readers
-> Catalog remove source
-> Registry detach / close
-> rename to trash / fsync directories / delete
-> remove marker / fsync journal directory
```

marker 包含 StoreUUID、RecordLogID、BaseGeneration、CoveredCommitSeq、ReplayStart 和完整 source summary。
Open 在 RecordLog 打开文件前恢复：

| durable evidence | Open 行为 |
|---|---|
| source 仍在 BaseGeneration Catalog，字段与 marker 一致 | Catalog remove 未发生；删除 marker，保留 source |
| source 仍以相同 summary 位于 generation >= BaseGeneration 的 Catalog | retire 尚未发布；删除 marker |
| source 不在 generation > BaseGeneration 的 Catalog | Catalog remove 已 durable；幂等完成 canonical/trash 清理，再删除 marker |
| identity、summary 或 checkpoint 单调边界不匹配 | corruption，拒绝打开 |

Catalog 是方向判定的唯一状态源；marker 只标识本次允许清理的确定文件，避免把未知 orphan 当作 GC
产物自动删除。普通 Checkpoint 不写 marker，Data Segment rotation 继续使用自己的短物理 journal。

## 11. Manifest 与文件操作顺序

正式删除使用可恢复的 rename-to-trash：

```text
write new Manifest without source
-> fsync Manifest/CURRENT/root dirs
-> rename data/source -> trash/source.retired
-> fsync data dir
-> fsync trash dir
-> unlink trash file
-> fsync trash dir
-> remove maintenance marker
-> fsync journal dir
```

如果 Manifest 已移除但 rename 前崩溃，Journal 指示继续移动；如果文件已在 trash，Open 不把它重新加入正式 Segment。

在 POSIX unlink 后仍有 fd 可读不能替代 Reader Pin 协议；ridstore 需要跨平台可解释的显式生命周期。

## 12. 并发 GC

第一版：

- 同时只清理一个 Data Segment；
- GC copy 通过唯一 RecordLog writer；
- 用户 Commit 优先级高于 GC copy；
- 每个 Relocation Batch 有字节/Mutation 上限；
- 磁盘压力过高时允许 backpressure 新写，但不破坏已开始 Commit；
- Close 阻止新的 GC，并等待当前操作到 marker/Catalog 可恢复边界。

多 Segment 并发 GC 只有在单 Segment协议和资源控制稳定后考虑。

## 13. 候选选择与成本模型

选择策略不属于正确性，但默认按收益估计：

```text
score = reclaimableBytes / (copyBytes + fixedCost)
```

考虑：

- `LiveUpperBytes` 推导的 live ratio 上界和 reclaimable bytes 下界；
- Segment age；
- 预计 copy bytes；
- 当前前台 fsync/带宽压力；
- 是否能推进最旧恢复边界；
- 是否包含长时间 Open Batch ref。

不能把时间、TTL 或业务类型写入 GC 语义。策略只观察物理统计和 Mapping liveness。

## 14. 空间与资源保护

必须配置：

- GC 启动磁盘水位；
- 新写停止水位；
- 最小保留空闲空间；
- 单次 Relocation Batch 上限；
- Data GC 临时空间保留 `GCMinFreeBytes`；
- GC 带宽/并发限制；
- trash 最大停留时间告警。

内核以 `WriteStopFreeBytes` 承接“新写停止水位”：Put 在 payload append 前执行缓存式空间
admission；已有 Batch 的 Commit/Abort、读取和普通 Checkpoint 保持可运行。Data GC 与前台 Put
共用同一个进程内 reservation 账本，但使用独立的 `GCMinFreeBytes` 水位执行 copy/checkpoint
两阶段准入。该检查不能替代所有 write/fsync 的 ENOSPC 传播，也不能替代部署层独立文件系统、
容量告警和外部写入隔离。

当可用空间不足以同时保存 live copy 和旧 Segment 时，不能开始无法完成的 GC。返回明确 `ENOSPC`/资源错误并保持旧数据可读。

第一版采用两段保守 admission：maintenance marker 前覆盖 exact live copy、Relocation Descriptor、两个
rotation Segment 和 `GCMinFreeBytes`；Relocation 完成后，Checkpoint barrier 冻结实际 Delta layers，再按
实际 entry 数覆盖每个 entry 最坏八层 Dense Mapping COW、一个 rotation Segment 和 `GCMinFreeBytes`。
不能只按源 Segment 的 live Record 数估计最终 Mapping，因为 copy 期间允许前台 Commit，最终 cut 还可能
包含这些用户 Delta。第二段返回 `ErrInsufficientSpace` 时 Relocation 副本可作为垃圾保留，source 和旧
checkpoint 仍可恢复。文件系统可用空间不是可锁定资源，因此 admission 通过后仍必须处理真实
`ENOSPC`；Catalog remove 之前可以撤销 marker，之后则 fail closed 并由 Open 完成既定物理清理。

## 15. Scrub

Scrub 与 GC 分离：

- 顺序验证 Segment Header/Footer/Record CRC；
- 验证 Mapping 指向正确 ID 的 PutRecord；
- 验证 Manifest/Mapping Root 文件引用；
- 报告 orphan/dead bytes；
- 默认只读，不自动修复；
- 修复必须走独立、可审计协议。

GC 遇到 corruption 不能通过复制“看起来可读”的部分掩盖错误。

## 16. 必测竞态和崩溃点

- Reader Lookup 后、Acquire 前 Retire；
- Acquire 后 GC 等待 pin；
- Copy 后、Relocation Descriptor durable 前崩溃；
- Relocation fsync 后、CAS 前崩溃；
- CAS 一半后崩溃；
- 用户 Put 与 Relocation CAS 两种顺序；
- Relocation 前后 Value 与 OriginBatchID 完全相同，VAddr observation token 允许改变；
- Mapping Checkpoint 构建/安装各阶段崩溃；
- Manifest 移除前后崩溃；
- rename-to-trash 前后崩溃；
- unlink/dir fsync 前后崩溃；
- Open Batch 跨 Rotation 长时间不结束；
- ENOSPC、fsync error、permission error；
- Close 与 GC 的 marker/Catalog/remove 各 durable boundary 并发。

## 17. 完成定义

GC 只有满足以下证据才算完成：

- 源 Segment 无 Mapping 引用；
- Relocation 已被 durable Mapping Checkpoint 覆盖；
- Reader/Open Batch/Maintenance pin 为 0；
- Manifest 不引用源文件；
- 文件从正式目录消失且目录已 fsync；
- Journal 已完成或清理；
- 重启后状态一致；
- 前台读写在竞态测试中没有错误值；
- 长时间运行 live/dead/trash/FD/RSS 最终收敛。

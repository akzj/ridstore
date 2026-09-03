# ridstore v2 API 与一致性契约

状态：Implemented v2 contract

## 1. 边界

v2 仍是嵌入式、单机、单目录独占的 Stable-ID Record Store：

```text
uint64 ID -> variable-length bytes
```

它提供原子 Batch、durable commit、单 Record 读取和稳定 ID，不提供业务 Revision、MVCC、自动读集、
Serializable 隔离或跨多次 Get 的 Snapshot。根包公开 API 直接使用 v2 Engine；代码中不存在
Revision adapter、旧格式兼容层或双运行时。

## 2. 内部唯一事实

v2 Mapping 的完整可见状态是：

```text
ID -> VAddr
ID -> NotFound
```

`VAddr` 是当前 committed Record 的物理地址，也是 ridstore 内部唯一一致性 token。Mapping 不保存第二个
Revision 字段；条件解析不读取 Data Record，也不把 PutRecord 的 OriginBatchID 解释为版本。

内部 Engine 当前直接使用：

```go
type Record struct {
    Value []byte
    Addr  recordlog.VAddr
}

CompareAndPut(id, expectedAddr, value)
CompareAndDelete(id, expectedAddr)
ExpectAddress(id, expectedAddr)
ExpectAbsent(id)
```

零地址只在条件中表示“必须不存在”，不是合法 Record 地址。

## 3. 条件提交

Coordinator 在全局提交顺序中的 virtual Mapping 上解析整个 group：

```text
for batch in queue order:
    if every Mapping[id] equals ExpectedVAddr or expected absence:
        admit all final mutations atomically
    else:
        reject the entire batch with ErrConflict
```

验证与组内前序 mutation 使用同一个 `ID -> VAddr` 模型。通过验证的 Descriptor 不保存条件；Recovery
只重放 durable CommitGroup，不重新判断历史条件。Blind Put/Delete 不读取旧地址，按 commit order
last-writer-wins。

## 4. 读取与重验证

单次 Get 使用：

```text
addr = Mapping.Lookup(id)
payload = RecordLog.Read(addr)
current = Mapping.Lookup(id)
if current != addr: retry
decode PutRecord and verify RecordID
return copied value and addr
```

第二次 Lookup 防止并发用户 Commit 或 GC relocation 让读取的物理 Record 在返回前失去当前性。RecordLog
负责地址与物理 CRC；Record Protocol 负责类型、RecordID 和 payload 边界；Mapping 决定可见性。

## 5. 公共 observation token

最终公共 API 不应暴露可拆解的 SegmentID/offset。若调用者需要乐观条件，公共层可以把当前 VAddr
封装成不可构造、不可排序、只允许原样回传的 `VersionToken`（最终名字在公开 API 切换时决定）：

```go
type Record struct {
    Value []byte
    Token VersionToken
}

CompareAndPut(id, token, value)
CompareAndDelete(id, token)
ExpectToken(id, token)
ExpectAbsent(id)
```

该 token 只是一次物理 Mapping 观察的封装，不是 LogicalRevision。实现不得为它增加独立持久化字段、
resolver 或 Header read。无效、跨 Store 或伪造 token 必须被拒绝；调用者不能依赖其数值、顺序或编码。

## 6. GC relocation 的影响

Relocation 复制相同的 RecordID、Value 和 OriginBatchID，但生成新的 VAddr，并以
`Mapping[id] == ExpectedOldVAddr` 做 CAS。成功后：

```text
Value: unchanged
OriginBatchID: unchanged
VAddr/token: changed
```

因此一个基于旧 token 的用户条件可能在没有业务值变化时返回 `ErrConflict`。这是安全的伪冲突，调用者
应重读并重试。为了消除此冲突而恢复 LogicalRevision 会重新引入额外状态、Record Header 冷读以及
Mapping/Record 双重一致性，v2 明确不这样做。

## 7. 上层数据结构职责

B-link tree、B+Tree 或 page engine 可以把稳定 Record ID 当作 PageID，Page 内只保存其他稳定 PageID，
不保存 VAddr。它们有两种并发策略：

- 在进程内由自己的 latch/lock protocol 串行化结构修改，再使用 ridstore 原子 Blind Batch；
- 使用 ridstore observation token 做乐观 CAS，并接受 GC relocation 导致的重试。

如果上层需要跨 relocation 稳定的 page epoch、generation 或业务版本，字段必须编码在 Page Value 中，
并由上层协议更新和解释。Ridstore 不解析 Value，不能替上层验证该业务字段。

一次 split 仍可把 right page、left page 与 parent page 的写入放进同一 Batch；ridstore 保证这些最终
mutation 一起可见或全不生效，但不替 B-link tree 证明搜索路径、锁顺序或 page epoch 正确。

## 8. OriginBatchID 的有限职责

PutRecord 保留 OriginBatchID，用于：

- Commit 前证明 PutRecord 属于当前用户 Batch；
- Recovery 验证 Descriptor 引用的 Record 身份；
- GC relocation 复制和核对原始内容来源；
- orphan、corruption 与离线审计诊断。

OriginBatchID 不进入 Mapping、不由 Get 返回、不参与用户条件，也不是 MVCC timestamp 或 LogicalRevision。

## 9. ID 发放边界

`Put` 只能接受已经越过当前 allocator 发放点的 ID。`Allocate` 或 `Create` 在返回 ID 前先持久化其预留
区间；同一进程中，尚可能被后续 `Allocate` 返回的 ID 必须在追加 PutRecord 前以 `ErrInvalidID` 拒绝。

崩溃恢复从 durable reserved high 之后继续发放，因此上一个进程预留区间内无法证明是否实际返回给
调用者的 ID 全部保守地视为已消耗。它们可能形成永久空洞，但不会与未来 Allocate 结果碰撞。

## 10. Changed / unchanged

本次改变：

- 删除 v2 Mapping、Transaction、Coordinator、Engine 和 Replay 中的 Revision；
- 条件验证从“Mapping 后再读 Record Header”缩减为一次 Mapping 地址比较；
- 明确 GC relocation 可造成安全的地址条件冲突；
- 把业务版本、page epoch 和锁协议归还上层。

保持不变：

- Stable ID 永不复用；
- Batch 的 durable-before-publish 与全有或全无；
- CommitSeq 是恢复和发布顺序，不是 Record 版本；
- OriginBatchID 的持久化身份验证用途；
- Relocation 的 expected-old-VAddr CAS、Reader Pin 和删除门禁；
- 单次 Get 的 Mapping revalidation。

## 11. Batch Status 保留边界

`Status(BatchID)` 只为不确定提交恢复提供有界查询窗口，不是永久业务历史。`RuntimeConfig.StatusRetention`
同时限制：

- 进程内保留的最近用户 Batch 终态数量；
- Checkpoint cut 之后允许 Replay 构造的用户终态数量；
- 尚未终结的公开 Batch 为未来终态预留的容量。

`StatusRetention` 必须不小于创建时持久化的 `MaxOpenBatches`；Create 在写入目录前验证该关系，Open 在
执行任何可变恢复前验证该关系。

当 `tail terminal count + open batch count` 达到上限时，新的 `Begin` 先同步推进 Checkpoint，再重新
尝试 admission。已被淘汰或被新 Checkpoint 覆盖、且无法再给出确定结果的 Batch 返回
`ErrStatusExpired`；从未发放的未来 BatchID 返回 `ErrNotFound`。

GC relocation 使用共享的 durable BatchID/CommitSeq 顺序，但不是用户提交，不进入公开 Status 保留集。
Replay 仍验证其 CommitSeq、Descriptor 和 mutation，只是不为它生成可查询状态。

Data GC 的运行时资源由 `GCBatchBytes`、`GCBatchMutations`、`GCMinFreeBytes` 与
`GCBytesPerSecond` 约束。它们可以在每次 Open 时调整，但不能超过持久化 Batch 上限，也不能改变
relocation CAS、Checkpoint coverage 或 source retirement proof。

`SetGCBytesPerSecond(rate)` 只修改后续 Data Compact 的复制速率。`rate == 0` 返回
`ErrInvalidConfig`；已经运行的 Compact 使用开始时的速率快照，调用方通过取消该次 Context 停止它。
时间窗、容量水位和调用频率属于运行时 Maintenance Scheduler 策略，不进入 Store 的持久化状态。

## 12. 离线 Verify

根包公开 `Verify(ctx, VerifyConfig)`。它必须在 Store 关闭时取得同一目录独占锁，并按
physical files、checkpoint Mapping、semantic replay、exact join 的顺序验证；成功报告的终态 Stage
固定为 `VerifyStageExact`。`MaxLiveIDs` 和 `MaxReplayStatuses` 是 verifier 自身的显式内存上限，命中时
返回 `ErrVerifyLimit` 或 `ErrStatusCapacity`，不把资源不足误报为数据损坏。Verify 不调用正常 `Open`，
不截断 tail、不完成 Journal、不清理垃圾文件，也不修改 Manifest。

## 13. Mapping 物理重写

根包公开 `CompactMapping(ctx)` 作为显式维护入口。它以
typed Mapping GC worker 提交给 Maintenance Scheduler。显式请求直接声明 Checkpoint 依赖；自动请求先执行
Survey，通过策略门槛后再声明该依赖。随后按 `Copy -> Publish -> Cleanup` 推进各阶段；`heavyIO`、
`mappingWriter` 和 `recoveryProtocol` 由 Scheduler
按阶段独占分配，worker 不自行获取全局维护锁。Checkpoint 具有更高优先级，Copy 期间允许新的 Checkpoint 推进；
PublishCoordinator 通过 generation 校验拒绝过期的重建结果。

Mapping GC 把全部可达 `ID -> VAddr` 流式复制到新 Mapping generation。新 Manifest durable 后，Engine 原子替换
Persistent root 与物理 owner；旧 reader 清零、旧 owner 关闭后才允许 retirement。重建、Manifest fsync 和
reader drain 均不持有 Store 全局数据面锁。该操作不改变 Value、VAddr、
CoveredCommitSeq、ReplayStart、allocator high、Batch Status 或 SegmentStats。

自动维护启用且未设置 `DisableMappingGC` 时，Scheduler 周期性运行 generation-bound Survey；只有可回收字节、
可回收比例和 cooldown 同时达到配置门槛才启动重写。显式入口不受这些自动策略门槛限制。

## 14. 离线 Backup / Restore

Linux 上根包公开 `Backup(ctx, BackupConfig)` 和 `Restore(ctx, RestoreConfig)`。Backup 要求源 Store 已关闭，
并在同一个独占目录 lease 内完成 Exact Verify、Manifest 文件集确定和逐文件复制；artifact 只含当前权威
Manifest、其引用的 RecordLog/Mapping 文件和带 CRC 的 `BACKUP-v2` 元数据。

Restore 只接受严格白名单、size/SHA-256 全匹配且可通过 v2 Exact Verify 的 artifact，并通过同父目录
`RENAME_NOREPLACE` 原子发布到不存在的目标。两者的 verification budget 可显式配置，零值使用 Verify
默认值。Restore 按字节保留 StoreUUID/RecordLogID，因此是灾难恢复而不是 Clone；原目录和任一恢复目录
不得同时作为 writer。非 Linux 平台在无法提供原子 no-replace directory rename 时返回 `ErrUnsupported`。

## 15. 生命周期与关闭

`CloseContext(ctx)` 只负责发起一次 Store shutdown 并等待结果。Store 首先拒绝新操作并取消统一 root
context；Maintenance Scheduler 随后停止接收请求、取消 worker 并关闭自身 `Done` 信号。已进入 durable
队列的 Coordinator/RecordLog 请求必须完成或返回确定错误，不能被直接丢弃。全部前台操作退出后，Store
按依赖顺序关闭 Coordinator、RecordLog、Mapping 和目录 lease，最后关闭 `Store.Done()`。

调用方 context 超时只终止本次等待，后台 shutdown 仍继续；调用方可以再次调用 `CloseContext`，或读取
`Done()` 等待最终完成。生命周期锁只保护 admission 与计数，不跨 I/O、goroutine 等待或资源释放。

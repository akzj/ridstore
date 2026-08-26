# ridstore v2 API 与一致性契约

状态：Development contract v2

## 1. 边界

v2 仍是嵌入式、单机、单目录独占的 Stable-ID Record Store：

```text
uint64 ID -> variable-length bytes
```

它提供原子 Batch、durable commit、单 Record 读取和稳定 ID，不提供业务 Revision、MVCC、自动读集、
Serializable 隔离或跨多次 Get 的 Snapshot。根包公开 API 已在 M6 直接切换到 v2 Engine；旧
[api-contract.md](api-contract.md) 仅是 Format v1 历史记录，代码中不存在 Revision adapter 或双运行时。

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

## 9. Changed / unchanged

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

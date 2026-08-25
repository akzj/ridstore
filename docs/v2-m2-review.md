# ridstore v2 M2 Review

状态：Implemented, pending architecture review

提交：

- `ce629e2 feat: add recordlog segment registry`
- `b7bf06c feat: add recordlog single writer`
- `99b0b78 feat: integrate recordlog catalog lifecycle`
- `1e409e3 test: verify recordlog recovery boundaries`

## 1. 阶段结论

M2 已在 `internal/recordlog` 中生成一条独立的 Format v2 物理主路径：

```text
Append([]byte, sync)
  -> queued-byte admission
  -> one writer
  -> reserve VAddr + copy payload
  -> contiguous write
  -> optional shared fsync
  -> Active Segment / rotation journal / Catalog / Registry
```

新代码不导入旧 `internal/appendlog` 或 `internal/segment`，也没有 fallback、adapter 或双写。当前公开
ridstore `Store` 尚未接入 v2，所以本阶段不会改变 Format v1 用户路径。

## 2. 代码与职责

| 代码 | 唯一职责 |
|---|---|
| `recordlog/fileio.go` | 完整 positional I/O 与目录 fsync 后端 |
| `recordlog/segment.go` | Segment 创建、追加、读取、seal、tail repair |
| `recordlog/registry.go` | fd membership、Reader Pin、Retiring、detach |
| `recordlog/budget.go` | 等待可取消的 queued-byte hard limit |
| `recordlog/writer.go` | 唯一物理顺序、自然 batching、三个水位、poison |
| `recordlog/log.go` | 统一 Append、Read、稳定 Scan、Close |
| `recordlog/rotation_journal.go` | 独立 v2 rotation intent 的原子持久化 |
| `recordlog/open.go` | Catalog 驱动的文件打开和 rotation recovery |
| `recordlog/retire.go` | Catalog-first 的物理退休与删除 |
| `storecatalog/catalog.go` | 直接实现窄 Catalog port |

Catalog port 使用 `recordlog.CatalogSnapshot` 和 `recordlog.SegmentSummary`。这不是旧接口适配层：它是
RecordLog 所依赖的最终端口，`storecatalog.Manager` 直接实现它。

## 3. 已落实的不变量

### 3.1 写入与完成

- writer 是唯一 VAddr 和物理顺序分配者；
- `sync=false` 只在 payload 已复制、地址已保留后返回；
- `sync=true` 等到覆盖其 `End` 的 fsync；
- drain 不以 `sync=true` 为边界，当前已经排队的同步请求共享 write/fsync；
- 没有 MaxWriteDelay 或 MaxSyncDelay；
- `durable <= written <= reserved` 使用 LogPos 表达；
- write、short write、sync、rotation 或 Catalog 不确定错误使当前 Log poisoned；
- terminal error 只设置一次，所有尚未完成的接纳请求都会得到结果。

### 3.2 读取与恢复

- reserved 但未 written 的 Record 从 pending index 读取；
- written Record 通过 Registry Reader Pin 读取；
- size tag 小 Record 一次 ReadAt，大 Record 先读 4096 bytes 再读取余部；
- Scan 固定 written cut 和 pending 副本，不追逐并发 append；
- active 恢复只截断不完整 Header/Record；完整但 CRC、VAddr、padding 错误直接报 corruption；
- sealed 文件长度、Header、Footer 必须与 Catalog summary 精确相同。

### 3.3 Rotation

正常顺序：

```text
flush current group
  -> durable RECORDLOG-ROTATION-v2 journal
  -> write/sync Footer and rename old sealed
  -> create/sync new active
  -> Catalog compare-and-install
  -> Registry publish
  -> remove journal + fsync directory
  -> next user Record receives address
```

恢复只接受两种 Catalog 状态：仍指向 old active，或已经精确安装 journal 描述的 new active。前者完成
seal/create/Catalog install；后者校验文件后清理 journal。其他组合都是 corruption。未被 Catalog 引用
的更大文件不会因为目录扫描而自动进入 live set。

### 3.4 Reader Pin 与删除

```text
BeginRetire (block new pins)
  -> wait existing pins
  -> Catalog remove exact Segment summary
  -> Registry detach
  -> close fd
  -> rename trash + fsync source/destination directories
  -> unlink + fsync trash directory
```

真实 `storecatalog.Manager` 仍要求 checkpoint 对该 Segment 有精确零存活统计；RecordLog 不自行判断
liveness。Catalog remove 失败时撤销 Retiring。Catalog 已成功后发生物理清理失败只留下安全 orphan，
不会让 Manifest 指向已经删除的文件。

## 4. 本次 Review 发现并修正的问题

- terminal error 与提交队列不再共用锁，避免队列满时 submitter、writer 和 poison 形成锁循环；
- rotation 不再要求旧 active 的 Reader Pin 为零，已有 pin 在 Registry publication 后切换到 sealed view；
- active fd 的 ownership 只在 Registry publication 时转交，rotation 中途失败仍可由 Registry Close 回收；
- Scan 使用固定 written 上界，不能把 snapshot 之后追加的 active Record 混入结果；
- active tail 只修剪物理不完整尾部，不把完整 CRC 损坏误判为可恢复 torn write；
- Close 先阻止新 pin，并等待已有 reader 释放后关闭 fd。

## 5. 验证证据

已执行：

```text
go test -count=20 ./internal/recordlog ./internal/storecatalog
go test -race -count=5 ./internal/recordlog ./internal/storecatalog
go test -count=1 ./...
go vet ./...
git diff --check

go test ./internal/recordlog -run='^$' -fuzz='^FuzzDecodeRecord$' -fuzztime=2s
go test ./internal/recordlog -run='^$' -fuzz='^FuzzDecodeSegmentHeader$' -fuzztime=2s
go test ./internal/recordlog -run='^$' -fuzz='^FuzzDecodeSegmentFooter$' -fuzztime=2s
go test ./internal/recordlog -run='^$' -fuzz='^FuzzDecodeRotationJournal$' -fuzztime=2s
```

测试覆盖：

- active incomplete tail 截断与完整 corruption 拒绝；
- sealed Footer/Catalog summary 精确匹配；
- Reader Pin 阻止 retirement，rotation 不破坏已有 pin；
- payload ownership、pending read、跨 Segment Scan 和 reopen；
- 32 个预排队 `sync=true` Append 共用一次 write/fsync；
- write failure poison 与取消发生在 reservation 前；
- rotation 在 journal、old sealed、new active 三个进程退出点恢复；
- RecordLog Catalog port 的 rotation/retirement 安装；
- Record、Segment Header/Footer 和 rotation journal decoder 短时 fuzz。

短时 fuzz 只证明本次运行没有发现错误，不替代长期 fuzz。

## 6. 明确未完成

- v2 Store/Batch/Coordinator 尚未接入 RecordLog；
- 目录独占锁属于顶层 Store 生命周期，M2 没有在 RecordLog 内建立第二个锁所有者；
- Mapping checkpoint、protocol replay、CommitUnknown 查询仍属于 M3/M4；
- Cleaning/open-batch refs、Relocation 和 liveness proof 的产生属于 M5；M2 retirement 只消费 Catalog 已验证的零存活事实；
- orphan/trash 离线报告、完整 syscall fault matrix、benchmark、长时 fuzz 和 soak 尚未完成；
- 旧 Format v1 runtime 仍在仓库中，只有 v2 最小闭环通过后才删除。

因此 M2 的结论是“RecordLog 子系统主路径完整”，不是“ridstore v2 可供生产使用”。

## 7. 进入 M3 前 Review 项

1. `Config` 只包含运行时预算，SegmentSize/MaxPayload 是否应继续只来自 Catalog；
2. rotation journal 是否接受固定 96-byte 单阶段 intent，恢复通过 Catalog 状态判定，而不记录冗余 phase；
3. Scan 遇到任一 Retiring Segment 时 fail-fast 是否符合上层 checkpoint/replay 调度；
4. `RetireSegment` 是否应继续由 RecordLog 执行 Catalog remove 和物理删除，还是由 M5 maintenance coordinator 显式编排；
5. 顶层 Store lock、orphan 报告和 trash 清理放在 M3 Open 还是 M5 maintenance。

Review 通过后进入 M3：生成新的 Store/Batch/Coordinator 最小闭环，并使 v2 Record Protocol、RecordLog
和 Mapping 成为一条可运行路径；仍不通过 adapter 调用旧 runtime。

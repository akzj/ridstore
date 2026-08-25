# ridstore v2 M3 Review

状态：最小运行闭环已实现，等待架构 Review

提交：

- `493344f feat: add atomic group mapping`
- `9af7c09 feat: add recordlog-native transactions`
- `e4482b6 feat: add durable id allocator`
- `a1dd9d3 feat: add atomic commit coordinator`
- `d390b2c fix: validate transaction limits before use`
- `22bf83d feat: compose minimal record store engine`
- `d41cd3f test: exercise engine on real record log`

## 1. 阶段结论

M3 已形成一条不调用旧 AppendLog/Frame runtime 的最小闭环：

```text
Engine.Begin
  -> BatchID ReserveRecord(sync=true)
  -> Transaction PutRecord(sync=false)
  -> Coordinator ResolveGroup
  -> CommitGroupRecord(sync=true)
  -> Mapping PublishGroup
  -> Engine.Get
```

`internal/transaction`、`internal/idalloc`、`internal/coordinator` 是重新生成的最终职责包，不是旧
`internal/batch`、`internal/allocator`、`internal/commit` 的 adapter。旧包暂时只服务尚未切换的 v1
公开路径；M6 切换后直接删除。

## 2. 已建立的不变量

- Mapping 保存 `ID -> {VAddr, LogicalRevision}`，一个 durable CommitGroup 只执行一次原子发布；
- group 内后续 Batch 可观察前序 accepted Batch 的 virtual Mapping；冲突 Batch 不污染后续状态；
- 冲突 Batch 不写 Descriptor、不消耗 CommitSeq；空 Batch 仍可提交；
- Transaction 只保留每个 ID 的最终 mutation，PutRecord orphan 由后续 GC 回收；
- `Create` 依赖 ID 永不复用，不增加 `ExpectAbsent`，因此不产生无意义的 Mapping 条件读取；
- 用户 Put 的 Revision 等于 BatchID；Relocation CAS 保留原 Revision；
- ReserveRecord 必须 `sync=true` 成功后才发放新区间，重启从 durable high 之后继续；
- Commit 请求进入 Coordinator 队列后由 Coordinator 持有完成责任，调用者等待确定结果；
- Coordinator 不使用 MaxDelay。前一轮 I/O 期间自然排队的请求在下一轮合并；
- CommitGroup `sync=true` 成功前不发布 Mapping；durability 不确定时 Batch 进入 CommitUnknown，
  Coordinator fail-closed；
- Get 执行 `lookup -> read -> revalidate`，避免并发 Mapping 改变时返回地址与版本不一致的记录；
- MaxOpenBatches 是可取消的背压，不允许并发 Begin 越过上限。

## 3. 当前代码边界

| 包 | 职责 |
|---|---|
| `internal/mapping` | group 条件解析、virtual state、单锁原子发布 |
| `internal/transaction` | Batch 状态机、Put 编码、最终 mutation/condition 集合 |
| `internal/idalloc` | RecordID/BatchID durable range reservation |
| `internal/coordinator` | CommitSeq、自然 group、durable-before-publish、失败分类 |
| `internal/engine` | Begin/Get/Batch/Close 与 open-batch 生命周期组合 |

Engine 的真实 RecordLog 测试已经穿过协议 codec、writer、fsync 和读路径；内存 fake 只用于精确验证
冲突、状态机和背压。

## 4. 本轮 Review 修正

- `Create` 最初被实现为隐式 `ExpectAbsent`；Review 后删除。永久不复用 ID 已经证明 absent，额外条件
  会把 blind create 错误地变成 Mapping read；
- group byte 预算现在始终包含一次 CommitGroup header，切换到下一个 group 时不会漏算 header；
- Engine 在 Begin 发布 Batch 前先预占 open slot，避免并发 Begin 同时观察旧 map 长度而超限；
- Abort 与并发 Commit 只在 Batch 真正进入 terminal state 后释放 slot；
- Engine 构造时验证 Batch hard limits，不在首次 Begin 后才暴露配置错误。

## 5. 验证

已执行：

```text
go test -count=100 ./internal/coordinator
go test -race -count=20 ./internal/coordinator
go test -race -count=5 ./internal/engine ./internal/coordinator ./internal/idalloc ./internal/transaction ./internal/mapping
go test -count=1 ./...
go vet ./...
git diff --check
```

覆盖单 Batch durable publish、自然形成双 Batch group、组内 Revision 依赖、冲突不消耗 CommitSeq、
sync failure 的 CommitUnknown、Reserve fsync-before-issue、并发 ID 唯一性、真实 RecordLog round trip、
Get revision、Delete 和 open-batch backpressure。

## 6. 明确未完成

- 当前 `internal/mapping` 是 M3 内存 Mapping；尚未生成 persistent radix checkpoint；
- Engine 尚无 Create/Open recovery，重启后不能恢复 Mapping、allocator high 和 Batch status；
- checkpoint barrier、ReplayStart 与 Catalog 原子安装属于 M4；
- Relocation、SegmentStats 和 retire/delete 属于 M5；
- 顶层公开 `ridstore.Store` 仍是 v1 runtime，尚未切换到 Engine；
- 旧 `internal/batch`、`internal/allocator`、`internal/commit`、`internal/appendlog` 尚未删除；
- 状态保留、目录锁、故障注入矩阵和长期 soak 尚未进入 v2 主路径。

因此本阶段的准确结论是：v2 已有可执行的进程内最小读写闭环，但尚不具备重启恢复和公开 API
切换条件。下一阶段应先实现 protocol replay 与 checkpoint cut，再考虑删除旧 runtime。

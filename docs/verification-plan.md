# 验证与验收计划

状态：Acceptance contract v1

## 1. 原则

测试通过不是生产就绪的充分条件。ridstore 的证据分为：

```text
格式正确性
协议状态机
崩溃恢复
并发竞态
长期空间/资源收敛
同 durability 性能
```

每项结论必须说明验证边界。正常 Close 后重开不能替代 crash recovery；初始空库吞吐不能替代稳定 GC 状态。

## 2. 测试层次

### 2.1 Codec/Format

- golden bytes；
- encode/decode round trip；
- CRC 覆盖范围；
- Length/Count/Offset 溢出；
- padding 非零；
- unknown version/type/TLV；
- truncated Header/Payload/Footer；
- Store UUID/FileID 不匹配；
- full uint64 ID、边界 VAddr、ReplayStartLogPos 的 active-end/rotation 边界和类型不可互换；
- PutRecord 固定 64-byte Header、OriginBatchID revision、空 Value 和 Relocation revision preservation；
- SparseBitmap/Dense512 Node golden bytes、EntryCount/NodeSize/bitmap rank 边界；
- SegmentSeal/Footer 镜像字段、DescriptorCRC 精确拼接顺序和空 Batch vector；
- INITIALIZING/MAINTENANCE 各 OperationType/Phase 的 golden bytes、非法跳级和恢复后置条件；
- 所有 v1 Flags/CommitFlags/Reserved 非零均拒绝；
- SegmentStatsEntry 排序、重复、零项、covered seq 和 Root generation 一致性；
- fuzz 所有不可信 decoder。

### 2.2 模型属性测试

以简单内存参考模型：

```text
map[ID]{value, revision} + atomic conditional batch
```

随机生成 Allocate/Put/Delete/Commit/Abort/Get/Checkpoint/GC 序列，每步比较 ridstore 可见状态。

属性：

- Committed Batch 全部生效；
- Aborted Batch 零生效；
- 同 Batch 同 ID 最后操作生效；
- ID 不复用；
- Delete 幂等；
- GC 前后逻辑状态相同；
- Recovery 前后逻辑状态相同；
- Mapping Checkpoint 前后状态相同；
- 同一逻辑 Node 在 SparseBitmap/Dense512 编码下 Lookup 结果相同；
- 随机稀疏完整 uint64 ID 的 Mapping bytes 不退化为每个 Leaf 固定 4160 bytes；
- Blind Put 按 CommitSeq Last-Writer-Wins；
- ExpectRevision/ExpectAbsent 全部满足才提交；
- 任一条件冲突时所有 mutation 零生效且不产生 Seal；
- Relocation 不改变 LogicalRevision。

### 2.3 并发测试

- 多 goroutine 独立 Batch；
- 同 ID commit-order last-writer-wins；
- 同 ID ExpectRevision 竞争只有一个成功；
- 同组 ExpectAbsent 按 virtual Mapping 顺序只有一个成功；
- group commit 多 Batch 独立原子性；
- Lookup 与 Publish；
- Checkpoint cut 与 Commit；
- Reader pin 与 Retire；
- 用户 Put 与 GC CAS；
- 冷 Root 上 Relocation CAS 在 pre-Seal 阶段解析，publish 临界区零文件 I/O，runtime/recovery outcome 一致；
- Close 与 Commit/Checkpoint/GC；
- backpressure 和 Context cancel。

全部并发测试在 `-race` 下运行，并增加高重复次数和调度扰动。

## 3. Crash Harness

使用父进程驱动真实子进程和真实数据目录：

```text
parent
  -> start child workload
  -> wait for named failpoint
  -> SIGKILL / forced exit
  -> reopen with fresh process
  -> verify committed oracle
```

禁止：

- defer Close；
- 在 kill 前调用 Flush；
- 只 mock fsync 就宣称掉电安全；
- 复用进程内 Mapping 验证恢复。

Failpoint 必须位于真实 write/fsync/rename/dir sync/publish 操作两侧。

## 4. Commit Crash Matrix

每个 Case 至少验证旧值、新值、NotFound 和 Batch Status。

| 阶段 | 允许恢复结果 |
|---|---|
| PutRecord 前/中/后，无 Seal | Batch 不可见 |
| CommitPart 中间 | Active 尾部截断且 Batch 不可见；若损坏位于 Sealed/中间位置则 corruption |
| CommitSeal 中间 | Active 尾部截断且 Batch 不可见；若损坏位于 Sealed/中间位置则 corruption |
| Seal write 后、fsync 前 | Committed 或未提交，调用者语义 Unknown |
| fsync 后、Publish 前 | 必须 Committed |
| Publish 部分后 | 重启后必须整批 Committed |
| Publish 后、Reply 前 | 必须 Committed，Status 可确认 |
| 条件检查前/中取消或冲突 | 确定 Aborted，无 CommitSeal，Mapping 零变化 |

若硬件/文件系统不能模拟真实 power loss，报告只能称为 process-crash evidence。

初始化另有独立 crash matrix：INITIALIZING marker、子目录、两个初始 Segment Header、generation-1 Manifest、CURRENT 和 marker 删除的每个 write/fsync/rename 前后强制退出；恢复结果只能是可继续完成的同一 UUID Store 或明确错误，不能静默创建第二个空 Store。

## 5. ID 与 BatchID Reserve Crash Matrix

- Reserve frame 前崩溃：旧区间不变；
- write 后 fsync 前：恢复 high watermark 只能保持旧值或采用 durable 新值；
- fsync 后 publish 前：恢复必须采用新 high watermark；
- 已返回 ID 后崩溃：任何后续 Open 不得再次返回该 ID；
- 多次 reserve/rotation/checkpoint 后仍单调。

同一矩阵分别作用于 IDReserve 和 BatchIDReserve。额外验证：崩溃前已返回的 BatchID 不会在重启后重新分配；`Status` 不会把旧 BatchID 关联到新 Batch；uint64 边界返回 `ErrIDExhausted` 而不是回绕。

当前自动化 SIGKILL matrix 覆盖 reserve append 前、完整 write 后 sync 前、sync 后 allocator 内存 publish 前，以及 ID/BatchID 已返回四个边界。write 后 sync 前允许 fresh process 观察旧或新 durable high；sync 后只能采用新 high。另有多轮 ID/BatchID reserve 跨 Data Segment rotation、Checkpoint 和重新 Open 的单调性测试。该矩阵是 process-crash 证据，不替代设备忽略 flush 的 power-loss 验证。

## 6. Mapping Checkpoint Matrix

- captured/fresh Overlay 切换时并发 Commit；
- Mapping Node 写中断；
- Mapping file fsync 前后；
- Mapping Segment seal/footer/rename 前后；
- Manifest temp/write/fsync/rename；
- CURRENT temp/write/fsync/rename；
- Checkpoint 安装与 Data/Mapping Segment rotation、Data GC Manifest 安装并发；
- directory fsync 前后；
- 新 Root 发布后旧 Cache View 仍在读；
- Checkpoint 失败时 Delta 不丢失；
- Delete 后空 Leaf/Internal 路径剪枝；
- Sparse→Dense、Dense→Sparse 阈值两侧和 occupancy 1/503/504/512；
- 变长 Node 跨 Mapping Segment 尾部时先 rotation，不允许跨文件写 Node；
- Open 只加载 Root/上层，不全量加载；
- Delta hard limit backpressure。
- Root/Stats 在 Manifest 中同代安装：每个 write/fsync/rename 崩溃点只能得到旧 Root+旧 Stats 或新 Root+新 Stats；
- Stats Builder 对同一 ID 多次覆盖只计算 base→cut-final，Abort、条件冲突和 Relocation CAS failure 不计 live；
- Header 批读错误、Stats underflow 或 Segment 身份错误时新 Root/Stats 均不安装，frozen Delta 不丢失；
- cut 后并发 Commit、Checkpoint 安装和 Recovery replay 中，`exact Base + active/frozen additions` 始终不低于全量 Mapping 得到的精确 live；
- Stats 表为 0 或缺失 Segment 时仍不能绕过 GC 精确 Mapping 校验和删除门禁。

恢复只能选择完整旧 Root 或完整新 Root。

## 7. GC Matrix

采用 `gc-protocol.md` 第 16 节全部时序，并验证：

- 任意崩溃后旧 Record 或新 Record至少一个可读；
- Mapping 不指向不存在文件；
- CAS 失败不覆盖用户 Put；
- Reader pin 阻止提前删除；
- Journal Phase 可幂等继续；
- trash 文件不重新加入正式集合；
- 空间最终收敛；
- ENOSPC 不破坏旧数据。
- SegmentStats 只能改变候选顺序，不能改变任意 Record 的搬迁和删除结论。

## 8. Corruption 测试

主动翻转：

- Segment Header；
- Frame Header/Payload CRC；
- Commit Descriptor Entry；
- Segment Footer；
- Mapping Node Slot/CRC；
- Sparse bitmap、EntryCount、NodeSize、packed value 顺序和非法 Encoding；
- Manifest/CURRENT；
- INITIALIZING/ROTATION/Maintenance Journal。

期望：

- 不返回错误对象的数据；
- 不越界分配；
- 不 panic；
- 不静默修复 Sealed corruption；
- 返回可分类错误并 fail closed；
- Scrub 与 Open 对 corruption 的判断一致。

## 9. 资源与收敛测试

长期循环：

```text
Put/overwrite/delete/abort
-> checkpoint
-> data GC
-> mapping GC
```

观测：

- Data active/sealed/retired/trash 数量；
- Mapping files 和 unreachable bytes；
- Delta bytes；
- Mapping Cache bytes；
- RSS；
- FD；
- goroutine 数；
- disk allocated/logical bytes；
- GC copied/reclaimed bytes；
- Checkpoint lag。
- exact/live-upper SegmentStats、StatsCoveredCommitSeq 和 overestimate bytes。

通过条件不是固定文件数，而是工作负载停止后维护过程最终稳定、trash 清空、FD/goroutine 回到基线附近、空间不继续增长。

## 10. 性能对比

遵循 `positioning-vs-lsm.md` 的公平性约束。候选对照至少包括成熟 LSM 和一个简单 append baseline。

### 10.1 工作负载

- Value：128B、4KiB、64KiB、1MiB；
- Batch：1、10、100、1000；
- concurrency：1、4、16、64；
- create-only；
- hot overwrite；
- random overwrite；
- conditional overwrite：0%、10%、50% 冲突率；
- hot/cold ExpectRevision Header validation；
- delete-heavy；
- mixed 50/50；
- hot Mapping read；
- cold Mapping read，dataset > cache；
- 连续高 occupancy 与随机稀疏 ID 的 Mapping bytes/Lookup；
- 大量 Delete 后 Dense→Sparse 的空间收敛；
- foreground + checkpoint；
- foreground + GC；
- 不同 live ratio 稳定态。

### 10.2 指标

- ops/s 和 bytes/s；
- Commit p50/p99/p999；
- queue/write/fsync/publish 分段；
- condition validation latency、conflict rate、Header read bytes；
- read hot/cold latency；
- CPU、RSS、FD；
- user bytes / physical bytes；
- GC/compaction bytes；
- space amplification；
- recovery bytes/time；
- tail latency during maintenance。

所有结果保存原始 benchmark 输出、配置、Git commit、Go 版本、内核、文件系统和设备信息。

## 11. 工程命令门禁

项目形成代码后统一入口：

```text
make test
make test-race
make test-fuzz-smoke
make test-crash
make test-integration
make bench
make verify
```

最低合并门禁：

- `go test ./... -count=1`；
- `go test -race ./... -count=1`；
- `go vet ./...`；
- format golden tests；
- crash smoke suite；
- 无未解释数据竞争、panic、goroutine/FD 泄漏。

长时 fuzz、完整 crash matrix 和 soak 可以在独立 CI/nightly，但发布前必须完成。

## 12. Phase 验收

### Phase 0

- 格式 golden/fuzz；
- crash harness 可杀真实子进程；
- write/fsync/rename failpoint 可定位；
- Format Freeze Review 完成。

### Phase 1

- 单线程 Put/Delete/Get/Batch；
- GetRecord Revision、ExpectRevision/ExpectAbsent 与确定冲突；
- Commit/Abort/Unknown 矩阵；
- ID Reserve 不复用；
- 扫描恢复与参考模型一致。

### Phase 2

- group commit；
- group virtual Mapping 条件顺序与冲突隔离；
- 并发/取消/backpressure；
- race clean；
- 性能分段可解释。

### Phase 3

- Persistent Mapping、Cache、Checkpoint；
- 不全量加载；
- 内存有界；
- Root 安装 crash matrix。

### Phase 4

- Data/Mapping GC；
- Reader pin；
- Journal crash-resume；
- 空间和资源收敛。

### Phase 5

- Scrub/Verify、Backup/Restore；
- 完整 soak；
- 稳定态 RocksDB/Pebble 对比；
- 运维和格式升级文档。

## 13. 生产就绪声明

只有同时满足才可称为 production-ready：

- 所有 durable invariant 有自动化证据；
- 完整 crash matrix 通过；
- race/vet/fuzz 无未解决问题；
- soak 自然结束且报告有效；
- Data/Mapping GC 空间收敛；
- Scrub 验证通过；
- 备份恢复在独立目录验证；
- 格式版本和升级策略冻结；
- 已知限制明确；
- 性能报告区分 initial 与 steady state。

单次 benchmark、正常重启或单元测试全绿不能单独支持该声明。

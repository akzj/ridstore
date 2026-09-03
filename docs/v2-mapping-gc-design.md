# ridstore v2 Mapping GC

状态：G1-G5 implemented

## 1. 目标与边界

Mapping GC 解决 immutable Radix checkpoint 持续追加后，旧 Root 与旧 Node 永久占用磁盘的问题。它不是
LSM compaction，也不改变 `ID -> VAddr`、CommitSeq、Data Segment 或 Batch Status：

```text
current logical Mapping
  -> rebuild every reachable Radix node into a fresh Mapping generation
  -> atomically replace only Manifest.Mapping file set + MappingRoot
  -> switch the in-process reader/writer
  -> retire the old Mapping file set
```

显式 maintenance 操作始终可用。自动策略默认关闭；启用后 Scheduler 先执行 generation-bound 的物理文件与
可达 Node 精确 survey，达到 bytes 与 ratio 双门槛且越过 cooldown 后才提交 Mapping GC。

## 2. 必须保持的不变量

1. 发布前后 `CoveredCommitSeq`、`ReplayStart`、allocator high、open Batch cut、SegmentStats 和全部
   `ID -> VAddr` 完全不变；只允许 MapAddr 改变。
2. 新 generation 从空 Root 全量构建，任何 Node 都不得引用旧 Mapping Segment。
3. Mapping Segment ID 永不复用；新集合从旧 Manifest 的 `NextMapSegmentID` 开始连续分配。
4. Catalog 是唯一可见性边界。发布前旧集合权威，发布后新集合权威；目录中“看起来完整”的文件不能自行晋升。
5. 删除旧文件必须晚于新 Manifest durable、运行时 Root 切换和旧 reader 清零。
6. 任意 syscall 结果不确定时当前 Store fail closed；fresh Open 只依据 durable marker、Manifest 和文件事实收敛。

## 3. 并发切面

运行时协调顺序为：

```text
Mapping rewrite scheduler slot
  -> checkpoint capture lock (capture or final publish only)
  -> PublishCoordinator (durable Catalog generation)
```

Mapping GC 的 scheduler slot 只阻止另一轮 Mapping rewrite；Data maintenance 可以并行构建，最终由
PublishCoordinator 的 generation 校验合并或拒绝过期结果。初始 Coordinator admission fence 建立 durable
cut、捕获 Batch/allocator 元数据并冻结 Delta，随后立即释放；正常 checkpoint 在允许新 Commit 进入下一层
active Delta 的情况下安装 immutable Root。全量 Root 遍历、generation 构建和校验均不持有 admission fence
或 checkpoint capture lock。

最终阶段在 capture lock 内依次安装 recovery marker、提升新 generation、durable publish Catalog，并切换
运行时 Root 与 MapStore owner。完成这两个可见性切面后立即释放 capture lock；旧 reader drain、旧 Store
close、旧 generation retire、staging/marker cleanup 均在锁外执行。marker 恢复协议允许 cleanup 期间新的
Checkpoint 推进 Catalog generation，但 Mapping file-set 必须保持为已发布的新集合。

重建期间的新 Commit 留在 active Delta。GC 发布物理等价的新 Root 时不再暂停 Commit；Mapping epoch
使跨切换的 resolve 重试，Root owner reader ref 使旧文件延迟到旧 reader 清零后关闭。这样
O(live IDs) 遍历、promotion、Manifest fsync 和旧文件 retirement 都不会形成 Store 级数据面停顿；其中
promotion、Manifest fsync 与 runtime owner switch 仍会短暂推迟另一轮 Checkpoint capture，reader drain 与
retirement 不会。

## 4. 有界全量重建

旧 Radix `Walk` 已按 ID 严格递增输出。新 `radix.StreamBuilder` 从空 Root 接收有序 `(ID, VAddr)`，按 leaf
prefix 和固定深度 accumulator 逐层 flush；内存复杂度为一个 leaf 加每层一个 accumulator，不物化第二份
`O(live IDs)` 数组。

新 `mapstore.GenerationWriter`：

- 只接受起始 SegmentID、StoreID、SegmentSize 和 staging directory；
- 负责 NodeSeq、append、rotation、seal、sync，完全不访问 Catalog；
- `Finish(root, covered)` 返回新 sealed refs、active ID、next ID 和 Root；
- 失败时不得修改旧 MapStore，也不得删除无法确认归属的文件。

重建结束后必须用只读 Radix Walk 同时比较旧/新有序流，证明 ID、VAddr 和数量完全相同，再允许发布。

## 5. Durable marker 与状态机

Mapping GC 使用独立 `MAPPING-GC.v2` marker；不得复用只描述 DataRetire 的固定长度 `MAINTENANCE.v2`。
marker 包含：

- StoreUUID、BaseManifestGeneration、CoveredCommitSeq；
- staging directory identity；
- 旧 Mapping file set 摘要；
- 新 Mapping snapshot：sealed refs、active/next IDs、Root；
- payload length、版本和 CRC。

状态判断不依赖内存 phase：

```text
marker durable, Catalog still names old set
  -> 删除 staging/已提升的新文件，保留旧集合，移除 marker

Catalog names new set
  -> 验证新 Root，删除旧集合，移除 marker

Catalog 与 old/new 均不匹配
  -> ErrCorrupt，禁止猜测
```

新文件从 staging 提升到 canonical mapping 目录后必须 sync directory，随后才安装新 Manifest。旧文件先
rename 到 trash 并同步 mapping/trash 两侧目录，再 unlink 并同步 trash。任何失败保留 marker，使下一次
Open 可幂等继续。

## 6. Catalog 与运行时切换

Catalog 增加单一 `InstallMappingRewrite` mutation：要求 base 除 append-only Data rotation 外仍完全匹配、CoveredCommitSeq 不变、
新 SegmentID 区间从旧 `NextMapSegmentID` 开始，并一次替换 `SealedMapSegments`、`ActiveMapSegmentID`、
`NextMapSegmentID` 和 `MappingRoot`。它不得改写其他 Manifest 字段。

Manifest durable 后：

1. 以新 Catalog 打开新的 MapStore；
2. 打开并验证新 Radix Root；
3. `Persistent.ReplaceCheckpointRoot` 原子替换 root 与 owner，要求无 frozen Delta 且 covered 相同，并保留 active Delta；
4. Engine 替换 `mapStore` owner；
5. 等待旧 Root reader ref 清零，关闭旧 MapStore，随后执行旧文件 retirement。

若 Manifest 已发布但进程内切换失败，Store 立即 fail closed；不得继续使用旧 Root 接受 Commit。

## 7. 恢复顺序

正常 Open 的顺序调整为：

```text
exclusive LOCK
  -> load Catalog
  -> recover RecordLog rotation
  -> recover Mapping rotation
  -> recover Mapping GC marker
  -> recover Data GC marker
  -> open authoritative RecordLog/MapStore
  -> replay
```

Mapping rotation 与 Mapping GC 不能同时存在。GC 开始前必须确认 rotation journal 不存在；GC builder 的
rotation 只发生在 staging 内，不生成正常运行时 rotation journal。

## 8. 故障与验证矩阵

至少覆盖：staging create/write/file-sync、staging directory sync、每个文件 promote rename、mapping directory
sync、Manifest write/sync/rename/root-sync、旧文件 trash rename、mapping/trash directory sync、unlink 和最终
marker remove。每个边界验证原始错误保留、当前 Store fail closed、fresh Open 可再次恢复。

子进程退出至少覆盖：marker-only、部分新文件已提升、Manifest 已发布但旧文件尚在、部分旧文件进 trash、
trash 已删除但 marker 尚在。成功后必须通过 v2 Offline Verify，且所有 ID/Value 与 GC 前一致。

## 9. 实现阶段

1. **G1 streaming rebuild（已实现）**：Radix 有界流式 builder 与独立 GenerationWriter；
2. **G2 catalog transaction（已实现）**：Mapping file-set 原子替换及字段不变性测试；
3. **G3 durable recovery（已实现）**：独立 marker、staging promotion、old/new Catalog 判定、发布前回滚、
   发布后新 Root 验证、旧文件 retirement、fresh-open 幂等收敛及 Verify 门禁；
4. **G4 runtime switch（已实现）**：Engine 固定锁序、同一独占 cut 下的 checkpoint + rebuild、
   `Persistent.ReplaceCheckpointRoot`、旧 owner 关闭后 retirement，以及公开 `CompactMapping(ctx)`；
5. **G5 evidence（已实现）**：generation header/node/footer/sync、marker、promotion、Manifest rewrite、
   rollback、retirement 和 marker cleanup 的 runtime fault matrix；rollback 自身失败保留恢复证据；多文件
   partial promote/retire；staging build、marker-only、Catalog published、old files in trash、trash deleted 五个
   process-exit 恢复点；race、Offline Verify、重复 GC 与物理空间收敛测试。

自动触发只消费精确 survey 的缓存结果，不改变本节的 durable marker、publication 或 recovery 协议。

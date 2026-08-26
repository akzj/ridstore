# ridstore v2 Mapping GC

状态：Design frozen for implementation

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

第一版只提供显式 maintenance 操作，不增加后台策略。触发阈值、周期调度和限速属于后续策略层。

## 2. 必须保持的不变量

1. 发布前后 `CoveredCommitSeq`、`ReplayStart`、allocator high、open Batch cut、SegmentStats 和全部
   `ID -> VAddr` 完全不变；只允许 MapAddr 改变。
2. 新 generation 从空 Root 全量构建，任何 Node 都不得引用旧 Mapping Segment。
3. Mapping Segment ID 永不复用；新集合从旧 Manifest 的 `NextMapSegmentID` 开始连续分配。
4. Catalog 是唯一可见性边界。发布前旧集合权威，发布后新集合权威；目录中“看起来完整”的文件不能自行晋升。
5. 删除旧文件必须晚于新 Manifest durable、运行时 Root 切换和旧 reader 清零。
6. 任意 syscall 结果不确定时当前 Store fail closed；fresh Open 只依据 durable marker、Manifest 和文件事实收敛。

## 3. 并发切面

锁顺序固定为：

```text
maintenanceMu -> checkpointMu -> ops.Lock
```

Mapping GC 持有三者直到运行时切换完成。`ops.Lock` 会等待已进入的 Get/Commit/Batch 操作退出，并阻止新操作；
随后向唯一 Coordinator 插入 barrier，确保此前已接纳 Commit 全部完成。GC 在该 cut 上完成正常 checkpoint，
使 Persistent Mapping 不再含 active/frozen Delta，再复制 Root。

不能简单调用公开 `Checkpoint` 后再抢 `ops.Lock`：两者之间可以进入新 Commit，从而使待重写 Root 立即过期。
实现应抽取一个“调用者已持有 checkpointMu + ops.Lock”的 checkpoint 内核，而不是增加第二套 checkpoint 协议。

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

Catalog 增加单一 `InstallMappingRewrite` mutation：要求 generation 精确匹配、CoveredCommitSeq 不变、
新 SegmentID 区间从旧 `NextMapSegmentID` 开始，并一次替换 `SealedMapSegments`、`ActiveMapSegmentID`、
`NextMapSegmentID` 和 `MappingRoot`。它不得改写其他 Manifest 字段。

Manifest durable 后，在 `ops.Lock` 内：

1. 以新 Catalog 打开新的 MapStore；
2. 打开并验证新 Radix Root；
3. `Persistent.ReplaceCheckpointRoot` 原子替换 root 与 syncer，要求无 active/frozen Delta 且 covered 相同；
4. Engine 替换 `mapStore` owner；
5. 关闭旧 MapStore，随后执行旧文件 retirement。

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

1. **G1 streaming rebuild**：Radix 有界流式 builder 与独立 GenerationWriter；
2. **G2 catalog transaction**：Mapping file-set 原子替换及字段不变性测试；
3. **G3 durable recovery**：独立 marker、promotion/retirement 和 fresh-open 收敛；
4. **G4 runtime switch**：Engine 锁序、checkpoint cut、Persistent rebase 和公开显式入口；
5. **G5 evidence**：fault matrix、process-exit、race、Verify 与重复 GC 收敛测试。

在 G3 完成前不得发布新 Manifest；在 G4 完成前不得将该实现接入 Store。


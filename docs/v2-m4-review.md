# ridstore v2 M4 Review

状态：核心运行闭环、Delta hard admission 与 bounded builder 已实现

## 1. 本阶段结果

M4 已建立一条不依赖旧 Mapping/Recovery 的恢复与 checkpoint 主路径：

```text
Catalog Manifest
  -> RecordLog + MapStore Open
  -> immutable Radix Root
  -> Persistent Mapping
  -> replay from exact ReplayStart
  -> Engine

Engine.Checkpoint
  -> quiesce API operations
  -> Coordinator durable CheckpointMarker
  -> capture allocator/open-batch state + freeze Delta
  -> resume API operations
  -> build and fsync candidate Root
  -> walk candidate Root and build exact sealed SegmentStats
  -> Catalog generation CAS
  -> install candidate in runtime Mapping
```

Catalog 成功前 frozen layers 始终可见且不会释放。Catalog 安装失败时 Abort 只结束本轮构建，保留
全部 frozen layers；下一轮会连同新 active Delta 再次构建。Catalog 已成功而内存 Install 失败时
Engine fail closed，重启以 durable Manifest 为准。

## 2. 关键所有权

- Coordinator 独占 CommitSeq 顺序，并把 checkpoint marker 排在此前已 admission 的 Commit 后；
- Engine 的 `ops` barrier 只保护 cut、open Batch 和 allocator 快照，不覆盖后台 Root/Stats I/O；
- Persistent Mapping 独占 active/frozen/root 发布；
- Persistent Mapping 同时独占 Delta charged/reserved 预算；Coordinator 只持 reservation，Engine 只负责
  在 hard pressure 时推进 Checkpoint；
- MapStore 独占 Mapping Node append 与 fsync；
- Catalog 独占 Manifest generation；并发 Data/Map rotation 通过 generation CAS 使过期 checkpoint 失败；
- SegmentStats 是 checkpoint 派生数据，不进入 Put/Commit 热路径，也不单独授权 GC 删除。
- Engine Open 在任何 RecordLog/MapStore 恢复前取得目录独占锁，Open 失败释放，Close 在所有文件关闭后
  最后释放；同一目录不能同时存在两个 v2 writer。

## 3. 精确统计路径

第一版按设计选择有界内存、顺序遍历 candidate Root：

- Radix `Walk` 按 ID 顺序遍历 immutable checkpoint；
- `RecordLog.Inspect` 读取物理 Header 和固定 Put header，不读取 Value body；
- Put metadata 必须与 Mapping ID、VAddr、物理大小和 Segment 边界一致；
- 只为 sealed Segment 输出非零统计，按 SegmentID 排序；
- 未知 Segment、统计溢出、身份不一致或损坏都会阻止 Manifest 安装。

这里验证的是统计所需 Header，不声称校验未读取 Value 的 payload CRC。正常 Get、Replay、Verify/GC
仍负责完整 Record 校验。

## 4. 已验证

- checkpoint marker 位于此前 Commit 之后并返回精确 ReplayStart；
- Persistent Mapping Freeze/Build/Install 期间的新 Commit 不丢失；
- 高位 ID 的 Radix Walk 顺序与地址一致；
- SegmentStats 覆盖多 Segment、active 排除、身份错误、未知 Segment和预算上限；
- 小 Segment 触发 RecordLog rotation 后，Checkpoint 同代安装非空精确统计；
- Close/Reopen 从新 Root 和 ReplayStart 恢复并读取原值；
- 第二次 Open 返回 `ErrLocked`，第一个 Store Close 后可重新 Open；
- v2 Create 以初始 Manifest 作为 durable marker，重试保持 StoreUUID/RecordLogID，并幂等恢复两个初始
  Segment；普通 Open 拒绝未完成初始化；
- Checkpoint Manifest 的 write、file sync、rename、directory sync 四个失败边界均 fail closed；fresh Open
  分别从旧 generation replay 或从已发布新 generation 恢复，已提交值不丢失；
- MapStore Node append/checkpoint sync 失败会 poison writer 并使 Engine fail closed；旧 Manifest 不变，
  fresh Open 忽略或修复不可达 COW tail，再从 RecordLog replay 恢复已提交值；active-tail truncate/sync
  失败可再次 Open 收敛；
- RecordLog data sync 失败使提交返回 CommitUnknown、拒绝后续写；fresh Open 对完整 Commit Record 判定
  Committed，对不完整 Commit Record 截断并判定 Aborted；
- MapStore rotation 的 Journal、Footer、sealed rename、new-active、Catalog publish 与 cleanup 全部故障边界
  已覆盖；每一点 fresh Open 均由旧/新 Catalog 与 Journal 收敛，recovery 自身失败可再次 Open，且
  journal/sealed/new-active 三阶段已通过子进程退出测试；
- RecordLog rotation 在 Journal 发布前先 sync 其声明的旧 Segment 前缀；Journal、partial-footer repair、
  Footer、sealed rename、new-active、Catalog publish 与 cleanup 的逐点失败均由 fresh Open 收敛，recovery
  自身失败可再次 Open，且成功恢复后 Catalog 与目录文件集合精确一致；
- Delta reservation 位于 Prepare/durable Descriptor 之前，冲突、取消和 pre-durable 失败会归还；重复更新
  active hot ID 不重复计费，Freeze/Abort 不释放，Install 只释放精确 frozen prefix；
- Delta hard pressure 会在不持有 `ops.RLock` 等待的情况下推进 Checkpoint 并重试；Commit、Checkpoint、
  Close 的并发路径已纳入 race 测试；
- Open replay 超过本次 Delta hard limit 时确定返回配置错误，不部分发布、不等待运行期 Checkpoint；
- 配置拒绝 `floor(DeltaHardLimitBytes / 64) > floor(CheckpointSortBytes / 16)`，保证 Builder 至少能处理
  admission 允许形成的最坏 frozen entry 数；Mapping sort 与 SegmentStats 使用独立预算；
- Checkpoint 以单一原地稳定排序数组折叠 frozen layers，Radix 以固定深度 accumulator 流式生成父节点，
  不再保留 `latest map`、第二份 mutation copy 或逐层 O(N) child-change slice；
- 相关包 race、全仓 test、vet 与 diff check 通过。

## 5. 尚未完成

- checkpoint Catalog、MapStore append/sync/tail-repair/rotation，以及 RecordLog append/sync/rotation 的
  syscall fault matrix 已覆盖；详见 [v2-fault-matrix.md](v2-fault-matrix.md)；
- soft-limit 后台 Checkpoint；
- Relocation、Data GC、Mapping GC；
- 顶层公开 API 切换和旧 v1 模块删除。

因此 M4 目前证明正常执行、重启恢复、Delta 有界接纳与 Checkpoint 有界构建闭环，不构成
production-ready 声明。下一优先级是复核完整 M4 durable boundary，再决定是否进入 Relocation/GC，
不能先删除旧公开路径。

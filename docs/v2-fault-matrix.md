# ridstore v2 Durable Fault Matrix

状态：持续更新；仅记录当前 v2 writer

## 1. 判定标准

v2 对每个 durable writer 分开验证：

- syscall 返回错误时，原始 cause 必须保留；
- 写入状态不确定的 writer 必须 fail closed；
- Catalog 未安装时，fresh Open 只能使用旧 Manifest；
- Catalog 已安装时，fresh Open 必须完成或验证已发布状态；
- 重试恢复不能依赖失败进程的内存状态。

单元 fault hook 证明错误路径；子进程退出测试证明阶段边界后的恢复。两者不能互相替代。

## 2. 当前覆盖

| v2 writer | syscall/fault hook | fresh-open / process crash | 当前结论 |
|---|---|---|---|
| Catalog Manifest | write、file sync、rename、directory sync | Engine fresh Open 覆盖旧/新 generation | 已闭合 |
| MapStore Node append | append write | Engine fresh Open 从旧 Manifest + RecordLog replay | 已覆盖 |
| MapStore checkpoint sync | file sync | Engine fail closed；fresh Open 忽略不可达 Node 并 replay | 已覆盖 |
| MapStore active tail repair | truncate、file sync | 失败后再次 Open 可完成修复 | 已覆盖 |
| MapStore rotation | journal write/sync/rename/dir-sync，footer write/sync，seal rename/dir-sync，new-active write/sync/rename/dir-sync，journal remove/cleanup dir-sync | 每点 fresh Open 收敛；recovery 自身失败可重试；journal/sealed/new-active 子进程退出已覆盖 | 已闭合 |
| RecordLog append/sync | append write 与 data sync fault hook 已有 | Engine 已覆盖 CommitUnknown、fail-closed，以及 fresh Open 对完整/不完整 Commit Record 的 Committed/Aborted 判定 | 已覆盖当前 active append/sync 边界 |
| RecordLog rotation | journal write/sync/rename/dir-sync、partial-footer truncate/sync、footer write/sync、seal rename/dir-sync、new-active write/sync/rename/dir-sync、journal remove/cleanup dir-sync | 每点 fresh Open 收敛；recovery 失败可重试；journal/sealed/new-active 子进程退出已覆盖 | 已闭合 |
| v2 Data GC | marker temp/write/file-sync/close/rename/dir-sync/remove；retire rename、records/trash dir-sync、trash unlink/final dir-sync | 每个 fault point 可由 fresh Open 重试；子进程覆盖 marker-only、Catalog removed、trash、deleted 四个状态 | durable fault/crash matrix 已闭合 |
| Public Batch lifecycle | 复用底层 durable writer 边界 | 子进程退出覆盖 uncommitted Put、Checkpoint-open、committed tail、checkpoint-committed；公开 Open 验证 Value 与 Status | 已覆盖公开恢复语义 |
| v2 Mapping GC | generation header/node/footer/sync、marker、promotion、Manifest rewrite、rollback、retirement、cleanup；多文件 partial failure | fresh Open old/new Catalog 收敛；staging/marker/Catalog/trash/deleted 五阶段 process-exit；Offline Verify 与空间收敛 | durable fault/crash coverage 已闭合 |
| v2 Backup / Restore | 每次 lstat/readDir/open/openFile/mkdir/mkdirTemp/remove/rename/read/write/stat/sync/close 注入 EIO、ENOSPC、EACCES；另含 short-write 与 cleanup 双故障 | Backup staging/partial/metadata/marker-removed/published 与 Restore partial/verified/marker-removed/published 子进程退出 | 离线全量 writer fault/crash matrix 已闭合；远端传输不在范围内 |

## 3. MapStore 已确认语义

Checkpoint 构建产生的 Node 在 Catalog 发布前都是 COW orphan。Node append 或 checkpoint sync 失败时：

```text
MapStore poisoned
-> Engine read-only
-> old Manifest remains authoritative
-> fresh Open validates/truncates active tail as needed
-> replay durable RecordLog tail rebuilds Mapping Delta
```

完整但未被旧 Root 引用的 Node 可以保留；它不具备可见性，也不能被目录扫描提升为 Root。不完整 Node
只能位于 active tail，且旧 Manifest Root 不得指向该 tail；Open 才允许截断。

## 4. RecordLog rotation 已确认语义

rotation 在发布 durable Journal 之前先 sync Journal 所描述的旧 Segment 前缀。否则一次掉电可能留下
durable Journal，却丢失它声明的 `Old.ValidEnd` 数据，恢复将没有可验证的物理事实。

Journal 尚未 rename 时，fresh Open 删除 `.tmp` 并继续使用旧 Catalog；Journal 已可见时，fresh Open
完成 seal、new-active 创建和 Catalog 安装。Catalog 已安装则只验证精确文件集合并清理 Journal。
cleanup 删除成功但 directory sync 失败时，下次 Open 即使看不到 Journal，也会重新 sync Journal 目录。

## 5. Data GC 已确认语义

marker 发布前的失败不会授权 Catalog remove。marker rename 后 directory sync 失败属于 durable outcome
不确定，Engine 必须 fail closed；fresh Open 读取实际目录状态，不能允许当前进程继续 checkpoint 或维护。

Catalog remove 之后的任一物理清理错误都保留 marker，并使运行时只读。恢复可能看到 canonical、trash、
或二者都不存在；它会先稳定 `records/` 与 `trash/` 目录，再删除 trash。特别是从上一次进程继承 trash
时，不能跳过两目录 fsync，否则未持久化的跨目录 rename 仍可能在 marker 删除后回滚。

真实子进程退出覆盖：marker durable/Catalog present、Catalog removed/canonical present、trash present、
trash deleted/marker present。四种状态 fresh Open 都保持 relocated value 可读，并按 Catalog membership
选择回滚或完成，而不是使用内存 phase。

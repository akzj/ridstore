# ridstore v2 Durable Fault Matrix

状态：持续更新；不与 Format v1 的 `syscall-fault-matrix.md` 合并

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
| v2 Data GC relocation | sealed Segment 单段扫描、Put 复制、共享 BatchID、Coordinator CAS 已进入主路径 | 并发用户更新胜出并使 relocation skip；复制品成为 orphan | Checkpoint coverage、精确零存活证明和 retire crash matrix 尚未接线 |
| v2 Mapping GC | 尚未进入 v2 主路径 | 尚未进入 v2 主路径 | 不适用 |

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

进入 Relocation/Data GC 前，下一步是全局复核 M4 durable boundary，确认没有同级别缺口。

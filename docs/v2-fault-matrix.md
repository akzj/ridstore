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
| MapStore rotation | 尚未逐 syscall 注入 | prepared/committed 状态恢复测试已有 | 待补 |
| RecordLog append/sync | append write 已有；data sync 需补齐矩阵 | writer poison 测试已有；Engine 级 CommitUnknown/fresh Open 待补 | 部分覆盖 |
| RecordLog rotation | create syscall 边界已有 | journal/sealed/new-active 子进程退出已覆盖 | syscall matrix 待补 |
| v2 Data GC / Mapping GC | 尚未进入 v2 主路径 | 尚未进入 v2 主路径 | 不适用 |

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

## 4. 下一门禁

进入 Relocation/Data GC 前仍需完成：

1. MapStore rotation journal、footer、new-active、rename 与 directory sync 的逐点错误矩阵；
2. RecordLog data sync 的 `CommitUnknown` 与 fresh-open committed/aborted 两种结果；
3. RecordLog rotation 的逐 syscall 错误矩阵；
4. 上述路径的重复恢复与目录文件集合断言。

这些工作只增加故障注入与恢复证据，不改变 Catalog、RecordLog 或 Mapping 的所有权。

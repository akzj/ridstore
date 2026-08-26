# ridstore v2 Maintenance Marker

状态：Implemented format；durable fault/crash matrix closed

## 1. 定位

`journal/MAINTENANCE.v2` 是一次不可逆维护操作的唯一 durable marker。它不是进度日志，不复制
Catalog，也不记录内存 Phase。当前唯一 Operation 是 Data Segment retirement。

正确性依据只有：

```text
marker + authoritative Catalog generation/membership
```

marker 安装前只会产生 COW orphan。marker 安装后，第一个不可逆动作是从 Catalog 移除 source。

## 2. 固定编码

文件固定 128 bytes，小端序，CRC32C 覆盖 `[0,124)`：

| Offset | Size | Field |
|---:|---:|---|
| 0 | 8 | magic `RIDMNT2\0` |
| 8 | 2 | version = 2 |
| 10 | 2 | encoded size = 128 |
| 12 | 1 | operation，1 = DataRetire |
| 13 | 3 | reserved = 0 |
| 16 | 16 | StoreUUID |
| 32 | 16 | RecordLogID |
| 48 | 8 | BaseCatalogGeneration |
| 56 | 8 | CoveredCommitSeq |
| 64 | 8 | ReplayStart LogPos |
| 72 | 4 | source SegmentID |
| 76 | 4 | source ValidEnd |
| 80 | 8 | source RecordCount |
| 88 | 8 | source FirstAddr |
| 96 | 8 | source LastAddr |
| 104 | 20 | reserved = 0 |
| 124 | 4 | CRC32C |

source summary 必须与 sealed Footer 和 BaseGeneration Catalog 完全一致。未知 operation、非零 reserved、
错误地址或 CRC 一律拒绝。

## 3. 发布与清理

发布顺序：

```text
remove stale temp
-> fsync journal directory when a stale temp was removed
-> create .MAINTENANCE.v2.tmp
-> write full 128 bytes
-> fsync file
-> close file
-> rename to MAINTENANCE.v2
-> fsync journal directory
```

已有 final marker 时拒绝启动第二个维护操作。temp 不是权威状态；Open 只删除 regular temp，并将目录
fsync。symlink 或非 regular 路径视为 corruption。

完成后删除 final/temp 并 fsync journal directory。

## 4. 恢复判定

Open 已持有 Store directory lock，并在打开 RecordLog 文件前恢复：

| Catalog | 判定与动作 |
|---|---|
| generation = base，source summary 仍存在，checkpoint tuple 与 marker 一致 | Catalog remove 未发生；删除 marker |
| generation = base+1，source 不存在 | Catalog remove 已 durable；继续 canonical→trash→delete，再删除 marker |
| 其他情况 | corruption；不猜测、不删除 |

物理清理是幂等的：source 存在则 rename 到带 Catalog generation 的确定 trash 名称；在删除 trash 前总是
重新 fsync `records/` 和 `trash/` 两个目录，即使 rename 来自上一次失败的进程。随后 unlink trash 并再次
fsync `trash/`。两者均不存在表示已经完成；两者同时存在表示 corruption。trash 目录必须是真实目录，
symlink 或其他文件类型一律拒绝。

marker rename 已发生、但 journal directory fsync 返回错误时，调用方不能继续运行：marker 是否 durable
已经不确定，Engine 进入 `RecoveryRequired`。fresh Open 以实际可见 marker 和 Catalog 为准收敛。

## 5. 不变量

- Catalog source removal 永远早于物理删除；
- marker 永远早于 Catalog source removal；
- marker 删除永远晚于物理清理；
- marker、temp、trash 路径都不跟随 symlink；
- SegmentStats 只是一项前置条件，Engine 仍执行 open-batch 与 Mapping 精确证明；
- 未匹配 marker 的 Catalog 外文件不会被该恢复协议自动删除。

## 6. 已验证崩溃点

fault hook 覆盖 temp cleanup/create、marker write/file-sync/close/rename/directory-sync、marker unlink，
以及 source-to-trash rename、两个目录的稳定化、trash unlink 和最终 directory sync。子进程退出测试覆盖：

- marker durable，Catalog 仍拥有 source；
- Catalog 已移除 source，canonical 文件仍存在；
- source 已进入 trash；
- trash 已删除，marker 仍存在。

四种状态均由 fresh Open 收敛，且 relocated value 保持可读。

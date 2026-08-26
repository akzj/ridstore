# ridstore v2 Maintenance Marker

状态：Implemented format；fault matrix pending

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
-> create .MAINTENANCE.v2.tmp
-> write full 128 bytes
-> fsync file
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

物理清理是幂等的：source 存在则 rename 到带 Catalog generation 的确定 trash 名称；trash 存在则删除；
两者均不存在表示已经完成；两者同时存在表示 corruption。

## 5. 不变量

- Catalog source removal 永远早于物理删除；
- marker 永远早于 Catalog source removal；
- marker 删除永远晚于物理清理；
- SegmentStats 只是一项前置条件，Engine 仍执行 open-batch 与 Mapping 精确证明；
- 未匹配 marker 的 Catalog 外文件不会被该恢复协议自动删除。

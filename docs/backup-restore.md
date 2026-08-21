# Consistent Backup 与 Restore

状态：Phase 5 protocol v1

## 1. 边界

第一版只提供离线目录备份。`ridstore-tool backup` 必须取得源 Store 的独占
`LOCK`，因此不会与 Commit、Checkpoint、Rotation 或 GC 并发。它不是在线
snapshot，也不通过复制一个正在变化的 Active tail 来猜测一致性。

```text
go run ./cmd/ridstore-tool backup \
  --source /path/to/offline-store \
  --dest /path/to/new-backup
```

备份前必须通过与 `verify` 相同的只读完整性检查；存在 INITIALIZING、
MAINTENANCE、ROTATION、invalid active tail、trash 文件或其他 corruption 时拒绝
创建备份。operator 应先用匹配版本正常 Open/Close 完成恢复，再重试。

## 2. Artifact v1

备份是独立目录，而不是可直接 Open 的 Store：

```text
backup/
  BACKUP.json
  files/
    CURRENT
    manifests/MANIFEST-<current-generation>
    data/<Manifest 引用的全部 Data files>
    mapping/<Manifest 引用的全部 Mapping files>
```

创建期间根目录含 `INCOMPLETE`。只有全部文件 copy、`fsync`、逐文件 SHA-256
计算、`BACKUP.json` 写入并同步、所有目录同步完成后，才删除 `INCOMPLETE` 并
再次同步根目录。已经存在的目标目录绝不覆盖；失败或取消后保留
`INCOMPLETE` artifact 作为明确的未完成证据。

`BACKUP.json` 包含 artifact format/version、源 StoreUUID、Manifest generation、
创建时间以及按 path 严格排序的 `(path,size,sha256)`。path 只能是协议生成的
相对 clean path，不能包含绝对路径、`.` 或 `..`。Restore 必须拒绝重复、未知、
缺失、非普通文件、symlink、size/hash 不匹配和 artifact version 不支持。

备份只复制 CURRENT 所指向的 Manifest 及其正式文件集合。旧 Manifest、LOCK、
journal、trash、tmp 和未被 Manifest 引用的文件都不是 snapshot 的一部分。

## 3. 一致性时序

```text
Acquire source LOCK
-> offline Verify under the same lease
-> derive exact file set from current Manifest
-> create destination + INCOMPLETE
-> copy each regular file, fsync, hash
-> run full Verify against the copied payload
-> write/fsync BACKUP.json
-> fsync child directories and artifact root
-> remove INCOMPLETE
-> fsync artifact root
-> release source LOCK
```

Verify 与 copy 之间不能释放源 lease；否则另一个 writer 可以替换 Active tail 或
CURRENT，使 artifact 混合两个 durable generation。

## 4. Restore 与 UUID 策略

Restore 只接受一个不存在的新目录。它先在目标目录的私有 `.payload` 中恢复并
运行完整 offline Verify，随后在 `RESTORING` marker 保护下发布正式目录；
`ridstore.Open` 和公开 `verify` 必须拒绝仍含 `RESTORING` 的目录。目标目录已存在
时绝不覆盖或合并。

```text
go run ./cmd/ridstore-tool restore \
  --backup /path/to/backup \
  --dest /path/to/new-store

# 仅用于确保旧 Store 不会重新成为 writer 的灾难替换
go run ./cmd/ridstore-tool restore \
  --backup /path/to/backup \
  --dest /path/to/replacement-store \
  --preserve-uuid
```

默认 Restore 生成新的 StoreUUID，并重写 Manifest 与所有 Data/Mapping Segment
Header 的 UUID 和 CRC。Record Frame、Mapping Node、Footer、ID、Revision、
CommitSeq 和 Manifest generation 不改变。新 UUID 防止原 Store 与可写 clone
分叉后，operator 把两边同 FileID 文件误混而仍通过身份检查。

灾难恢复替换原 Store 时可显式选择 `preserve UUID`。此模式保持备份的 UUID，
但 operator 必须保证旧实例不会再次成为 writer。该选择必须出现在命令参数和
restore report 中，不能由工具猜测。

Restore 完成条件：artifact hashes 全部通过、payload Verify clean、目标目录布局
在仍持有 lease 且 `RESTORING` 尚存在时再次 Verify clean，并且 report 中 UUID 与
选择的策略一致。最后才删除并同步 marker；此前任何失败都保留 `RESTORING`，
目标不能被正常 Open。

SIGKILL subprocess matrix 覆盖 Backup prepared/files-copied/payload-verified/
metadata-synced/marker-removed/published，以及 Restore prepared/files-copied/
UUID-rewritten/payload-verified/payload-published/layout-verified/marker-removed/
published。marker 前崩溃必须保持明确 incomplete；marker 删除后必须得到可完整
Verify/Open 的 artifact 或 Store。该证据属于 process-crash，不等价于存储设备
忽略 flush 的 power-loss 证明。

## 5. 不提供的保证

- 不做在线增量备份、hardlink/reflink snapshot、压缩、加密或远端传输；
- 不把 artifact 自身当作长期归档介质规范；外部系统仍需提供不可变保存和副本；
- `fsync` 证据仍受本机文件系统和存储设备语义限制；
- Backup/Restore 不能替代完整 crash matrix、长期 soak 或应用级恢复演练。

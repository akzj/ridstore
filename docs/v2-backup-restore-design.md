# ridstore v2 Backup / Restore

状态：Implemented；离线全量 v2 主路径、关键 fault 边界与 process-crash matrix 已覆盖

## 1. 第一版边界

第一版只提供离线、全量、单机 Backup/Restore：

- Backup 要求源 Store 已关闭，并持有与 `Open` 相同的独占目录锁直到复制和校验完成；
- Restore 只发布到不存在的新目录，不覆盖、不合并已有 Store；
- artifact 只包含 Format v2，不识别、导入或迁移 Format v1；
- 第一版是灾难恢复快照，按字节恢复并保留 StoreUUID、RecordLogID、ID、CommitSeq 和 Batch 状态；
- 不提供可同时写入的 clone。需要 clone 时应另行设计“新 identity + 全文件重写”，不能暗中复用 Restore。

不提供在线 snapshot、增量备份、压缩、加密、远端传输或对象存储协议。

## 2. 核心不变量

1. Backup 的 Manifest、Data active tail、Mapping Root 必须来自同一个独占 lease；Verify 与复制之间不得释放锁。
2. 只复制最新权威 Manifest 引用的文件集；journal、trash、staging、临时文件和旧 Manifest slot 不进入 artifact。
3. artifact 在完成前必须显式标记 incomplete；目标路径已经存在时绝不覆盖。
4. Restore 在发布前必须完成 artifact hash 校验和 v2 Offline Verify。
5. 发布边界是同一父目录中的 directory rename；rename 后必须 fsync 父目录。
6. 任何相对路径必须来自协议生成的白名单，禁止绝对路径、`.`、`..`、重复路径和 symlink。
7. Restore 保留 StoreUUID，因此旧 Store 与恢复 Store 不得同时作为 writer。该限制必须出现在 API、报告和文档中。

## 3. Artifact 布局

```text
backup-dir/
  BACKUP-v2
  payload/
    MANIFEST-v2-{slot}
    records/
      record-*.sealed
      record-*.active
    mapping-v2/
      map-*.sealed
      map-*.active
```

构建期间使用同级隐藏 staging 目录，并放置 `INCOMPLETE-v2`。最终 `backup-dir` 在 publication rename 前
不存在。`LOCK`、`journal/`、`trash/`、Mapping GC staging 和所有 `.tmp`/`.creating` 文件不复制。

`BACKUP-v2` 使用有界、带版本和 CRC 的二进制元数据，至少包含：

- artifact format major/minor；
- StoreUUID、RecordLogID、Manifest generation；
- 创建时间仅作为报告信息，不参与恢复正确性；
- 按 path 严格排序的 `(relative path, size, SHA-256)`；
- entry count、payload length 和总 CRC。

解码必须先验证总长度和 entry 上限，再分配内存。未知 required 字段返回 unsupported。

## 4. 精确文件集

Backup 从 `storecatalog.LoadStrict` 得到唯一最新 Manifest，并复制：

- generation 对应 slot 的 `MANIFEST-v2-{generation & 1}`；
- Manifest 中全部 sealed Data Segment 和当前 active Data Segment；
- Manifest 中全部 sealed Mapping Segment 和当前 active Mapping Segment。

只复制当前权威 slot，避免 artifact 携带引用已经退休文件的旧 Manifest。Active 文件复制到当时精确文件
长度；独占 lease 保证复制期间不会 append、repair、rotation、checkpoint 或 GC。

## 5. Backup 时序

```text
validate source/destination paths
-> acquire source LOCK
-> reject initialization/maintenance/rotation/Mapping-GC artifacts
-> run v2 exact verifier under the same held lease
-> derive exact whitelist from authoritative Manifest
-> create sibling staging + INCOMPLETE-v2; fsync parent
-> copy each regular file, fsync destination file, calculate SHA-256
-> create payload LOCK and run Offline Verify on copied payload
-> remove payload LOCK
-> write/fsync BACKUP-v2
-> fsync payload directories and staging root
-> remove INCOMPLETE-v2; fsync staging root
-> rename staging to final backup-dir; fsync parent
-> release source LOCK
```

公开 `Verify` 会自行取得锁；Backup 使用已经抽取的 caller-held-lease verifier 内核，整个 Verify、Manifest
读取和复制阶段不释放源 lease。

## 6. Restore 时序

```text
require destination absent
-> open and strictly validate BACKUP-v2
-> reject INCOMPLETE-v2, unknown files, symlinks and hash/size mismatch
-> create destination sibling staging; fsync parent
-> copy whitelisted payload files and fsync each file
-> create RESTORE-INCOMPLETE-v2, empty regular LOCK and required empty journal directory
-> fsync all child directories and staging root
-> run v2 Offline Verify against staging
-> remove RESTORE-INCOMPLETE-v2; fsync staging root
-> rename staging to destination; fsync parent
```

Restore 不调用 `Create`，也不重放业务写入；它恢复的是已经验证过的完整物理快照。发布后第一次 `Open`
仍执行正常 v2 recovery，但一个成功 artifact 不应包含任何需要恢复的 journal。

若最终 rename 成功但父目录 fsync 返回错误，结果属于 publication uncertain：调用者必须检查 destination；
存在时它必须已经是可 Verify/Open 的完整 Store，不存在时 staging 必须仍可重试，不能出现半目录。

## 7. 公开 API

```go
type BackupConfig struct {
    SourceDir string
    DestDir   string
    MappingCacheBytes uint64
    MaxLiveIDs uint64
    MaxReplayStatuses uint64
}

type BackupReport struct {
    StoreID            [16]byte
    ManifestGeneration uint64
    Files              uint64
    Bytes              uint64
}

func Backup(context.Context, BackupConfig) (BackupReport, error)

type RestoreConfig struct {
    BackupDir string
    DestDir   string
    MappingCacheBytes uint64
    MaxLiveIDs uint64
    MaxReplayStatuses uint64
}

func Restore(context.Context, RestoreConfig) (BackupReport, error)
```

CLI 只是上述库 API 的薄封装，不拥有另一套复制、校验或恢复协议。

当前 publication 使用 Linux `RENAME_NOREPLACE`，从 syscall 层保证并发出现的目标也不会被覆盖。其他平台
在没有等价原子原语时返回 `ErrUnsupported`，不退化为有 TOCTOU 窗口的 check-then-rename。

## 8. 故障与验证矩阵

Backup fault points：staging create/root sync、INCOMPLETE write/sync、每个 payload create/write/sync/close、
metadata write/sync、各层 directory sync、marker remove、final rename 和 parent sync。

Restore fault points：artifact read、每个 payload copy、LOCK/journal create、各层 sync、Verify、final rename 和
parent sync。每个错误必须保留原始 cause，且不得覆盖已有目标。

子进程退出至少覆盖：Backup staging、部分 files、metadata durable、marker removed、published；Restore
staging、部分 files、payload verified、published。成功 artifact 必须可重复 Restore 到多个新目录，但这些
恢复目录共享 StoreUUID，只能选择一个作为 writer。

## 9. Open items

1. 是否在后续提供显式 `Clone`，通过重写 StoreUUID/RecordLogID 和全部 Segment Header 产生独立身份；
2. artifact metadata 是否需要签名；第一版 SHA-256 只证明内容与 metadata 一致，不提供来源认证；
3. 是否增加流式远端 sink/source；这不应改变本地 artifact 的发布与校验边界。

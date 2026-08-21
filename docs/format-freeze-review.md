# Format v1 Freeze Review

状态：Frozen for implementation（2026-08-21）

## 1. 结论

Phase 0 的 Format v1 主路径已实现并冻结，可以进入 Phase 1。冻结范围包括：

- Data/Mapping Segment Header 与 Footer；
- Data Frame、系统 payload、Commit/Relocation Descriptor；
- SparseBitmap/Dense512 Mapping Node；
- Manifest/CURRENT；
- INITIALIZING、ROTATION、MAINTENANCE Journal；
- StoreUUID、VAddr、MapAddr、LogPos 和各序号类型的二进制表示。

冻结后，现有字段偏移、Magic、CRC 覆盖范围、required TLV、枚举值或语义的非兼容修改必须提升 major version，并提供离线迁移；不能让 `Open` 静默改写旧格式。

## 2. Review 中修复的缺口

- Manifest 现在证明 `ReplayStartLogPos` 属于同代 Data file set 且不越过有效范围；
- Manifest 现在证明非零 `MappingRootAddr` 属于同代 Mapping file set，且不能指向 valid end；
- Sealed Data/Mapping extent 不能超过 `SegmentSize - FooterSize`；
- `SegmentSize` 按文档允许精确 4 GiB，地址仍只允许可表示的 uint32 起始 offset；
- INITIALIZING/MAINTENANCE/ROTATION 的 generation、Phase 和文件引用转换保持单调；
- SparseBitmap/Dense512 和三种 Journal 均有固定 golden vector；
- 系统 payload 和 Descriptor Seal 增加独立 decoder fuzz target；
- 初始化在真实 write/fsync/rename/directory-sync 边界支持显式 failpoint，并由父进程 SIGKILL 子进程验证恢复。

## 3. 不变量与代码映射

| 不变量 | 实现位置 |
|---|---|
| 所有多字节整数为 little-endian，磁盘不序列化 Go 内存布局 | `internal/format` |
| Header/Payload CRC32C 分离，CRC 字段计算时为 0 | `internal/format/segment.go`, `frame.go`, `container.go`, `node.go` |
| VAddr/MapAddr/LogPos 类型隔离，offset 8-byte aligned | `internal/base/types.go` |
| Decoder 在分配前验证 size/count/segment bound | `internal/format` 各 decoder |
| v1 未知 flag 和非零 reserved 拒绝 | `internal/format` 各 decoder |
| Manifest/CURRENT 使用 file fsync → rename → directory fsync | `internal/manifest/install.go` |
| 初始化 UUID 在 `INITIALIZING` durable 后不再改变 | `internal/initialize/initialize.go` |
| 数据目录同一时刻只有一个 writer | `internal/filelock/filelock.go` |

## 4. 验证证据

本次冻结执行：

```text
go test ./...
go test -race ./...
go vet ./...
go test ./... -count=10
```

所有 format fuzz target 均执行短时本地 fuzz：Segment structures、Frame、system payloads、Mutation entries、Descriptor Seal、Mapping Node、Manifest 和 Journals。初始化 crash matrix 对 22 个命名边界启动真实子进程，由父进程收到边界通知后直接 SIGKILL，再以新 `Open` 验证同一 StoreUUID 和 generation-1 Manifest。

## 5. 验证边界

- 当前 crash matrix 提供 process-crash evidence，不宣称模拟掉电后设备 cache 行为；
- 当前 fuzz 是短时开发门禁，长期 fuzz 属于 Phase 5/nightly；
- Active tail、Sealed strict scan、Commit recovery 和 rotation 的端到端验证属于 Phase 1/2；codec 已冻结不等于这些协议已经完成；
- Format v1 冻结不冻结 runtime 调度、cache、group commit 和 GC 策略。

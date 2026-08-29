# Format Upgrade 与 Migration

状态：v2 只读 migration planner 已实现；当前没有迁移执行 step。

## 1. 兼容规则

磁盘格式版本为 `(major, minor)`：

- decoder 只接受当前精确的 `(major, minor)`；更旧或更新的版本均返回
  `ErrUnsupported`，直到 registry 提供显式迁移路径；
- major 改变表示必须显式迁移，不能由 Open 猜测；
- unknown required TLV、flag 或 reserved bit 仍是 unsupported/corruption，不能因
  “尝试兼容”而忽略。

普通 `ridstore.Open` 永远不执行格式迁移。

## 2. 只读 plan

```text
go run ./cmd/ridstore-tool migrate plan --dir /path/to/offline-store
```

planner 先取得既有 `LOCK`，严格读取两个 `MANIFEST-v2-{0,1}` 槽并选择最高 generation。
Header 检查 magic、header/payload CRC、slot generation、StoreUUID、payload length，但允许报告当前 decoder
不支持的版本。若磁盘已经是当前版本，planner 在同一 lease 下运行完整 Verify，
输出 `verified_current=true`；这是真正的 no-op，不改写任何字节。

若 registry 中不存在从 source 到当前版本的严格递增连续路径，plan 连同
`ErrUnsupported` 返回。当前 registry 为空：开发期 v1 没有生产数据，不迁移、不兼容；任何非当前
v2.1 格式都明确不支持。v2.1 增加与 Mapping Root 原子安装的精确
`MappingEntryCount`；开发期没有生产数据，因此不为 v2.0 提供迁移 step。planner 的存在不等于已经具备跨版本升级能力。

## 3. 未来 step 契约

每个 migration step 必须声明唯一的 `name/from/to`，registry 不允许同一 source
有两个隐式分支，也不允许缺口或循环。增加真实 step 前必须单独冻结：

1. source decoder 与 corruption 边界；
2. destination 格式、UUID 策略和资源上界；
3. Record/ID/VAddr/CommitSeq/Batch atomicity 保持证明；
4. crash matrix、golden fixture 和 rollback 策略；
5. Backup/Restore 演练以及旧 binary 对新格式的明确拒绝。

执行模型只能是 copy-on-write：读取离线 source，写入一个带 migration marker 的
全新目录，完整 Verify 后发布 destination。不得原地覆盖 Segment、Mapping Node、
Manifest 或 CURRENT；失败时 source 保持只读可用，destination 保持明确未完成。

## 4. 发布门禁

支持新 minor/major 之前必须同时提交 decoder、migration step、跨版本 fixture、
升级/中断/重试测试和文档。只提升 `FormatMinorVersion`、只让 decoder 接受新值，
或依赖 Backup 的 UUID rewrite 都不构成 migration。

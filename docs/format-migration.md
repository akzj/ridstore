# Format Upgrade 与 Migration

状态：Format v1 历史设计；v2 migration planner 尚未实现

以下内容保留为 v2 planner 的设计输入；其中命令、registry 和实现路径在当前仓库中不存在。

## 1. 兼容规则

磁盘格式版本为 `(major, minor)`：

- decoder 只接受相同 major；
- 当前实现可以读取 `minor <= current minor`，未知更高 minor 返回
  `ErrUnsupported`；
- major 改变表示必须显式迁移，不能由 Open 猜测；
- unknown required TLV、flag 或 reserved bit 仍是 unsupported/corruption，不能因
  “尝试兼容”而忽略。

普通 `ridstore.Open` 永远不执行格式迁移。

## 2. 只读 plan

```text
go run ./cmd/ridstore-tool migrate plan --dir /path/to/offline-store
```

planner 先取得既有 `LOCK`，只读取 CURRENT 指向的 Manifest fixed header。Header
检查 magic、CRC、generation、StoreUUID、payload length，但允许报告当前 decoder
不支持的版本。若磁盘已经是当前版本，planner 在同一 lease 下运行完整 Verify，
输出 `verified_current=true`；这是真正的 no-op，不改写任何字节。

若 registry 中不存在从 source 到当前版本的连续路径，plan 连同
`ErrUnsupported` 返回。当前 v1.0 没有历史 migration step，因此任何非 v1.0
格式都明确不支持；skeleton 的存在不等于已经具备跨版本升级能力。

## 3. 未来 step 契约

每个 migration step 必须声明唯一的 `name/from/to`，registry 不允许同一 source
有两个隐式分支，也不允许缺口或循环。增加真实 step 前必须单独冻结：

1. source decoder 与 corruption 边界；
2. destination 格式、UUID 策略和资源上界；
3. Record/ID/Revision/CommitSeq/Batch atomicity 保持证明；
4. crash matrix、golden fixture 和 rollback 策略；
5. Backup/Restore 演练以及旧 binary 对新格式的明确拒绝。

执行模型只能是 copy-on-write：读取离线 source，写入一个带 migration marker 的
全新目录，完整 Verify 后发布 destination。不得原地覆盖 Segment、Mapping Node、
Manifest 或 CURRENT；失败时 source 保持只读可用，destination 保持明确未完成。

## 4. 发布门禁

支持新 minor/major 之前必须同时提交 decoder、migration step、跨版本 fixture、
升级/中断/重试测试和文档。只提升 `FormatMinorVersion`、只让 decoder 接受新值，
或依赖 Backup 的 UUID rewrite 都不构成 migration。

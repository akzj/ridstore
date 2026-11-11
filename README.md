# ridstore

ridstore 是一个嵌入式、单机、单目录独占的 Stable-ID Log-Structured Record Store Library：

```text
uint64 ID -> variable-length bytes
```

当前 `v2` 分支只有一条运行路径。根包 API 直接建立在 v2 Engine 上；Format v1 runtime、adapter、
dual-write、旧 CLI 与旧内部模块均已删除。

核心语义：

- ID 稳定、永不复用；更新只追加新 Record，删除只改变 Mapping；
- Batch 内多个 Put/Delete 原子发布，durable commit 由唯一 Coordinator 排序；
- `Get` 返回不可拆解的 `VersionToken`，条件写只比较当前 Mapping 地址；
- token 绑定持久化 Store identity，同一 Store 重开后仍有效，跨 Store token 被拒绝；
- GC relocation 可能让旧 token 安全冲突，但不会改变 Value；
- Data GC 具有独立 batch、空间 admission 和复制速率预算，空间不足不会越过 source 退休门禁；
- 不提供业务 Revision、MVCC、Snapshot、KV 排序、RPC、复制或分布式能力。

最小使用方式：

```go
store, err := ridstore.Create(ctx, ridstore.CreateConfig{Dir: path})
if err != nil { /* handle */ }
defer store.Close()

batch, err := store.Begin(ctx)
if err != nil { /* handle */ }
id, err := batch.Create(ctx, value)
if err != nil { /* abort or handle */ }
result, err := batch.Commit(ctx)
_ = id
_ = result
```

离线审计使用 `ridstore.Verify(ctx, ridstore.VerifyConfig{Dir: path})`；它要求 Store 已关闭并取得目录
独占锁，不执行恢复或修复。可用 `ridstore.Backup` / `ridstore.Restore` 创建和恢复 v2 全量灾备
artifact；Restore 保留 Store identity，不能把原目录和恢复目录同时作为 writer。`Store.Metrics()` 提供
有界 v2 runtime 快照，`metrics/prometheus` 提供无第三方依赖的导出适配器。设计入口见
[v2 文档索引](docs/README.md)。项目尚未声明 production-ready；未来格式的实际 Migration step、
long-fuzz/72h soak 自然结束证据、部署侧监控验收与真实 workload 容量基线仍未完成。

ridstore 的交付形态类似 RocksDB：由应用直接链接并独占本地目录；但它不是 RocksDB/LevelDB 的功能替代品。

## 平台支持

| 平台 | 构建 | 测试 | 原子 no-replace 目录发布 | 目录 fsync | soak harness |
| --- | --- | --- | --- | --- | --- |
| linux/amd64 | 已验证 | 已验证（本机全量 `go test ./...` 通过） | `renameat2(RENAME_NOREPLACE)` | 真实 fsync | 可运行 |
| darwin/arm64 | 已验证（交叉编译 + `go vet`） | 未在真机运行（无 macOS 主机） | `renamex_np(RENAME_EXCL)` | 真实 fsync | 可运行 |
| windows/amd64 | 已验证 | 已验证（本机全量 `go test ./...` 通过） | `MoveFileEx`（不带 REPLACE 标志，见下） | **no-op**（见下） | **拒绝启动** |

Windows 上的实现要点与明确降级：

- **文件句柄共享删除**：`internal/fileutil` 在 Windows 用 `CreateFile` 打开文件时请求
  `FILE_SHARE_DELETE`，因此"写临时文件 → fsync → rename 就位 → 继续用同一句柄追加"这条发布协议
  在 Windows 上与 POSIX 行为一致。Go 自带的 `os.OpenFile` 不带该位，rename 会失败。
- **Backup/Restore 发布**：`internal/backuprestore` 在 Windows 用不带 `MOVEFILE_REPLACE_EXISTING` 的
  `MoveFileEx` 实现原子 no-replace rename，与 Linux `RENAME_NOREPLACE` / macOS `RENAME_EXCL` 语义等价；
  artifact 内路径始终是 `/` 分隔的归档路径，与创建/读取它的平台无关。没有等价原语的平台仍返回
  `ErrUnsupported`，不会退化成"先检查再 rename"。
- **目录 fsync 降级**：Windows 上 `internal/dirsync` 的目录刷新是 no-op，因为 `FlushFileBuffers`
  对目录句柄返回 `ERROR_ACCESS_DENIED`，Windows 也没有等价的目录持久化 API。这意味着 Windows 上
  rename 之后的目录项持久性依赖 NTFS 元数据日志；文件内容本身的 fsync 不受影响，语义与 POSIX 不等价，
  这是唯一被明确接受的降级。
- **soak harness**：`internal/soak` 的报告依赖 POSIX 的 `statfs`、`Stat_t.Blocks`、`/proc/self/fd`
  与 `/proc/self/statm`。非 linux/darwin 平台仍可编译，但 harness 启动即返回 `ErrUnsupported`，
  不会输出编造的文件系统类型、块大小、描述符数与 RSS。
- **symlink 相关用例**：Windows 非管理员且未开启开发者模式时无法创建 symlink，这类测试会跳过并
  说明原因，而不是失败（symlink 攻击面的检查逻辑本身仍在代码中，POSIX 上有覆盖）。
- **阶段计时断言**：Windows 单调时钟以约 100ns 的步进推进，快于一个步进的阶段会合法地记录为 0；
  相关测试先探测主机时钟粒度，粒度大于 1ns 时只校验计数类指标。

`.github/workflows/ci.yml` 在 linux / windows / macos 三个 runner 上执行 `go build ./...`、
`go vet ./...` 与 `go test ./...`，保证上述矩阵不是一次性结论。

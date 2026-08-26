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

设计入口见 [v2 文档索引](docs/README.md)。项目尚未声明 production-ready；离线 Verify、Backup、
迁移、可观测性、长期 soak 与容量/故障验证需要在 v2 语义上重新实现和验收。

ridstore 的交付形态类似 RocksDB：由应用直接链接并独占本地目录；但它不是 RocksDB/LevelDB 的功能替代品。

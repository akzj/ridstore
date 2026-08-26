# ridstore v2 M6 Review

状态：Implemented, pending owner review

## 1. 结论

M6 已完成代码层面的单路径切换：根包公开 API 直接包装 `internal/engine`，仓库不再编译或保留
Format v1 runtime。没有 adapter、fallback、dual-write 或格式自动探测。

这意味着“v1 清理完成”，不意味着“production-ready”。被删除的 Backup、Verify、Metrics、Soak 和
离线 CLI 必须以后按 v2 格式重新设计；它们不是继续保留 v1 的理由。

## 2. 公开契约

公开层保留 Stable ID、BatchID、CommitSeq、原子 Batch、Get、Checkpoint、Status 与有界单 Segment GC。
删除 LogicalRevision，新增不可拆解的 `VersionToken`：

```text
VersionToken = persistent Store identity + current Mapping VAddr
```

- 同一 Store Close/Open 后，Mapping 未变化时 token 仍可用于条件提交；
- 跨 Store、零值或非法地址 token 返回 `ErrInvalidToken`；
- 用户更新或 GC relocation 改变 VAddr 后，旧 token 在提交点返回 `ErrConflict`；
- token 不增加磁盘字段，不读取 Record Header，不提供排序或业务版本语义。

Create 与 Open 使用不同配置类型。HardLimits 只在 Create 提供并持久化；Open 只接受 runtime budgets，
避免调用者把磁盘事实重复配置成第二个真相来源。

## 3. 删除边界

已删除的 v1 所有者包括：

- 根 `Store`/`Batch` runtime、Revision API、旧 GC、metrics 和对应测试；
- appendlog、allocator、batch、commit、catalog、manifest、format；
- v1 mapping api/memory/radix、segment、rotation、recovery、initialize；
- v1 backup、verify、migration、soak、Prometheus adapter 和 CLI；
- 只服务于旧路径的 diskspace、failpoint、metrics 与旧 base 地址类型。

当前生产依赖图只包含根包和这些内部模块：

```text
base bootstrap coordinator engine filelock idalloc maintstate mapping
mapstore model radix recordcodec recordlog replay segmentstats
storecatalog transaction
```

其中 `internal/radix` 是 v2 immutable Mapping tree；它与已删除的 `internal/mapping/radix` 不是并存实现。

## 4. 验证证据

M6 切换时执行并通过：

```text
go test ./...
go vet ./...
go test -race ./...
make test-crash
make test-fuzz-smoke FUZZ_TIME=1s FUZZ_PARALLEL=2
```

公开契约测试覆盖 Create/Open、CRUD、Checkpoint、Batch Status、token 跨重开、stale token 冲突、
零 token 和跨 Store token 拒绝。v2 Engine 原有测试继续覆盖 CommitUnknown、Replay、Mapping、GC 与维护恢复。

## 5. 未改变与后续缺口

未改变：durable-before-publish、唯一 Coordinator、ID 不复用、Mapping revalidation、Checkpoint cut、
relocation CAS、Reader Pin 和退休前精确证明。

后续优先级：

1. 按 `v2-verify-design.md` 实现 Offline Verify；
2. 按需重新设计 Backup、Metrics 与长时 soak；
3. 在真实 workload 上建立吞吐、写放大、读放大和 GC 收敛基线。

M6 后的第一个完整性迭代已经为 terminal Batch Status 增加有界保留、admission 驱动的 Checkpoint 和
Replay 容量门禁；CommitUnknown 的恢复查询语义保持不变。

第二个完整性迭代增加了 v2 原生用户写入磁盘水位。它只阻止新的 Put Record，控制记录、已接收
Batch 的 Commit、Checkpoint 和 GC relocation 可使用保留 headroom；真实 ENOSPC 仍由底层 fail-closed。

公开层并发模型测试现在覆盖同一 token 的多 Batch CAS、并发 Checkpoint、唯一 winner、连续 CommitSeq、
终态 Status 和 Close/Open 后最终值恢复，并纳入完整 `go test -race ./...` 门禁。

公开层子进程退出测试覆盖 uncommitted Put、Checkpoint 时仍开放的 Batch、committed tail 和已 Checkpoint
的 commit；fresh Open 分别验证 Aborted、Committed、StatusExpired 与最终 Mapping 可见性。

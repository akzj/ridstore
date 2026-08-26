# ridstore v2 M1 Review

> 历史阶段快照：其中关于“旧 runtime 仍存在”的陈述已被 M6 取代，当前边界见
> [v2 M6 Review](v2-m6-review.md)。

状态：Implemented, pending architecture review

提交：

- `d70a937 feat: add record log v2 format`
- `4335478 feat: add ridstore v2 record protocol`
- `ff0ffb7 feat: add manifest v2 catalog`

## 1. 本阶段完成内容

M1 只实现不依赖旧运行路径的基础层：

```text
internal/model
  ID / BatchID / CommitSeq / MapAddr

internal/recordlog
  VAddr / LogPos / AppendResult
  Segment Header / Record Envelope / Segment Footer

internal/recordcodec
  Put / CommitGroup / Abort / Reserve / Checkpoint

internal/storecatalog
  Manifest v2 codec
  dual-slot atomic install/load
  typed Catalog mutations
```

没有修改旧 runtime，也没有建立旧、新路径 adapter。当前用户 API 仍由 Format v1 路径运行。

## 2. 已落实的架构变化

- VAddr 低三位是 size tag，LogPos 使用独立无标签类型；
- RecordLog envelope 不包含 BatchID、RecordID 或 CommitSeq；
- 一个 CommitGroupRecord 自包含多个完整 Batch Descriptor；
- CommitPart/CommitSeal 和 FrameSeq 不进入 v2；
- Manifest v2 是 Data、Mapping、Checkpoint 和 GC 文件集的唯一 Catalog；
- Catalog 不暴露任意 manifest mutation closure；
- DataRetire 要求当前 checkpoint 的精确零存活统计，并限制 ReplayStart 只能做等价规范化；
- Format v2 decoder 不接受 Format v1。

## 3. 代码复用结果

本阶段没有修改旧 `internal/base`、`internal/format`、`internal/manifest` 或 `internal/catalog`。

RecordLog 使用 size tag、32-byte Record Header、CRC32C、
64-byte Segment Header/Footer。代码在最终职责包中重新生成，没有调用原型 API。

## 4. 安全边界

已经覆盖：

- checked size arithmetic；
- 解码前 count/length 限制；
- VAddr tag、offset、PhysicalSize 一致性；
- reserved byte 和 padding 必须为零；
- CommitSeq 连续、BatchID 唯一、mutation ID 排序；
- UserCommit/Relocation 操作形状；
- Manifest 文件集、Root、ReplayStart、Stats 和 HardLimits 交叉校验；
- typed Catalog generation compare-and-install；
- Manifest file sync、rename 和 directory sync 顺序；
- failed install 不更新进程内 current Manifest。

## 5. 本阶段明确未实现

- RecordLog writer、queue、buffer 和 fsync；
- Segment 创建、读取、rotation 和 Registry；
- v2 Mapping 实现；
- v2 Store/Batch/Coordinator；
- Checkpoint 和 Recovery 执行路径；
- GC 和旧代码删除。

因此本阶段不改变 ridstore 的运行行为，也不能进行 v2 性能结论。

## 6. 验证证据

已执行：

```text
go test -count=1 ./...
go test -race -count=1 ./internal/recordlog
go test -race -count=1 ./internal/recordcodec
go test -race -count=1 ./internal/storecatalog
go vet ./...
go test ./internal/storecatalog -run '^$' -fuzz=FuzzDecodeManifest -fuzztime=2s
git diff --check
```

测试包含 round-trip、golden、boundary、corruption、fault injection 和 decoder fuzz seed。

## 7. 进入 M2 前 Review 项

1. `Append` 是否保持同步统一接口，不再引入 Receipt/AppendGroup；
2. CommitGroupRecord 的 40-byte Descriptor + 32-byte Mutation 是否接受；
3. `MaxRecordLogPayload` 是否由最大 Put/单 Batch Descriptor 推导；
4. Manifest v2 的 checkpoint tuple 是否遗漏必须原子保存的状态；
5. Catalog typed mutation 是否足够表达 rotation/checkpoint/retire；
6. DataRetire 的零存活统计和 ReplayStart 规范化条件是否完整；
7. MapAddr 暂定的 32-bit SegmentID + 32-bit aligned offset 是否继续保留。

Review 通过后进入 M2：实现唯一 writer、buffer、水位、Segment、Registry、rotation 和物理恢复。

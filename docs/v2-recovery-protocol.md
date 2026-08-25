# ridstore v2 Recovery Protocol

状态：Draft for Review

本文只定义 v2 的权威状态、恢复顺序和崩溃判定。它不兼容 Format v1。

## 1. 权威状态

恢复只信任三类 durable 证据：

1. Catalog 安装完成的最新合法 Manifest；
2. Manifest 引用的 Mapping Root；
3. RecordLog 中位于 checkpoint cut 之后的完整、CRC 合法 Record。

目录中文件名、文件长度、未安装的新 Root 和未完成 journal 都只是恢复输入，不自动成为权威状态。

恢复目标是：

```text
Visible Mapping = Checkpoint Mapping Root
                + durable Commit/Relocation records after cut
```

没有 durable CommitGroupRecord 引用的 PutRecord 永远不可见。

## 2. Open 顺序

```text
1. 获取目录独占锁
2. 读取并校验最新 Manifest
3. 恢复或回滚未完成的 Catalog/rotation/maintenance journal
4. 按 Manifest 打开 RecordLog 文件集
5. 校验 sealed Footer、Segment chain 和 active Header
6. 扫描 active tail，截断最后一个 torn/incomplete Record
7. 打开 Manifest 指向的 Mapping Root
8. 从 checkpoint cut 扫描 ridstore protocol records
9. 验证并按 CommitSeq 重放 CommitGroup/Relocation
10. 恢复 allocator high watermarks 和 Batch 状态
11. 构造新的 active writer，最后对外发布 Store
```

任何一步失败都不能返回半打开、可写的 Store。

## 3. RecordLog tail

RecordLog 只接受从 Segment Header 开始连续出现的合法 Record 前缀。扫描每条 Record 时校验：

- VAddr SegmentID 和真实文件一致；
- VAddr offset 等于当前扫描 offset；
- size tag 与 PhysicalSize 一致；
- PhysicalSize 对齐且不越过 Segment 边界；
- envelope header 和 payload CRC；
- 前后 Record 地址严格单调。

active 文件最后一个不完整 Record 可以被截断。sealed 文件的任何不一致都是 corruption，不能修剪
后继续运行。位于最后 durable fsync 之后但恰好完整的 active Record可能保留；它是否产生业务状态
仍由 ridstore CommitGroupRecord 判断。

## 4. Put 和 Commit 崩溃时间线

### 4.1 Put 已预留但尚未 write

进程崩溃后内存 pending 消失，active tail 不包含该 Record。其 VAddr 可以在新 incarnation 中被
重新使用，因为它从未被 durable Commit 发布。上层不得在 Commit durable 前将地址暴露为持久状态。

### 4.2 Put 已 write 但未 Commit

恢复能够扫描到 PutRecord，但没有引用它的合法 CommitGroupRecord。它是 orphan，只占空间，不进入
Mapping，后续由 GC 回收。

### 4.3 Commit write 完整但 fsync 结果未知

调用者收到 CommitUnknown，Store fail-closed。重新 Open 后：

- 若 CommitGroupRecord 位于合法 durable 前缀，按 CommitSeq 重放并报告 Committed；
- 若不存在完整 CommitGroupRecord，报告 Aborted/NotCommitted；
- 不允许仅凭调用者超时推断结果。

### 4.4 Commit durable、Mapping 尚未发布

重启从 checkpoint cut 扫描到 CommitGroupRecord并发布全部 mutations，因此结果仍为 Committed。

### 4.5 Mapping 已发布、调用者尚未收到结果

结果同样为 Committed。响应不是 durable 证据，CommitGroupRecord 才是。

## 5. CommitGroupRecord 重放规则

每个 CommitGroupRecord 包含一个或多个完整 Batch Descriptor。每个 Descriptor 必须自包含：

- BatchID 和 CommitSeq；
- mutation 数量及有界长度；
- 按 ID 排序且 ID 唯一的最终 mutations；
- Put 的 VAddr 或 Delete 标记；
- descriptor semantic checksum/hash。

group 内 Descriptor 按 CommitSeq 严格递增，相邻 group 也必须连续。Recovery 要求 CommitSeq 从
checkpoint covered sequence 之后严格连续。重复、倒退、空洞、非法
VAddr、指向非 PutRecord、RecordID/OriginBatch 不匹配均视为 corruption。

历史条件不重放。只有运行时验证成功的 Batch 才能进入 CommitGroupRecord，因此 Recovery 直接应用
durable descriptor。

## 6. Checkpoint 时间线

Checkpoint 必须形成一个不可拼接的快照：

```text
1. Coordinator 建立 publication fence
2. 取得 covered CommitSeq
3. RecordLog 完成对应 durable cut
4. freeze Mapping Delta
5. 写并 fsync 新 Mapping nodes/root
6. 构造包含 root、covered CommitSeq、cut 的 Manifest
7. 原子安装并 fsync Manifest + directory
8. 解除 publication fence
```

崩溃在第 7 步之前：旧 Manifest 仍权威，新 Mapping 文件是 orphan。

崩溃在第 7 步之后：新 Root 和 cut 同时生效，从新 cut 继续重放。Manifest 不允许引用未 durable 的
Mapping Root，也不允许 cut 越过 covered CommitSeq。

## 7. Rotation 时间线

RecordLog rotation 必须处于单 writer 顺序内：

```text
1. 确认下一 Record 无法放入 active
2. flush/sync 当前 pending 前缀
3. 写 rotation journal PREPARED
4. seal 并 sync old active
5. 创建并 sync new active
6. Catalog 安装 sealed old + active new
7. Registry 原子切换 active
8. 删除 rotation journal
9. 为原请求分配新 Segment VAddr
```

原请求在步骤 9 前没有地址，因此 rotation 失败不能移动已经返回的 VAddr。

恢复根据 Manifest 与 journal phase完成或回滚。目录中额外存在的完整文件不自动进入 live set；
Catalog 是成员关系的唯一权威。

## 8. GC 与删除时间线

GC 的统计只选择候选。对候选 Segment 扫描时，每个 PutRecord 的最终判断是：

```text
live iff Mapping[ID] == scanned VAddr
```

存活 Record 通过 RelocationRecord 形成带 expected-old-VAddr 的 CAS mutation。安全顺序：

```text
1. Registry 标记 Cleaning，阻止新的 open-batch 引用
2. 扫描并复制仍存活 Record
3. durable RelocationRecord
4. Mapping CAS 发布
5. Checkpoint 覆盖 relocation
6. Registry 进入 Retiring，阻止新 reader
7. 等待 Reader Pin 和 open-batch refs 清零
8. Catalog Manifest 移除源 Segment；失败则撤销 Retiring
9. Registry detach 并关闭文件
10. rename 到 trash 并 fsync directory
11. 删除 trash 文件并 fsync directory
```

第 8 步前不能删除文件。第 8 步后旧文件即使因崩溃仍残留，也只是 orphan，恢复不得重新接纳。

## 9. Reserve 与 ID 不复用

IDReserveRecord 和 BatchIDReserveRecord 使用 `sync=true`。只有 Append 成功后才能发放
新区间。结果不确定时 Store fail-closed；恢复取 checkpoint 与 replay 中最大的合法 high watermark。

允许跳过一段 ID，绝不允许回退或复用。

## 10. Fail-closed 条件

以下情况必须停止写入并要求重新 Open 或离线修复：

- write/short write/fsync 结果使物理尾部不确定；
- CommitSeq 不连续；
- VAddr 与真实 Segment/offset/size 不一致；
- Mapping 指向错误 RecordID 或非 PutRecord；
- Manifest 引用缺失或未 durable 文件；
- rotation/maintenance journal 无法确定完成或回滚；
- durable Commit 后 Mapping publication 失败。

不能通过跳过损坏 Record、猜测最新文件或继续复用 active tail 来保持服务。

## 11. 必须验证的崩溃点

- Append 地址预留前后；
- 地址预留后、write 前；
- partial write；
- write 完成、fsync 前后；
- rotation journal 每个 phase；
- Manifest rename 和 directory fsync 前后；
- Mapping Root fsync 和 Manifest 安装之间；
- Relocation durable、Checkpoint、Manifest remove 和文件删除之间；
- Close 与并发 Append；
- Append 等待者取消和 writer poison。

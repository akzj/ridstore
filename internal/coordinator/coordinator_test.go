package coordinator

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/akzj/ridstore/internal/base"
	"github.com/akzj/ridstore/internal/mapping"
	"github.com/akzj/ridstore/internal/mapstore"
	"github.com/akzj/ridstore/internal/model"
	"github.com/akzj/ridstore/internal/radix"
	"github.com/akzj/ridstore/internal/recordcodec"
	"github.com/akzj/ridstore/internal/recordlog"
	"github.com/akzj/ridstore/internal/transaction"
)

type fakeAllocator struct{}

func (fakeAllocator) Allocate(context.Context) (uint64, error) { return 1, nil }
func (fakeAllocator) CanUse(uint64) bool                       { return true }

func resultRef(t *testing.T, result recordlog.AppendResult) recordlog.RecordRef {
	t.Helper()
	ref, err := result.Ref()
	if err != nil {
		t.Fatal(err)
	}
	return ref
}

func addrRef(t *testing.T, addr recordlog.VAddr) recordlog.RecordRef {
	t.Helper()
	size, err := addr.ReadHint()
	if err != nil {
		t.Fatal(err)
	}
	ref, err := recordlog.NewRecordRef(addr, size)
	if err != nil {
		t.Fatal(err)
	}
	return ref
}

type fakeLog struct {
	mu          sync.Mutex
	nextOffset  uint32
	groups      []recordcodec.CommitGroup
	records     []recordcodec.RecordType
	putAddrs    []recordlog.VAddr
	checkpoints []recordcodec.CheckpointMarker
	syncErr     error
	poisoned    bool
	firstSync   chan struct{}
	releaseSync chan struct{}
	once        sync.Once
}

func (l *fakeLog) Append(_ context.Context, payload []byte, syncWrite bool) (recordlog.AppendResult, error) {
	physical, err := recordlog.PhysicalRecordSize(uint64(len(payload)))
	if err != nil {
		return recordlog.AppendResult{}, err
	}
	typ, err := recordcodec.TypeOf(payload)
	if err != nil {
		return recordlog.AppendResult{}, err
	}
	l.mu.Lock()
	if l.nextOffset == 0 {
		l.nextOffset = recordlog.SegmentHeaderSize
	}
	addr, err := recordlog.NewVAddr(1, l.nextOffset, physical)
	if err != nil {
		l.mu.Unlock()
		return recordlog.AppendResult{}, err
	}
	l.nextOffset += physical
	l.records = append(l.records, typ)
	if typ == recordcodec.RecordTypePut {
		l.putAddrs = append(l.putAddrs, addr)
	}
	if syncWrite {
		switch typ {
		case recordcodec.RecordTypeCommitGroup:
			group, decodeErr := recordcodec.DecodeCommitGroup(payload, 1<<20, 128, 1024)
			if decodeErr != nil {
				l.mu.Unlock()
				return recordlog.AppendResult{}, decodeErr
			}
			l.groups = append(l.groups, group)
		case recordcodec.RecordTypeCheckpoint:
			checkpoint, decodeErr := recordcodec.DecodeCheckpoint(payload)
			if decodeErr != nil {
				l.mu.Unlock()
				return recordlog.AppendResult{}, decodeErr
			}
			l.checkpoints = append(l.checkpoints, checkpoint)
		}
	}
	wantErr, poisoned := l.syncErr, l.poisoned
	l.mu.Unlock()
	if syncWrite && l.firstSync != nil {
		l.once.Do(func() {
			close(l.firstSync)
			<-l.releaseSync
		})
	}
	if syncWrite && wantErr != nil {
		l.mu.Lock()
		l.poisoned = poisoned
		l.mu.Unlock()
		return recordlog.AppendResult{}, wantErr
	}
	return recordlog.NewAppendResult(addr, physical)
}

func (l *fakeLog) snapshotRecords() ([]recordcodec.RecordType, []recordcodec.CheckpointMarker) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]recordcodec.RecordType(nil), l.records...), append([]recordcodec.CheckpointMarker(nil), l.checkpoints...)
}

func (l *fakeLog) latestPutAddr() recordlog.VAddr {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.putAddrs[len(l.putAddrs)-1]
}

func (l *fakeLog) Status() recordlog.Status {
	l.mu.Lock()
	defer l.mu.Unlock()
	return recordlog.Status{Poisoned: l.poisoned}
}

func (l *fakeLog) snapshotGroups() []recordcodec.CommitGroup {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]recordcodec.CommitGroup(nil), l.groups...)
}

func newBatch(t *testing.T, id model.BatchID, log *fakeLog) *transaction.Batch {
	t.Helper()
	b, err := transaction.New(id, transaction.Limits{
		MaxValueSize: 1024, MaxBatchBytes: 4096, MaxBatchMutations: 16, MaxBatchConditions: 16,
	}, log, fakeAllocator{})
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func newCoordinator(t *testing.T, log *fakeLog, current mapping.Index) *Coordinator {
	t.Helper()
	c, err := New(current.CoveredCommitSeq()+1, log, current, Config{QueueCapacity: 16, MaxGroupBatches: 8, MaxGroupPayload: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := c.Close(); err != nil && !errors.Is(err, base.ErrClosed) {
			t.Errorf("close: %v", err)
		}
	})
	return c
}

type coordinatorNodeStore struct{}

func (coordinatorNodeStore) Read(model.MapAddr) (mapstore.Node, error) {
	return mapstore.Node{}, mapping.ErrCorrupt
}

func (coordinatorNodeStore) Append(uint8, uint64, model.CommitSeq, [mapstore.NodeSlots]uint64) (model.MapAddr, error) {
	return 0, mapping.ErrCorrupt
}

func (coordinatorNodeStore) AppendLeaf(uint64, model.CommitSeq, [mapstore.NodeSlots]recordlog.RecordRef) (model.MapAddr, error) {
	return 0, mapping.ErrCorrupt
}

func (coordinatorNodeStore) Sync() error { return nil }

func newPersistentMapping(t *testing.T) *mapping.Persistent {
	t.Helper()
	nodes := coordinatorNodeStore{}
	tree, err := radix.Open(nodes, 0, 0, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	current, err := mapping.OpenPersistent(tree, nodes, mapping.PersistentConfig{
		CheckpointSortBytes: 384, DeltaSoftLimitBytes: 512, DeltaHardLimitBytes: 1024,
	}, mapping.PersistentState{})
	if err != nil {
		t.Fatal(err)
	}
	return current
}

func TestCommitWritesDurableDescriptorBeforeMappingPublish(t *testing.T) {
	log := &fakeLog{}
	current := mapping.NewEmpty()
	c := newCoordinator(t, log, current)
	b := newBatch(t, 7, log)
	if err := b.Put(context.Background(), 3, []byte("value")); err != nil {
		t.Fatal(err)
	}
	result, err := c.Commit(context.Background(), b)
	if err != nil || result != (Result{BatchID: 7, CommitSeq: 1}) {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	addr, exists, err := current.Lookup(3)
	if err != nil || !exists || addr == 0 {
		t.Fatalf("addr=%+v exists=%v err=%v", addr, exists, err)
	}
	groups := log.snapshotGroups()
	if len(groups) != 1 || len(groups[0].Descriptors) != 1 || groups[0].Descriptors[0].BatchID != 7 || groups[0].Descriptors[0].CommitSeq != 1 {
		t.Fatalf("groups=%+v", groups)
	}
}

func TestSubmitPropagatesDeltaSoftPressure(t *testing.T) {
	log := &fakeLog{}
	current := newPersistentMapping(t)
	c := newCoordinator(t, log, current)
	batch := newBatch(t, 7, log)
	for id := model.ID(1); id <= 8; id++ {
		if err := batch.Put(context.Background(), id, []byte("value")); err != nil {
			t.Fatal(err)
		}
	}
	receipt, err := c.Submit(context.Background(), batch)
	if err != nil || !receipt.DeltaPressure() || receipt.DeltaPressureGeneration() == 0 {
		t.Fatalf("pressure=%v generation=%d err=%v", receipt.DeltaPressure(), receipt.DeltaPressureGeneration(), err)
	}
	if _, err := receipt.Wait(); err != nil {
		t.Fatal(err)
	}
}

func TestCheckpointFenceStopsAdmissionOnlyUntilRelease(t *testing.T) {
	log := &fakeLog{firstSync: make(chan struct{}), releaseSync: make(chan struct{})}
	current := mapping.NewEmpty()
	c := newCoordinator(t, log, current)

	fenceResult := make(chan struct {
		fence *CheckpointFence
		err   error
	}, 1)
	go func() {
		fence, err := c.AcquireCheckpointFence(context.Background())
		fenceResult <- struct {
			fence *CheckpointFence
			err   error
		}{fence: fence, err: err}
	}()
	select {
	case <-log.firstSync:
	case <-time.After(time.Second):
		t.Fatal("checkpoint marker did not reach durable append")
	}

	batch := newBatch(t, 7, log)
	if err := batch.Put(context.Background(), 3, []byte("value")); err != nil {
		t.Fatal(err)
	}
	submitDone := make(chan error, 1)
	go func() {
		receipt, err := c.Submit(context.Background(), batch)
		if err == nil {
			_, err = receipt.Wait()
		}
		submitDone <- err
	}()
	select {
	case err := <-submitDone:
		t.Fatalf("submit crossed checkpoint admission fence: %v", err)
	case <-time.After(20 * time.Millisecond):
	}

	close(log.releaseSync)
	answer := <-fenceResult
	if answer.err != nil {
		t.Fatal(answer.err)
	}
	select {
	case err := <-submitDone:
		t.Fatalf("submit crossed held checkpoint fence: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	answer.fence.Release()
	select {
	case err := <-submitDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("submit did not resume after checkpoint fence release")
	}
	metrics := c.Metrics()
	if metrics.CheckpointFences != 1 || metrics.CheckpointFenceAcquireNanos == 0 || metrics.CheckpointFenceHeldNanos == 0 || metrics.CheckpointFenceMaxHeldNanos == 0 ||
		metrics.CheckpointFenceMaxHeldNanos > metrics.CheckpointFenceHeldNanos {
		t.Fatalf("checkpoint fence metrics=%+v", metrics)
	}
}

func TestCommitRedirectsRewriteUserDescriptorWithoutCheckpointMarker(t *testing.T) {
	log := &fakeLog{}
	current := mapping.NewEmpty()
	c := newCoordinator(t, log, current)
	batch := newBatch(t, 1, log)
	if err := batch.Put(context.Background(), 7, []byte("pending")); err != nil {
		t.Fatal(err)
	}
	oldRef := batch.PendingPutRefs()[0]
	prepared, err := batch.Prepare()
	if err != nil {
		t.Fatal(err)
	}
	reservation, _, err := current.ReserveDelta([]model.ID{7})
	if err != nil {
		t.Fatal(err)
	}
	newAddr, err := recordlog.NewVAddr(recordlog.CompactionSegmentBase, recordlog.SegmentHeaderSize, oldRef.PhysicalSize)
	if err != nil {
		t.Fatal(err)
	}
	newRef, err := recordlog.NewRecordRef(newAddr, oldRef.PhysicalSize)
	if err != nil {
		t.Fatal(err)
	}
	redirects, err := c.InstallCommitRedirects(context.Background(), map[recordlog.VAddr]recordlog.RecordRef{oldRef.Addr: newRef})
	if err != nil {
		t.Fatal(err)
	}
	// Model a Submit that prepared before redirect installation but reached the
	// Coordinator queue afterwards. This is the race the redirect table closes.
	result := make(chan response, 1)
	c.requests <- request{batch: batch, prepared: prepared, reserve: reservation, result: result, queuedAt: time.Now()}
	if answer := <-result; answer.err != nil {
		t.Fatal(answer.err)
	}
	if _, err := redirects.Release(context.Background()); err != nil {
		t.Fatal(err)
	}
	log.mu.Lock()
	checkpoints := len(log.checkpoints)
	log.mu.Unlock()
	if checkpoints != 0 {
		t.Fatalf("checkpoints=%d", checkpoints)
	}
	if got, exists, err := current.LookupRef(7); err != nil || !exists || got != newRef {
		t.Fatalf("ref=%+v exists=%v err=%v", got, exists, err)
	}
	groups := log.snapshotGroups()
	if len(groups) != 1 || len(groups[0].Descriptors) != 1 || groups[0].Descriptors[0].Mutations[0].NewAddr != newRef.Addr {
		t.Fatalf("groups=%+v", groups)
	}
}

func TestCommitRedirectInstallDoesNotBlockLaterAdmission(t *testing.T) {
	log := &fakeLog{firstSync: make(chan struct{}), releaseSync: make(chan struct{})}
	defer func() {
		select {
		case <-log.releaseSync:
		default:
			close(log.releaseSync)
		}
	}()
	current := mapping.NewEmpty()
	c := newCoordinator(t, log, current)

	first := newBatch(t, 1, log)
	if err := first.Put(context.Background(), 1, []byte("first")); err != nil {
		t.Fatal(err)
	}
	firstReceipt, err := c.Submit(context.Background(), first)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-log.firstSync:
	case <-time.After(time.Second):
		t.Fatal("first commit did not reach durable append")
	}

	second := newBatch(t, 2, log)
	if err := second.Put(context.Background(), 2, []byte("second")); err != nil {
		t.Fatal(err)
	}
	oldRef := second.PendingPutRefs()[0]
	newAddr, err := recordlog.NewVAddr(recordlog.CompactionSegmentBase, recordlog.SegmentHeaderSize, oldRef.PhysicalSize)
	if err != nil {
		t.Fatal(err)
	}
	newRef, err := recordlog.NewRecordRef(newAddr, oldRef.PhysicalSize)
	if err != nil {
		t.Fatal(err)
	}
	type installResult struct {
		redirects *CommitRedirects
		err       error
	}
	installed := make(chan installResult, 1)
	go func() {
		redirects, err := c.InstallCommitRedirects(context.Background(), map[recordlog.VAddr]recordlog.RecordRef{oldRef.Addr: newRef})
		installed <- installResult{redirects: redirects, err: err}
	}()
	waitForQueuedRequests(t, c, 1)

	submitted := make(chan struct {
		receipt Receipt
		err     error
	}, 1)
	go func() {
		receipt, err := c.Submit(context.Background(), second)
		submitted <- struct {
			receipt Receipt
			err     error
		}{receipt: receipt, err: err}
	}()
	var secondReceipt Receipt
	select {
	case answer := <-submitted:
		if answer.err != nil {
			t.Fatal(answer.err)
		}
		secondReceipt = answer.receipt
	case <-time.After(time.Second):
		t.Fatal("redirect installation blocked later commit admission")
	}
	waitForQueuedRequests(t, c, 2)
	close(log.releaseSync)

	redirects := <-installed
	if redirects.err != nil {
		t.Fatal(redirects.err)
	}
	if _, err := redirects.redirects.Release(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := firstReceipt.Wait(); err != nil {
		t.Fatal(err)
	}
	if _, err := secondReceipt.Wait(); err != nil {
		t.Fatal(err)
	}
	if got, exists, err := current.LookupRef(2); err != nil || !exists || got != newRef {
		t.Fatalf("ref=%+v exists=%v err=%v", got, exists, err)
	}
}

func TestRelocationSharesCommitOrderAndUsesAddressCAS(t *testing.T) {
	log := &fakeLog{}
	current := mapping.NewEmpty()
	c := newCoordinator(t, log, current)

	user := newBatch(t, 7, log)
	if err := user.Put(context.Background(), 3, []byte("value")); err != nil {
		t.Fatal(err)
	}
	committed, err := c.Commit(context.Background(), user)
	if err != nil || committed.CommitSeq != 1 {
		t.Fatalf("user commit=%+v err=%v", committed, err)
	}
	oldAddr, exists, err := current.Lookup(3)
	if err != nil || !exists {
		t.Fatalf("old addr=%v exists=%v err=%v", oldAddr, exists, err)
	}
	copiedPayload, err := recordcodec.EncodePut(recordcodec.PutRecord{OriginBatchID: 7, RecordID: 3, Value: []byte("value")}, 1024)
	if err != nil {
		t.Fatal(err)
	}
	copied, err := log.Append(context.Background(), copiedPayload, false)
	if err != nil {
		t.Fatal(err)
	}
	relocated, err := c.Relocate(context.Background(), Relocation{
		BatchID: 8, LogicalPayloadBytes: 5,
		Changes: []mapping.Change{{RecordID: 3, ExpectedOldAddr: oldAddr, NewRef: resultRef(t, copied), Operation: mapping.OperationRelocate}},
	})
	if err != nil || relocated.CommitSeq != 2 || relocated.Applied != 1 || relocated.Skipped != 0 {
		t.Fatalf("relocation=%+v err=%v", relocated, err)
	}
	if got, exists, err := current.Lookup(3); err != nil || !exists || got != copied.Addr {
		t.Fatalf("relocated addr=%v exists=%v err=%v", got, exists, err)
	}

	staleCopy, err := log.Append(context.Background(), copiedPayload, false)
	if err != nil {
		t.Fatal(err)
	}
	stale, err := c.Relocate(context.Background(), Relocation{
		BatchID: 9, LogicalPayloadBytes: 5,
		Changes: []mapping.Change{{RecordID: 3, ExpectedOldAddr: oldAddr, NewRef: resultRef(t, staleCopy), Operation: mapping.OperationRelocate}},
	})
	if err != nil || stale.CommitSeq != 3 || stale.Applied != 0 || stale.Skipped != 1 {
		t.Fatalf("stale relocation=%+v err=%v", stale, err)
	}
	if got, _, _ := current.Lookup(3); got != copied.Addr {
		t.Fatalf("stale relocation overwrote current addr=%v", got)
	}
	groups := log.snapshotGroups()
	if len(groups) != 3 || groups[1].Descriptors[0].Kind != recordcodec.DescriptorRelocation ||
		groups[1].Descriptors[0].Mutations[0].ExpectedOldAddr != oldAddr ||
		groups[2].Descriptors[0].Kind != recordcodec.DescriptorRelocation {
		t.Fatalf("groups=%+v", groups)
	}
}

func TestUserCommitPrecedesRelocationWithinCommitGroup(t *testing.T) {
	log := &fakeLog{}
	current := mapping.NewEmpty()
	c := newCoordinator(t, log, current)

	initial := newBatch(t, 1, log)
	if err := initial.Put(context.Background(), 3, []byte("initial")); err != nil {
		t.Fatal(err)
	}
	if result, err := c.Commit(context.Background(), initial); err != nil || result.CommitSeq != 1 {
		t.Fatalf("initial commit=%+v err=%v", result, err)
	}
	oldAddr, exists, err := current.Lookup(3)
	if err != nil || !exists {
		t.Fatalf("old addr=%v exists=%v err=%v", oldAddr, exists, err)
	}

	copiedPayload, err := recordcodec.EncodePut(recordcodec.PutRecord{OriginBatchID: 1, RecordID: 3, Value: []byte("initial")}, 1024)
	if err != nil {
		t.Fatal(err)
	}
	copied, err := log.Append(context.Background(), copiedPayload, false)
	if err != nil {
		t.Fatal(err)
	}

	log.firstSync = make(chan struct{})
	log.releaseSync = make(chan struct{})
	released := false
	defer func() {
		if !released {
			close(log.releaseSync)
		}
	}()
	blocker := newBatch(t, 2, log)
	if err := blocker.Put(context.Background(), 99, []byte("blocker")); err != nil {
		t.Fatal(err)
	}
	blockerDone := make(chan error, 1)
	go func() {
		_, commitErr := c.Commit(context.Background(), blocker)
		blockerDone <- commitErr
	}()
	select {
	case <-log.firstSync:
	case <-time.After(time.Second):
		t.Fatal("blocking commit did not reach durable append")
	}

	relocationDone := make(chan relocationResponse, 1)
	go func() {
		result, relocateErr := c.Relocate(context.Background(), Relocation{
			BatchID: 3, LogicalPayloadBytes: 7,
			Changes: []mapping.Change{{RecordID: 3, ExpectedOldAddr: oldAddr, NewRef: resultRef(t, copied), Operation: mapping.OperationRelocate}},
		})
		relocationDone <- relocationResponse{result: result, err: relocateErr}
	}()
	waitForQueuedRequests(t, c, 1)

	user := newBatch(t, 4, log)
	if err := user.CompareAndPut(context.Background(), 3, oldAddr, []byte("user")); err != nil {
		t.Fatal(err)
	}
	userAddr := log.latestPutAddr()
	userDone := make(chan response, 1)
	go func() {
		result, commitErr := c.Commit(context.Background(), user)
		userDone <- response{result: result, err: commitErr}
	}()
	waitForQueuedRequests(t, c, 2)
	close(log.releaseSync)
	released = true

	if err := <-blockerDone; err != nil {
		t.Fatal(err)
	}
	userResult := <-userDone
	if userResult.err != nil || userResult.result.CommitSeq != 3 {
		t.Fatalf("user commit=%+v err=%v", userResult.result, userResult.err)
	}
	relocationResult := <-relocationDone
	if relocationResult.err != nil || relocationResult.result.CommitSeq != 4 || relocationResult.result.Applied != 0 || relocationResult.result.Skipped != 1 {
		t.Fatalf("relocation=%+v err=%v", relocationResult.result, relocationResult.err)
	}
	if got, exists, lookupErr := current.Lookup(3); lookupErr != nil || !exists || got != userAddr {
		t.Fatalf("final addr=%v want=%v exists=%v err=%v", got, userAddr, exists, lookupErr)
	}
	groups := log.snapshotGroups()
	if len(groups) != 3 || len(groups[2].Descriptors) != 2 ||
		groups[2].Descriptors[0].Kind != recordcodec.DescriptorUserCommit ||
		groups[2].Descriptors[0].BatchID != 4 ||
		groups[2].Descriptors[0].CommitSeq != 3 ||
		groups[2].Descriptors[1].Kind != recordcodec.DescriptorRelocation ||
		groups[2].Descriptors[1].BatchID != 3 ||
		groups[2].Descriptors[1].CommitSeq != 4 {
		t.Fatalf("groups=%+v", groups)
	}
	if c.NextCommitSeq() != 5 {
		t.Fatalf("next commit seq=%d", c.NextCommitSeq())
	}
}

func TestOrderUserBeforeRelocationIsStable(t *testing.T) {
	relocation1 := Relocation{BatchID: 1}
	relocation2 := Relocation{BatchID: 2}
	group := []request{
		{relocation: &relocation1},
		{prepared: transaction.Prepared{BatchID: 3}},
		{prepared: transaction.Prepared{BatchID: 4}},
		{relocation: &relocation2},
	}
	ordered := orderUserBeforeRelocation(group)
	if len(ordered) != 4 || ordered[0].prepared.BatchID != 3 || ordered[1].prepared.BatchID != 4 ||
		ordered[2].relocation.BatchID != 1 || ordered[3].relocation.BatchID != 2 {
		t.Fatalf("ordered=%+v", ordered)
	}
}

func waitForQueuedRequests(t *testing.T, c *Coordinator, want int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for len(c.requests) != want && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := len(c.requests); got != want {
		t.Fatalf("queued=%d want=%d", got, want)
	}
}

func TestRelocationDurabilityFailureIsCommitUnknown(t *testing.T) {
	wantErr := errors.New("sync failed")
	log := &fakeLog{syncErr: wantErr, poisoned: true}
	current := mapping.NewEmpty()
	c := newCoordinator(t, log, current)
	oldAddr, _ := recordlog.NewVAddr(1, 64, 64)
	newAddr, _ := recordlog.NewVAddr(1, 128, 64)
	result, err := c.Relocate(context.Background(), Relocation{
		BatchID: 1, LogicalPayloadBytes: 1,
		Changes: []mapping.Change{{RecordID: 1, ExpectedOldAddr: oldAddr, NewRef: addrRef(t, newAddr), Operation: mapping.OperationRelocate}},
	})
	if result != (RelocationResult{}) || !errors.Is(err, base.ErrCommitUnknown) || !errors.Is(err, wantErr) || c.Fault() == nil {
		t.Fatalf("result=%+v err=%v fault=%v", result, err, c.Fault())
	}
}

func TestQueuedCommitsFormOneVirtualMappingGroup(t *testing.T) {
	log := &fakeLog{firstSync: make(chan struct{}), releaseSync: make(chan struct{})}
	current := mapping.NewEmpty()
	c := newCoordinator(t, log, current)

	first := newBatch(t, 1, log)
	if err := first.Put(context.Background(), 9, []byte("one")); err != nil {
		t.Fatal(err)
	}
	firstDone := make(chan error, 1)
	go func() {
		_, err := c.Commit(context.Background(), first)
		firstDone <- err
	}()
	select {
	case <-log.firstSync:
	case <-time.After(time.Second):
		t.Fatal("first commit did not reach durable append")
	}

	second := newBatch(t, 2, log)
	firstAddr := log.latestPutAddr()
	if err := second.CompareAndPut(context.Background(), 9, firstAddr, []byte("two")); err != nil {
		t.Fatal(err)
	}
	secondAddr := log.latestPutAddr()
	third := newBatch(t, 3, log)
	if err := third.CompareAndPut(context.Background(), 9, secondAddr, []byte("three")); err != nil {
		t.Fatal(err)
	}
	results := make(chan response, 2)
	go func() {
		result, err := c.Commit(context.Background(), second)
		results <- response{result: result, err: err}
	}()
	deadline := time.Now().Add(time.Second)
	for len(c.requests) != 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if len(c.requests) != 1 {
		t.Fatalf("second queued=%d", len(c.requests))
	}
	go func() {
		result, err := c.Commit(context.Background(), third)
		results <- response{result: result, err: err}
	}()
	deadline = time.Now().Add(time.Second)
	for len(c.requests) != 2 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if len(c.requests) != 2 {
		t.Fatalf("queued=%d", len(c.requests))
	}
	close(log.releaseSync)
	if err := <-firstDone; err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if answer := <-results; answer.err != nil {
			t.Fatal(answer.err)
		}
	}
	groups := log.snapshotGroups()
	if len(groups) != 2 || len(groups[1].Descriptors) != 2 {
		t.Fatalf("groups=%+v", groups)
	}
	addr, exists, _ := current.Lookup(9)
	if !exists || addr != log.latestPutAddr() {
		t.Fatalf("addr=%+v exists=%v", addr, exists)
	}
}

func TestConflictDoesNotConsumeCommitSequence(t *testing.T) {
	addr, err := recordlog.NewVAddr(1, 64, 64)
	if err != nil {
		t.Fatal(err)
	}
	current, err := mapping.New(mapping.Snapshot{CoveredCommitSeq: 4, Entries: map[model.ID]recordlog.RecordRef{1: addrRef(t, addr)}})
	if err != nil {
		t.Fatal(err)
	}
	log := &fakeLog{}
	c := newCoordinator(t, log, current)
	b := newBatch(t, 9, log)
	wrong, _ := recordlog.NewVAddr(2, 64, 64)
	if err := b.CompareAndDelete(1, wrong); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Commit(context.Background(), b); !errors.Is(err, base.ErrConflict) {
		t.Fatalf("commit err=%v", err)
	}
	if c.NextCommitSeq() != 5 || current.CoveredCommitSeq() != 4 || len(log.snapshotGroups()) != 0 {
		t.Fatalf("next=%d covered=%d groups=%d", c.NextCommitSeq(), current.CoveredCommitSeq(), len(log.snapshotGroups()))
	}
}

func TestConflictReleasesDeltaReservation(t *testing.T) {
	log := &fakeLog{}
	current := newPersistentMapping(t)
	c := newCoordinator(t, log, current)
	b := newBatch(t, 10, log)
	expected, _ := recordlog.NewVAddr(1, 64, 64)
	if err := b.Put(context.Background(), 1, []byte("value")); err != nil {
		t.Fatal(err)
	}
	if err := b.ExpectAddress(1, expected); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Commit(context.Background(), b); !errors.Is(err, base.ErrConflict) {
		t.Fatalf("commit err=%v", err)
	}
	if charged, reserved, _, _ := current.DeltaUsage(); charged != 0 || reserved != 0 {
		t.Fatalf("reservation leaked charged=%d reserved=%d", charged, reserved)
	}
}

func TestDurabilityFailureMarksCommitUnknownAndFaults(t *testing.T) {
	wantErr := errors.New("sync failed")
	log := &fakeLog{syncErr: wantErr, poisoned: true}
	current := newPersistentMapping(t)
	c := newCoordinator(t, log, current)
	b := newBatch(t, 1, log)
	if err := b.Put(context.Background(), 1, []byte("value")); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Commit(context.Background(), b); !errors.Is(err, base.ErrCommitUnknown) || !errors.Is(err, wantErr) {
		t.Fatalf("commit err=%v", err)
	}
	state, _ := b.State()
	if state != transaction.StateCommitUnknown || c.Fault() == nil {
		t.Fatalf("state=%d fault=%v", state, c.Fault())
	}
	if _, exists, _ := current.Lookup(1); exists {
		t.Fatal("mapping published after failed durability")
	}
	if charged, reserved, _, _ := current.DeltaUsage(); charged != 0 || reserved != 0 {
		t.Fatalf("reservation leaked charged=%d reserved=%d", charged, reserved)
	}
}

func TestCheckpointCutFollowsPublishedCommits(t *testing.T) {
	log := &fakeLog{}
	current := mapping.NewEmpty()
	c := newCoordinator(t, log, current)
	b := newBatch(t, 7, log)
	if err := b.Put(context.Background(), 3, []byte("value")); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Commit(context.Background(), b); err != nil {
		t.Fatal(err)
	}
	cut, err := c.CheckpointCut(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if cut.CoveredCommitSeq != 1 || !cut.ReplayStart.Valid() {
		t.Fatalf("cut=%+v", cut)
	}
	records, checkpoints := log.snapshotRecords()
	if len(records) != 3 || records[0] != recordcodec.RecordTypePut || records[1] != recordcodec.RecordTypeCommitGroup || records[2] != recordcodec.RecordTypeCheckpoint {
		t.Fatalf("records=%v", records)
	}
	if len(checkpoints) != 1 || checkpoints[0].CoveredCommitSeq != 1 {
		t.Fatalf("checkpoints=%+v", checkpoints)
	}
	if status := log.Status(); status.Poisoned {
		t.Fatalf("status=%+v", status)
	}
}

func TestMetricsObserveUserCommitPipelineAndConflict(t *testing.T) {
	log := &fakeLog{}
	current := mapping.NewEmpty()
	c := newCoordinator(t, log, current)

	committed := newBatch(t, 1, log)
	if err := committed.Put(context.Background(), 1, []byte("value")); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Commit(context.Background(), committed); err != nil {
		t.Fatal(err)
	}

	conflict := newBatch(t, 2, log)
	if err := conflict.Put(context.Background(), 1, []byte("stale")); err != nil {
		t.Fatal(err)
	}
	if err := conflict.ExpectAbsent(1); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Commit(context.Background(), conflict); !errors.Is(err, base.ErrConflict) {
		t.Fatalf("conflict err=%v", err)
	}

	metrics := c.Metrics()
	if metrics.CommitQueued != 2 || metrics.CommitGroups != 1 || metrics.GroupBatches != 1 || metrics.Conflicts != 1 {
		t.Fatalf("metrics=%+v", metrics)
	}
	if metrics.QueueWaitNanos == 0 || metrics.ValidationNanos == 0 || metrics.WriteSyncNanos == 0 || metrics.PublishNanos == 0 {
		t.Fatalf("missing duration metrics: %+v", metrics)
	}
}

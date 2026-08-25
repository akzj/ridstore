package coordinator

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/akzj/ridstore/internal/base"
	"github.com/akzj/ridstore/internal/mapping"
	"github.com/akzj/ridstore/internal/model"
	"github.com/akzj/ridstore/internal/recordcodec"
	"github.com/akzj/ridstore/internal/recordlog"
	"github.com/akzj/ridstore/internal/transaction"
)

type fakeAllocator struct{}

func (fakeAllocator) Allocate(context.Context) (uint64, error) { return 1, nil }

type fakeLog struct {
	mu          sync.Mutex
	nextOffset  uint32
	groups      []recordcodec.CommitGroup
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
	if syncWrite {
		group, decodeErr := recordcodec.DecodeCommitGroup(payload, 1<<20, 128, 1024)
		if decodeErr != nil {
			l.mu.Unlock()
			return recordlog.AppendResult{}, decodeErr
		}
		l.groups = append(l.groups, group)
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

func newCoordinator(t *testing.T, log *fakeLog, current *mapping.Mapping) *Coordinator {
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
	entry, exists, err := current.Lookup(3)
	if err != nil || !exists || entry.Revision != 7 {
		t.Fatalf("entry=%+v exists=%v err=%v", entry, exists, err)
	}
	groups := log.snapshotGroups()
	if len(groups) != 1 || len(groups[0].Descriptors) != 1 || groups[0].Descriptors[0].BatchID != 7 || groups[0].Descriptors[0].CommitSeq != 1 {
		t.Fatalf("groups=%+v", groups)
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
	if err := second.Update(context.Background(), 9, 1, []byte("two")); err != nil {
		t.Fatal(err)
	}
	third := newBatch(t, 3, log)
	if err := third.Update(context.Background(), 9, 2, []byte("three")); err != nil {
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
	entry, exists, _ := current.Lookup(9)
	if !exists || entry.Revision != 3 {
		t.Fatalf("entry=%+v exists=%v", entry, exists)
	}
}

func TestConflictDoesNotConsumeCommitSequence(t *testing.T) {
	addr, err := recordlog.NewVAddr(1, 64, 64)
	if err != nil {
		t.Fatal(err)
	}
	current, err := mapping.New(mapping.Snapshot{CoveredCommitSeq: 4, Entries: map[model.ID]mapping.Entry{1: {Addr: addr, Revision: 8}}})
	if err != nil {
		t.Fatal(err)
	}
	log := &fakeLog{}
	c := newCoordinator(t, log, current)
	b := newBatch(t, 9, log)
	if err := b.DeleteIfRevision(1, 7); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Commit(context.Background(), b); !errors.Is(err, base.ErrConflict) {
		t.Fatalf("commit err=%v", err)
	}
	if c.NextCommitSeq() != 5 || current.CoveredCommitSeq() != 4 || len(log.snapshotGroups()) != 0 {
		t.Fatalf("next=%d covered=%d groups=%d", c.NextCommitSeq(), current.CoveredCommitSeq(), len(log.snapshotGroups()))
	}
}

func TestDurabilityFailureMarksCommitUnknownAndFaults(t *testing.T) {
	wantErr := errors.New("sync failed")
	log := &fakeLog{syncErr: wantErr, poisoned: true}
	current := mapping.NewEmpty()
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
}

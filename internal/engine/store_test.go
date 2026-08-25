package engine

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/akzj/ridstore/internal/base"
	"github.com/akzj/ridstore/internal/coordinator"
	"github.com/akzj/ridstore/internal/idalloc"
	"github.com/akzj/ridstore/internal/mapping"
	"github.com/akzj/ridstore/internal/mapstore"
	"github.com/akzj/ridstore/internal/model"
	"github.com/akzj/ridstore/internal/radix"
	"github.com/akzj/ridstore/internal/recordlog"
	"github.com/akzj/ridstore/internal/storecatalog"
	"github.com/akzj/ridstore/internal/transaction"
)

type memoryLog struct {
	mu      sync.Mutex
	offset  uint32
	records map[recordlog.VAddr][]byte
	closed  bool
}

type emptyNodeStore struct{}

func (emptyNodeStore) Read(model.MapAddr) (mapstore.Node, error) {
	return mapstore.Node{}, errors.New("unexpected persistent node read")
}

func (emptyNodeStore) Append(uint8, uint64, model.CommitSeq, [mapstore.NodeSlots]uint64) (model.MapAddr, error) {
	return 0, errors.New("unexpected persistent node append")
}

func (emptyNodeStore) Sync() error { return nil }

func newPersistent(t *testing.T) *mapping.Persistent {
	t.Helper()
	nodes := emptyNodeStore{}
	tree, err := radix.Open(nodes, 0, 0, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	current, err := mapping.OpenPersistent(tree, nodes, mapping.PersistentConfig{
		MaxCheckpointEntries: 1024, DeltaSoftLimitBytes: 32 << 10, DeltaHardLimitBytes: 64 << 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	return current
}

func (l *memoryLog) Append(_ context.Context, payload []byte, _ bool) (recordlog.AppendResult, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return recordlog.AppendResult{}, recordlog.ErrClosed
	}
	physical, err := recordlog.PhysicalRecordSize(uint64(len(payload)))
	if err != nil {
		return recordlog.AppendResult{}, err
	}
	if l.offset == 0 {
		l.offset = recordlog.SegmentHeaderSize
	}
	addr, err := recordlog.NewVAddr(1, l.offset, physical)
	if err != nil {
		return recordlog.AppendResult{}, err
	}
	l.offset += physical
	l.records[addr] = append([]byte(nil), payload...)
	return recordlog.NewAppendResult(addr, physical)
}

func (l *memoryLog) Read(_ context.Context, addr recordlog.VAddr) ([]byte, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	payload, ok := l.records[addr]
	if !ok {
		return nil, recordlog.ErrInvalidVAddr
	}
	return append([]byte(nil), payload...), nil
}

func (l *memoryLog) Inspect(_ context.Context, addr recordlog.VAddr, prefixBytes uint32) (recordlog.RecordMetadata, []byte, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	payload, ok := l.records[addr]
	if !ok || prefixBytes > uint32(len(payload)) {
		return recordlog.RecordMetadata{}, nil, recordlog.ErrInvalidVAddr
	}
	physical, err := recordlog.PhysicalRecordSize(uint64(len(payload)))
	if err != nil {
		return recordlog.RecordMetadata{}, nil, err
	}
	return recordlog.RecordMetadata{PhysicalSize: physical, PayloadSize: uint32(len(payload)), Addr: addr}, append([]byte(nil), payload[:prefixBytes]...), nil
}

func (l *memoryLog) Status() recordlog.Status {
	l.mu.Lock()
	defer l.mu.Unlock()
	return recordlog.Status{Closed: l.closed}
}

func (l *memoryLog) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return recordlog.ErrClosed
	}
	l.closed = true
	return nil
}

func newStore(t *testing.T, maxOpen int) *Store {
	t.Helper()
	log := &memoryLog{records: make(map[recordlog.VAddr][]byte)}
	ids, err := idalloc.New(idalloc.RecordID, 16, 1, log)
	if err != nil {
		t.Fatal(err)
	}
	batches, err := idalloc.New(idalloc.BatchID, 16, 1, log)
	if err != nil {
		t.Fatal(err)
	}
	store, err := New(log, newPersistent(t), ids, batches, Config{
		Batch:          transaction.Limits{MaxValueSize: 1024, MaxBatchBytes: 4096, MaxBatchMutations: 16, MaxBatchConditions: 16},
		Commit:         coordinator.Config{QueueCapacity: 16, MaxGroupBatches: 8, MaxGroupPayload: 1 << 20},
		MaxOpenBatches: maxOpen,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil && !errors.Is(err, base.ErrClosed) {
			t.Errorf("close: %v", err)
		}
	})
	return store
}

func TestStoreCreateUpdateConflictDelete(t *testing.T) {
	store := newStore(t, 4)
	created, err := store.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	id, err := created.Create(context.Background(), []byte("one"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := created.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
	record, err := store.Get(context.Background(), id)
	if err != nil || string(record.Value) != "one" || record.Addr == 0 {
		t.Fatalf("record=%+v err=%v", record, err)
	}

	conflict, _ := store.Begin(context.Background())
	wrong, _ := recordlog.NewVAddr(99, 64, 64)
	if err := conflict.CompareAndPut(context.Background(), id, wrong, []byte("bad")); err != nil {
		t.Fatal(err)
	}
	if _, err := conflict.Commit(context.Background()); !errors.Is(err, base.ErrConflict) {
		t.Fatalf("conflict err=%v", err)
	}

	updated, _ := store.Begin(context.Background())
	if err := updated.CompareAndPut(context.Background(), id, record.Addr, []byte("two")); err != nil {
		t.Fatal(err)
	}
	if _, err := updated.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
	record, err = store.Get(context.Background(), id)
	if err != nil || string(record.Value) != "two" || record.Addr == 0 {
		t.Fatalf("record=%+v err=%v", record, err)
	}

	deleted, _ := store.Begin(context.Background())
	if err := deleted.CompareAndDelete(id, record.Addr); err != nil {
		t.Fatal(err)
	}
	if _, err := deleted.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Get(context.Background(), id); !errors.Is(err, base.ErrNotFound) {
		t.Fatalf("get deleted err=%v", err)
	}
}

func TestOpenBatchLimitBackpressuresAndReleases(t *testing.T) {
	store := newStore(t, 1)
	first, err := store.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := store.Begin(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("begin err=%v", err)
	}
	if err := first.Abort(context.Background()); err != nil {
		t.Fatal(err)
	}
	second, err := store.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := second.Abort(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestRealRecordLogRoundTrip(t *testing.T) {
	root := t.TempDir()
	logID := recordlog.LogID{1, 2, 3, 4}
	if err := recordlog.CreateInitialSegment(root, logID, 1<<20); err != nil {
		t.Fatal(err)
	}
	replayStart, err := recordlog.NewLogPos(1, recordlog.SegmentHeaderSize)
	if err != nil {
		t.Fatal(err)
	}
	manifest := storecatalog.Manifest{
		Generation: 1, StoreUUID: storecatalog.StoreUUID{1}, RecordLogID: logID,
		HardLimits: storecatalog.HardLimits{
			SegmentSize: 1 << 20, MaxValueSize: 1024, MaxBatchBytes: 4096,
			MaxBatchMutations: 16, MaxBatchConditions: 16, MaxOpenBatches: 4,
			MaxRecordLogPayload: 64 << 10, IDReserveSize: 16, BatchIDReserveSize: 16,
		},
		ActiveDataSegmentID: 1, NextDataSegmentID: 2,
		ActiveMapSegmentID: 1, NextMapSegmentID: 2,
		ReplayStart: replayStart, ReservedIDHigh: 1, ReservedBatchIDHigh: 1, IssuedBatchIDHighAtCut: 1,
	}
	if err := storecatalog.Install(root, manifest, nil); err != nil {
		t.Fatal(err)
	}
	catalog, err := storecatalog.NewManager(root, manifest, nil)
	if err != nil {
		t.Fatal(err)
	}
	log, err := recordlog.Open(root, recordlog.Config{MaxQueuedBytes: 1 << 20, QueueCapacity: 32, BufferBytes: 64 << 10, BufferRecords: 32}, catalog)
	if err != nil {
		t.Fatal(err)
	}
	ids, _ := idalloc.New(idalloc.RecordID, 16, 1, log)
	batches, _ := idalloc.New(idalloc.BatchID, 16, 1, log)
	store, err := New(log, newPersistent(t), ids, batches, Config{
		Batch:  transaction.Limits{MaxValueSize: 1024, MaxBatchBytes: 4096, MaxBatchMutations: 16, MaxBatchConditions: 16},
		Commit: coordinator.Config{QueueCapacity: 16, MaxGroupBatches: 8, MaxGroupPayload: 64 << 10}, MaxOpenBatches: 4,
	})
	if err != nil {
		t.Fatal(err)
	}
	batch, err := store.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	id, err := batch.Create(context.Background(), []byte("durable"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := batch.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
	record, err := store.Get(context.Background(), id)
	if err != nil || string(record.Value) != "durable" || record.Addr == 0 {
		t.Fatalf("record=%+v err=%v", record, err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestOpenReplaysIntoPersistentMapping(t *testing.T) {
	root := t.TempDir()
	logID := recordlog.LogID{9, 8, 7, 6}
	if err := recordlog.CreateInitialSegment(root, logID, 8192); err != nil {
		t.Fatal(err)
	}
	storeID := storecatalog.StoreUUID{4, 3, 2, 1}
	if err := mapstore.CreateInitialSegment(root, mapstore.StoreID(storeID), 8192); err != nil {
		t.Fatal(err)
	}
	replayStart, err := recordlog.NewLogPos(1, recordlog.SegmentHeaderSize)
	if err != nil {
		t.Fatal(err)
	}
	manifest := storecatalog.Manifest{
		Generation: 1, StoreUUID: storeID, RecordLogID: logID,
		HardLimits: storecatalog.HardLimits{
			SegmentSize: 8192, MaxValueSize: 1024, MaxBatchBytes: 4096,
			MaxBatchMutations: 16, MaxBatchConditions: 16, MaxOpenBatches: 4,
			MaxRecordLogPayload: 4096, IDReserveSize: 16, BatchIDReserveSize: 16,
		},
		ActiveDataSegmentID: 1, NextDataSegmentID: 2,
		ActiveMapSegmentID: 1, NextMapSegmentID: 2,
		ReplayStart: replayStart, ReservedIDHigh: 1, ReservedBatchIDHigh: 1, IssuedBatchIDHighAtCut: 1,
	}
	if err := storecatalog.Install(root, manifest, nil); err != nil {
		t.Fatal(err)
	}
	config := OpenConfig{
		RecordLog:         recordlog.Config{MaxQueuedBytes: 1 << 20, QueueCapacity: 32, BufferBytes: 64 << 10, BufferRecords: 32},
		Commit:            coordinator.Config{QueueCapacity: 16, MaxGroupBatches: 8, MaxGroupPayload: 4096},
		MappingCacheBytes: 1 << 20, MaxCheckpointEntries: 1024,
		DeltaSoftLimitBytes: 32 << 10, DeltaHardLimitBytes: 64 << 10,
	}
	store, err := Open(context.Background(), root, config)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Open(context.Background(), root, config); !errors.Is(err, base.ErrLocked) {
		t.Fatalf("second open err=%v", err)
	}
	batch, err := store.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	id, err := batch.Create(context.Background(), []byte("survives restart"))
	if err != nil {
		t.Fatal(err)
	}
	committed, err := batch.Commit(context.Background())
	if err != nil || committed.CommitSeq != 1 {
		t.Fatalf("commit=%+v err=%v", committed, err)
	}
	for index := 0; index < 12; index++ {
		item, err := store.Begin(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if _, err := item.Create(context.Background(), make([]byte, 900)); err != nil {
			t.Fatal(err)
		}
		if _, err := item.Commit(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.Checkpoint(context.Background()); err != nil {
		t.Fatal(err)
	}
	checkpoint, err := storecatalog.Load(root)
	if err != nil || checkpoint.MappingRoot == 0 || checkpoint.CoveredCommitSeq != 13 || checkpoint.StatsCoveredCommitSeq != 13 || checkpoint.ReplayStart == replayStart || len(checkpoint.SealedDataSegments) == 0 || len(checkpoint.SegmentStats) == 0 {
		t.Fatalf("checkpoint=%+v err=%v", checkpoint, err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(context.Background(), root, config)
	if err != nil {
		t.Fatal(err)
	}
	record, err := reopened.Get(context.Background(), id)
	if err != nil || string(record.Value) != "survives restart" || record.Addr == 0 {
		t.Fatalf("record=%+v err=%v", record, err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
}

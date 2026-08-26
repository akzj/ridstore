package engine

import (
	"context"
	"errors"
	"sort"
	"sync"

	"github.com/akzj/ridstore/internal/base"
	"github.com/akzj/ridstore/internal/coordinator"
	"github.com/akzj/ridstore/internal/filelock"
	"github.com/akzj/ridstore/internal/idalloc"
	"github.com/akzj/ridstore/internal/maintstate"
	"github.com/akzj/ridstore/internal/mapping"
	"github.com/akzj/ridstore/internal/mapstore"
	"github.com/akzj/ridstore/internal/model"
	"github.com/akzj/ridstore/internal/recordcodec"
	"github.com/akzj/ridstore/internal/recordlog"
	"github.com/akzj/ridstore/internal/segmentstats"
	"github.com/akzj/ridstore/internal/storecatalog"
	"github.com/akzj/ridstore/internal/transaction"
)

type Log interface {
	coordinator.Appender
	Read(context.Context, recordlog.VAddr) ([]byte, error)
	Inspect(context.Context, recordlog.VAddr, uint32) (recordlog.RecordMetadata, []byte, error)
	Close() error
}

type maintenanceLog interface {
	ScanSegment(context.Context, recordlog.SegmentID, func(recordlog.AppendResult, []byte) error) error
	RetireSegment(context.Context, recordlog.SegmentID, uint64) error
}

type Config struct {
	Batch          transaction.Limits
	Commit         coordinator.Config
	MaxOpenBatches int
}

type Record struct {
	Value []byte
	Addr  recordlog.VAddr
}

type Store struct {
	ops           sync.RWMutex
	mu            sync.Mutex
	checkpointMu  sync.Mutex
	maintenanceMu sync.Mutex

	log                    Log
	maintenance            maintenanceLog
	maintenanceHook        maintstate.FaultHook
	root                   string
	mapping                *mapping.Persistent
	mapStore               *mapstore.Store
	catalog                *storecatalog.Manager
	ids                    *idalloc.Allocator
	batches                *idalloc.Allocator
	commits                *coordinator.Coordinator
	limits                 transaction.Limits
	maxOpen                int
	open                   map[model.BatchID]*Batch
	openCount              int
	notify                 chan struct{}
	closed                 bool
	fault                  error
	maxStats               uint64
	maxRelocationMutations uint64
	dirLock                *filelock.Lock
}

func New(log Log, current *mapping.Persistent, ids, batches *idalloc.Allocator, config Config) (*Store, error) {
	if log == nil || current == nil || ids == nil || batches == nil || config.MaxOpenBatches <= 0 || transaction.ValidateLimits(config.Batch) != nil {
		return nil, base.ErrInvalidConfig
	}
	commits, err := coordinator.New(current.CoveredCommitSeq()+1, log, current, config.Commit)
	if err != nil {
		return nil, err
	}
	return &Store{
		log: log, mapping: current, ids: ids, batches: batches, commits: commits,
		limits: config.Batch, maxOpen: config.MaxOpenBatches, open: make(map[model.BatchID]*Batch), notify: make(chan struct{}),
		maxRelocationMutations: (config.Commit.MaxGroupPayload - uint64(recordcodec.CommitGroupHeadSize+recordcodec.DescriptorHeadSize)) / uint64(recordcodec.MutationSize),
	}, nil
}

func (s *Store) Begin(ctx context.Context) (*Batch, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		s.ops.RLock()
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			s.ops.RUnlock()
			return nil, base.ErrClosed
		}
		if s.fault != nil {
			err := s.fault
			s.mu.Unlock()
			s.ops.RUnlock()
			return nil, errors.Join(base.ErrReadOnly, err)
		}
		if fault := s.commits.Fault(); fault != nil {
			s.mu.Unlock()
			s.ops.RUnlock()
			return nil, errors.Join(base.ErrReadOnly, fault)
		}
		if s.openCount < s.maxOpen {
			s.openCount++
			s.mu.Unlock()
			break
		}
		notify := s.notify
		s.mu.Unlock()
		s.ops.RUnlock()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-notify:
		}
	}
	raw, err := s.batches.Allocate(ctx)
	if err != nil {
		s.releaseSlot()
		s.ops.RUnlock()
		return nil, err
	}
	inner, err := transaction.New(model.BatchID(raw), s.limits, s.log, s.ids)
	if err != nil {
		s.releaseSlot()
		s.ops.RUnlock()
		return nil, err
	}
	b := &Batch{store: s, inner: inner}
	s.mu.Lock()
	s.open[b.ID()] = b
	s.mu.Unlock()
	s.ops.RUnlock()
	return b, nil
}

func (s *Store) Get(ctx context.Context, id model.ID) (Record, error) {
	if id == 0 {
		return Record{}, base.ErrInvalidID
	}
	if ctx == nil {
		ctx = context.Background()
	}
	s.ops.RLock()
	defer s.ops.RUnlock()
	for {
		if err := ctx.Err(); err != nil {
			return Record{}, err
		}
		s.mu.Lock()
		closed, fault := s.closed, s.fault
		s.mu.Unlock()
		if closed {
			return Record{}, base.ErrClosed
		}
		if fault != nil {
			return Record{}, errors.Join(base.ErrReadOnly, fault)
		}
		addr, exists, err := s.mapping.Lookup(id)
		if err != nil {
			return Record{}, err
		}
		if !exists {
			return Record{}, base.ErrNotFound
		}
		payload, err := s.log.Read(ctx, addr)
		if err != nil {
			return Record{}, err
		}
		current, stillExists, err := s.mapping.Lookup(id)
		if err != nil {
			return Record{}, err
		}
		if !stillExists || current != addr {
			continue
		}
		put, err := recordcodec.DecodePut(payload, s.limits.MaxValueSize)
		if err != nil || put.RecordID != id {
			return Record{}, errors.Join(base.ErrCorrupt, err)
		}
		return Record{Value: put.Value, Addr: addr}, nil
	}
}

func (s *Store) Close() error {
	s.checkpointMu.Lock()
	defer s.checkpointMu.Unlock()
	s.ops.Lock()
	defer s.ops.Unlock()
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return base.ErrClosed
	}
	s.closed = true
	open := make([]*Batch, 0, len(s.open))
	for _, batch := range s.open {
		open = append(open, batch)
	}
	s.signalLocked()
	s.mu.Unlock()
	var result error
	for _, batch := range open {
		state, _ := batch.inner.State()
		if state == transaction.StateOpen || state == transaction.StateFailed {
			result = errors.Join(result, batch.inner.Abort(context.Background(), 2))
			batch.finish()
		}
	}
	result = errors.Join(result, s.commits.Close())
	result = errors.Join(result, s.log.Close())
	if s.mapStore != nil {
		result = errors.Join(result, s.mapStore.Close())
	}
	if s.dirLock != nil {
		result = errors.Join(result, s.dirLock.Close())
	}
	return result
}

// Checkpoint installs one atomic Mapping, replay-cut, allocator, open-batch,
// and exact sealed-segment statistics generation.
func (s *Store) Checkpoint(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	s.checkpointMu.Lock()
	defer s.checkpointMu.Unlock()

	s.ops.Lock()
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		s.ops.Unlock()
		return base.ErrClosed
	}
	if s.fault != nil {
		err := s.fault
		s.mu.Unlock()
		s.ops.Unlock()
		return errors.Join(base.ErrReadOnly, err)
	}
	if s.catalog == nil || s.mapStore == nil || s.maxStats == 0 {
		s.mu.Unlock()
		s.ops.Unlock()
		return base.ErrInvalidConfig
	}
	s.mu.Unlock()
	cut, err := s.commits.CheckpointCut(ctx)
	if err != nil {
		s.ops.Unlock()
		return err
	}
	open, err := s.openBatchIDsAtCut()
	if err != nil {
		s.ops.Unlock()
		s.setFault(err)
		return err
	}
	sort.Slice(open, func(i, j int) bool { return open[i] < open[j] })
	reservedIDHigh := s.ids.DurableHigh()
	reservedBatchIDHigh := s.batches.DurableHigh()
	issuedBatchIDHigh := s.batches.IssuedHigh()
	frozen, err := s.mapping.Freeze(cut.CoveredCommitSeq)
	s.ops.Unlock()
	if err != nil {
		return err
	}
	abort := func(cause error) error {
		return errors.Join(cause, s.mapping.AbortCheckpoint(frozen))
	}
	candidate, err := s.mapping.BuildCheckpoint(frozen)
	if err != nil {
		if errors.Is(err, mapstore.ErrPoisoned) || errors.Is(err, mapping.ErrCorrupt) {
			s.setFault(err)
		}
		return abort(err)
	}
	manifest := s.catalog.Snapshot()
	stats, err := segmentstats.Build(ctx, candidate, s.log, segmentstats.FileSet{
		Active: manifest.ActiveDataSegmentID, Sealed: manifest.SealedDataSegments,
	}, manifest.HardLimits.MaxValueSize, s.maxStats)
	if err != nil {
		if errors.Is(err, base.ErrCorrupt) {
			s.setFault(err)
		}
		return abort(err)
	}
	_, err = s.catalog.InstallCheckpoint(manifest.Generation, storecatalog.Checkpoint{
		MappingRoot: candidate.Root(), CoveredCommitSeq: candidate.CoveredCommitSeq(), ReplayStart: cut.ReplayStart,
		ReservedIDHigh: reservedIDHigh, ReservedBatchIDHigh: reservedBatchIDHigh, IssuedBatchIDHighAtCut: issuedBatchIDHigh,
		OpenBatchIDsAtCut: open, StatsCoveredCommitSeq: candidate.CoveredCommitSeq(), SegmentStats: stats,
	})
	if err != nil {
		if !errors.Is(err, storecatalog.ErrConflict) {
			s.setFault(err)
		}
		return abort(err)
	}
	if err := s.mapping.InstallCheckpoint(candidate); err != nil {
		s.setFault(err)
		return errors.Join(base.ErrReadOnly, err)
	}
	return nil
}

// openBatchIDsAtCut runs after the Coordinator barrier. Every Commit admitted
// before that barrier has already reached a terminal transaction state even
// when its caller has not yet consumed the response and removed the Batch from
// Store.open. Only genuinely non-terminal, non-committing batches belong in
// the recovery snapshot.
func (s *Store) openBatchIDsAtCut() ([]model.BatchID, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	open := make([]model.BatchID, 0, len(s.open))
	for id, batch := range s.open {
		state, _ := batch.inner.State()
		switch state {
		case transaction.StateOpen, transaction.StateFailed:
			open = append(open, id)
		case transaction.StateCommitted, transaction.StateAborted, transaction.StateCommitUnknown:
			// Client-side finish may lag behind the durable terminal state.
		case transaction.StateCommitting:
			return nil, errors.Join(base.ErrCorrupt, errors.New("committing batch remained after checkpoint barrier"))
		default:
			return nil, errors.Join(base.ErrCorrupt, errors.New("unknown batch state at checkpoint"))
		}
	}
	return open, nil
}

func (s *Store) setFault(err error) {
	s.mu.Lock()
	if s.fault == nil {
		s.fault = err
	}
	s.mu.Unlock()
}

func (s *Store) signalLocked() {
	close(s.notify)
	s.notify = make(chan struct{})
}

func (s *Store) releaseSlot() {
	s.mu.Lock()
	if s.openCount > 0 {
		s.openCount--
	}
	s.signalLocked()
	s.mu.Unlock()
}

type Batch struct {
	store *Store
	inner *transaction.Batch
	done  sync.Once
}

func (b *Batch) ID() model.BatchID { return b.inner.ID() }

func (b *Batch) Allocate(ctx context.Context) (model.ID, error) {
	return withBatch(b, func() (model.ID, error) { return b.inner.Allocate(ctx) })
}

func (b *Batch) Create(ctx context.Context, value []byte) (model.ID, error) {
	return withBatch(b, func() (model.ID, error) { return b.inner.Create(ctx, value) })
}

func (b *Batch) Put(ctx context.Context, id model.ID, value []byte) error {
	_, err := withBatch(b, func() (struct{}, error) { return struct{}{}, b.inner.Put(ctx, id, value) })
	return err
}

func (b *Batch) CompareAndPut(ctx context.Context, id model.ID, expected recordlog.VAddr, value []byte) error {
	_, err := withBatch(b, func() (struct{}, error) { return struct{}{}, b.inner.CompareAndPut(ctx, id, expected, value) })
	return err
}

func (b *Batch) Delete(id model.ID) error {
	_, err := withBatch(b, func() (struct{}, error) { return struct{}{}, b.inner.Delete(id) })
	return err
}

func (b *Batch) CompareAndDelete(id model.ID, expected recordlog.VAddr) error {
	_, err := withBatch(b, func() (struct{}, error) { return struct{}{}, b.inner.CompareAndDelete(id, expected) })
	return err
}

func (b *Batch) ExpectAddress(id model.ID, addr recordlog.VAddr) error {
	_, err := withBatch(b, func() (struct{}, error) { return struct{}{}, b.inner.ExpectAddress(id, addr) })
	return err
}

func (b *Batch) ExpectAbsent(id model.ID) error {
	_, err := withBatch(b, func() (struct{}, error) { return struct{}{}, b.inner.ExpectAbsent(id) })
	return err
}

func (b *Batch) Commit(ctx context.Context) (coordinator.Result, error) {
	if b == nil || b.store == nil || b.inner == nil {
		return coordinator.Result{}, base.ErrBatchClosed
	}
	for {
		receipt, err := b.store.submitCommit(ctx, b.inner)
		if errors.Is(err, mapping.ErrBudget) {
			// Reservation failed before Prepare or durable append. Do not hold
			// ops.RLock here: Checkpoint must be able to install a Root and
			// release frozen Delta charge before this Commit retries admission.
			if err := b.store.Checkpoint(ctx); err != nil {
				return coordinator.Result{}, err
			}
			continue
		}
		if err != nil {
			if terminal(b.inner) {
				b.finish()
			}
			return coordinator.Result{}, err
		}
		result, err := receipt.Wait()
		if err == nil || terminal(b.inner) {
			b.finish()
		}
		return result, err
	}
}

func (s *Store) submitCommit(ctx context.Context, batch *transaction.Batch) (coordinator.Receipt, error) {
	s.ops.RLock()
	defer s.ops.RUnlock()
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return coordinator.Receipt{}, base.ErrClosed
	}
	if s.fault != nil {
		err := s.fault
		s.mu.Unlock()
		return coordinator.Receipt{}, errors.Join(base.ErrReadOnly, err)
	}
	s.mu.Unlock()
	return s.commits.Submit(ctx, batch)
}

func (b *Batch) Abort(ctx context.Context) error {
	_, err := withBatch(b, func() (struct{}, error) { return struct{}{}, b.inner.Abort(ctx, 1) })
	if terminal(b.inner) {
		b.finish()
	}
	return err
}

func withBatch[T any](b *Batch, run func() (T, error)) (T, error) {
	var zero T
	if b == nil || b.store == nil || b.inner == nil {
		return zero, base.ErrBatchClosed
	}
	b.store.ops.RLock()
	defer b.store.ops.RUnlock()
	b.store.mu.Lock()
	closed, fault := b.store.closed, b.store.fault
	b.store.mu.Unlock()
	if closed {
		return zero, base.ErrClosed
	}
	if fault != nil {
		return zero, errors.Join(base.ErrReadOnly, fault)
	}
	return run()
}

func terminal(batch *transaction.Batch) bool {
	state, _ := batch.State()
	return state == transaction.StateCommitted || state == transaction.StateAborted || state == transaction.StateCommitUnknown
}

func (b *Batch) finish() {
	b.done.Do(func() {
		b.store.mu.Lock()
		delete(b.store.open, b.ID())
		if b.store.openCount > 0 {
			b.store.openCount--
		}
		b.store.signalLocked()
		b.store.mu.Unlock()
	})
}

package engine

import (
	"context"
	"errors"
	"sync"

	"github.com/akzj/ridstore/internal/base"
	"github.com/akzj/ridstore/internal/coordinator"
	"github.com/akzj/ridstore/internal/idalloc"
	"github.com/akzj/ridstore/internal/mapping"
	"github.com/akzj/ridstore/internal/mapstore"
	"github.com/akzj/ridstore/internal/model"
	"github.com/akzj/ridstore/internal/recordcodec"
	"github.com/akzj/ridstore/internal/recordlog"
	"github.com/akzj/ridstore/internal/transaction"
)

type Log interface {
	coordinator.Appender
	Read(context.Context, recordlog.VAddr) ([]byte, error)
	Close() error
}

type Config struct {
	Batch          transaction.Limits
	Commit         coordinator.Config
	MaxOpenBatches int
}

type Record struct {
	Value    []byte
	Revision model.Revision
}

type Store struct {
	ops sync.RWMutex
	mu  sync.Mutex

	log       Log
	mapping   *mapping.Persistent
	mapStore  *mapstore.Store
	ids       *idalloc.Allocator
	batches   *idalloc.Allocator
	commits   *coordinator.Coordinator
	limits    transaction.Limits
	maxOpen   int
	open      map[model.BatchID]*Batch
	openCount int
	notify    chan struct{}
	closed    bool
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
		closed := s.closed
		s.mu.Unlock()
		if closed {
			return Record{}, base.ErrClosed
		}
		entry, exists, err := s.mapping.Lookup(id)
		if err != nil {
			return Record{}, err
		}
		if !exists {
			return Record{}, base.ErrNotFound
		}
		payload, err := s.log.Read(ctx, entry.Addr)
		if err != nil {
			return Record{}, err
		}
		current, stillExists, err := s.mapping.Lookup(id)
		if err != nil {
			return Record{}, err
		}
		if !stillExists || current != entry {
			continue
		}
		put, err := recordcodec.DecodePut(payload, s.limits.MaxValueSize)
		if err != nil || put.RecordID != id || model.Revision(put.OriginBatchID) != entry.Revision {
			return Record{}, errors.Join(base.ErrCorrupt, err)
		}
		return Record{Value: put.Value, Revision: entry.Revision}, nil
	}
}

func (s *Store) Close() error {
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
	return result
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

func (b *Batch) Update(ctx context.Context, id model.ID, revision model.Revision, value []byte) error {
	_, err := withBatch(b, func() (struct{}, error) { return struct{}{}, b.inner.Update(ctx, id, revision, value) })
	return err
}

func (b *Batch) Delete(id model.ID) error {
	_, err := withBatch(b, func() (struct{}, error) { return struct{}{}, b.inner.Delete(id) })
	return err
}

func (b *Batch) DeleteIfRevision(id model.ID, revision model.Revision) error {
	_, err := withBatch(b, func() (struct{}, error) { return struct{}{}, b.inner.DeleteIfRevision(id, revision) })
	return err
}

func (b *Batch) ExpectRevision(id model.ID, revision model.Revision) error {
	_, err := withBatch(b, func() (struct{}, error) { return struct{}{}, b.inner.ExpectRevision(id, revision) })
	return err
}

func (b *Batch) ExpectAbsent(id model.ID) error {
	_, err := withBatch(b, func() (struct{}, error) { return struct{}{}, b.inner.ExpectAbsent(id) })
	return err
}

func (b *Batch) Commit(ctx context.Context) (coordinator.Result, error) {
	result, err := withBatch(b, func() (coordinator.Result, error) { return b.store.commits.Commit(ctx, b.inner) })
	if err == nil || terminal(b.inner) {
		b.finish()
	}
	return result, err
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
	closed := b.store.closed
	b.store.mu.Unlock()
	if closed {
		return zero, base.ErrClosed
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

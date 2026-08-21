// Package ridstore provides an embedded stable-ID log-structured record store.
package ridstore

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/akzj/ridstore/internal/allocator"
	"github.com/akzj/ridstore/internal/appendlog"
	"github.com/akzj/ridstore/internal/base"
	batchimpl "github.com/akzj/ridstore/internal/batch"
	"github.com/akzj/ridstore/internal/commit"
	"github.com/akzj/ridstore/internal/filelock"
	storeformat "github.com/akzj/ridstore/internal/format"
	"github.com/akzj/ridstore/internal/initialize"
	"github.com/akzj/ridstore/internal/mapping/memory"
	"github.com/akzj/ridstore/internal/segment"
)

// ID is a stable logical record identifier. Zero is invalid.
type ID = base.ID

// BatchID uniquely identifies a batch. Zero is invalid.
type BatchID = base.BatchID

// CommitSeq orders durable user commits and internal relocations.
type CommitSeq = base.CommitSeq

// Revision is an opaque logical record revision.
type Revision = base.Revision

// Store is an exclusively opened ridstore data directory.
type Store struct {
	ops sync.RWMutex
	mu  sync.Mutex

	config         Config
	manifest       storeformat.Manifest
	lock           *filelock.Lock
	active         *segment.ActiveData
	log            *appendlog.Sequencer
	mapping        *memory.Mapping
	coordinator    *commit.Coordinator
	idAllocator    *allocator.Allocator
	batchAllocator *allocator.Allocator

	batches              map[BatchID]*Batch
	statuses             map[BatchID]BatchStatus
	openCount            int
	slotNotify           chan struct{}
	issuedBatchHigh      uint64
	recoveryAbortedStart uint64
	recoveryAbortedEnd   uint64

	closed bool
	fault  error
}

// Create initializes and exclusively opens a new Store. Interrupted
// initialization is resumed from INITIALIZING using the original Store UUID.
func Create(cfg Config) (*Store, error) {
	return createWithOptions(cfg, initialize.Options{})
}

func createWithOptions(cfg Config, opts initialize.Options) (*Store, error) {
	normalized, hard, err := normalizeCreateConfig(cfg)
	if err != nil {
		return nil, err
	}
	if err := prepareDirectory(normalized.Dir, true); err != nil {
		return nil, err
	}
	lock, err := filelock.Acquire(normalized.Dir)
	if err != nil {
		return nil, err
	}
	m, err := initialize.CreateWithOptions(normalized.Dir, hard, opts)
	if err != nil {
		return nil, errors.Join(err, lock.Close())
	}
	store, err := buildStore(normalized, m, lock, opts.Hook)
	if err != nil {
		return nil, errors.Join(err, lock.Close())
	}
	return store, nil
}

// Open recovers and exclusively opens an existing Store. It never creates a
// missing data directory or silently creates an empty Store.
func Open(cfg Config) (*Store, error) {
	if err := normalizeDir(&cfg); err != nil {
		return nil, err
	}
	if err := prepareDirectory(cfg.Dir, false); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, base.ErrNotInitialized
		}
		return nil, err
	}
	lock, err := filelock.Acquire(cfg.Dir)
	if err != nil {
		return nil, err
	}
	m, err := initialize.Open(cfg.Dir)
	if err != nil {
		return nil, errors.Join(err, lock.Close())
	}
	normalized, err := normalizeOpenConfig(cfg, m.HardLimits)
	if err != nil {
		return nil, errors.Join(err, lock.Close())
	}
	store, err := buildStore(normalized, m, lock, nil)
	if err != nil {
		return nil, errors.Join(err, lock.Close())
	}
	return store, nil
}

// Close releases the directory writer lease. Repeated Close returns ErrClosed.
func (s *Store) Close() error {
	if s == nil {
		return base.ErrClosed
	}
	s.ops.Lock()
	defer s.ops.Unlock()
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return base.ErrClosed
	}
	s.closed = true
	batches := make([]*Batch, 0, len(s.batches))
	for _, b := range s.batches {
		batches = append(batches, b)
	}
	s.signalSlotLocked()
	s.mu.Unlock()
	var result error
	for _, b := range batches {
		state, _ := b.inner.State()
		if state == batchimpl.StateOpen || state == batchimpl.StateFailed {
			result = errors.Join(result, b.inner.Abort(context.Background(), storeformat.AbortReasonCloseCleanup))
			b.finish(BatchStatus{BatchID: b.ID(), State: BatchStateAborted})
		}
	}
	result = errors.Join(result, s.log.Close())
	result = errors.Join(result, s.active.Close())
	result = errors.Join(result, s.lock.Close())
	return result
}

func prepareDirectory(path string, create bool) error {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) && create {
		if err := os.Mkdir(path, 0o700); err != nil && !errors.Is(err, os.ErrExist) {
			return err
		}
		parent, openErr := os.Open(filepath.Dir(path))
		if openErr != nil {
			return openErr
		}
		syncErr := parent.Sync()
		closeErr := parent.Close()
		if err := errors.Join(syncErr, closeErr); err != nil {
			return err
		}
		info, err = os.Lstat(path)
	}
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("ridstore path is not a real directory: %w", base.ErrInvalidConfig)
	}
	return nil
}

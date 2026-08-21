// Package ridstore provides an embedded stable-ID log-structured record store.
package ridstore

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/akzj/ridstore/internal/base"
	"github.com/akzj/ridstore/internal/filelock"
	storeformat "github.com/akzj/ridstore/internal/format"
	"github.com/akzj/ridstore/internal/initialize"
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
	mu       sync.Mutex
	config   Config
	manifest storeformat.Manifest
	lock     *filelock.Lock
	closed   bool
}

// Create initializes and exclusively opens a new Store. Interrupted
// initialization is resumed from INITIALIZING using the original Store UUID.
func Create(cfg Config) (*Store, error) {
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
	m, err := initialize.Create(normalized.Dir, hard)
	if err != nil {
		_ = lock.Close()
		return nil, err
	}
	return &Store{config: normalized, manifest: m, lock: lock}, nil
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
		_ = lock.Close()
		return nil, err
	}
	normalized, err := normalizeOpenConfig(cfg, m.HardLimits)
	if err != nil {
		_ = lock.Close()
		return nil, err
	}
	return &Store{config: normalized, manifest: m, lock: lock}, nil
}

// Close releases the directory writer lease. Repeated Close returns ErrClosed.
func (s *Store) Close() error {
	if s == nil {
		return base.ErrClosed
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return base.ErrClosed
	}
	s.closed = true
	return s.lock.Close()
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

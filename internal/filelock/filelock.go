// Package filelock implements the writer lease that guards one ridstore
// directory.
//
// Exactly one process may hold the lease at a time. The lease lives in the
// kernel, not in the LOCK file's contents, so a crashed writer cannot leave the
// store permanently locked: the operating system releases the lease when the
// holding process exits, including after a hard kill.
//
// This file holds the platform-neutral checks and the Lock type. How a lease is
// opened and taken lives in filelock_unix.go and filelock_windows.go; both
// implementations must preserve the same observable semantics:
//
//   - the store directory must be a real directory, not a symlink;
//   - LOCK must be a regular file reached without following a symlink, so a
//     symlinked or otherwise non-regular LOCK reports base.ErrCorrupt;
//   - the lease is exclusive and non-blocking, so losing the race reports
//     base.ErrLocked instead of waiting for the holder.
package filelock

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/akzj/ridstore/internal/base"
)

const FileName = "LOCK"

// Lock owns the process-wide writer lease for one ridstore directory.
// The kernel releases the lease if the process exits without calling Close.
type Lock struct {
	mu     sync.Mutex
	file   *os.File
	closed bool
}

// Acquire takes the writer lease, creating LOCK when it is missing.
func Acquire(dir string) (*Lock, error) {
	return acquire(dir, true)
}

// AcquireExisting obtains the writer lease without creating or opening LOCK
// for writing. Offline read-only tools use it so a malformed store missing
// LOCK remains byte-for-byte unchanged.
func AcquireExisting(dir string) (*Lock, error) {
	return acquire(dir, false)
}

func acquire(dir string, create bool) (*Lock, error) {
	info, err := os.Lstat(dir)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return nil, fmt.Errorf("ridstore directory is not a real directory: %s: %w", dir, base.ErrInvalidConfig)
	}
	file, err := openLockFile(filepath.Join(dir, FileName), create)
	if err != nil {
		return nil, err
	}
	return &Lock{file: file}, nil
}

func (l *Lock) Close() error {
	if l == nil {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return nil
	}
	l.closed = true
	unlockErr := unlockFile(l.file)
	closeErr := l.file.Close()
	l.file = nil
	return errors.Join(unlockErr, closeErr)
}

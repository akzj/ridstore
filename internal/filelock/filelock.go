package filelock

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"syscall"

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

func Acquire(dir string) (*Lock, error) {
	return acquire(dir, syscall.O_RDWR|syscall.O_CREAT)
}

// AcquireExisting obtains the writer lease without creating or opening LOCK
// for writing. Offline read-only tools use it so a malformed store missing
// LOCK remains byte-for-byte unchanged.
func AcquireExisting(dir string) (*Lock, error) {
	return acquire(dir, syscall.O_RDONLY)
}

func acquire(dir string, accessFlags int) (*Lock, error) {
	info, err := os.Lstat(dir)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return nil, fmt.Errorf("ridstore directory is not a real directory: %s: %w", dir, base.ErrInvalidConfig)
	}
	path := filepath.Join(dir, FileName)
	fd, err := syscall.Open(path, accessFlags|syscall.O_CLOEXEC|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0o600)
	if err != nil {
		if errors.Is(err, syscall.ELOOP) {
			return nil, fmt.Errorf("LOCK must not be a symlink: %w", base.ErrCorrupt)
		}
		return nil, err
	}
	file := os.NewFile(uintptr(fd), path)
	if file == nil {
		_ = syscall.Close(fd)
		return nil, fmt.Errorf("open LOCK file")
	}
	var stat syscall.Stat_t
	if err := syscall.Fstat(fd, &stat); err != nil {
		_ = file.Close()
		return nil, err
	}
	if stat.Mode&syscall.S_IFMT != syscall.S_IFREG {
		_ = file.Close()
		return nil, fmt.Errorf("LOCK is not a regular file: %w", base.ErrCorrupt)
	}
	if err := syscall.Flock(fd, syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = file.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) || errors.Is(err, syscall.EAGAIN) {
			return nil, base.ErrLocked
		}
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
	unlockErr := syscall.Flock(int(l.file.Fd()), syscall.LOCK_UN)
	closeErr := l.file.Close()
	return errors.Join(unlockErr, closeErr)
}

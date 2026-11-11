//go:build unix

package filelock

import (
	"errors"
	"fmt"
	"os"
	"syscall"

	"github.com/akzj/ridstore/internal/base"
)

// openLockFile mirrors the POSIX lease protocol the store was designed around:
// open with O_NOFOLLOW so a symlinked LOCK cannot redirect the lease, refuse
// anything that is not a regular file, then take flock(2) LOCK_EX|LOCK_NB.
func openLockFile(path string, create bool) (*os.File, error) {
	accessFlags := syscall.O_RDONLY
	if create {
		accessFlags = syscall.O_RDWR | syscall.O_CREAT
	}
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
	return file, nil
}

// unlockFile releases the flock(2) lock. The kernel also releases it when the
// handle closes or the process exits.
func unlockFile(file *os.File) error {
	if file == nil {
		return nil
	}
	return syscall.Flock(int(file.Fd()), syscall.LOCK_UN)
}

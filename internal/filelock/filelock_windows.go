//go:build windows

package filelock

import (
	"errors"
	"fmt"
	"os"

	"golang.org/x/sys/windows"

	"github.com/akzj/ridstore/internal/base"
)

// openLockFile takes the Windows equivalent of flock(2) LOCK_EX|LOCK_NB.
//
// Windows has no flock, so the lease is a byte-range lock (LockFileEx) held on
// the LOCK file's handle. The three checks the POSIX path performs are kept:
//
//   - FILE_FLAG_OPEN_REPARSE_POINT opens a symlink or junction itself instead of
//     its target, which gives O_NOFOLLOW's behaviour; a reparse point is then
//     rejected as base.ErrCorrupt.
//   - GetFileInformationByHandle rejects directories and other non-regular
//     entries, matching the S_IFREG check.
//   - LockFileEx with LOCKFILE_EXCLUSIVE_LOCK|LOCKFILE_FAIL_IMMEDIATELY maps a
//     losing race to base.ErrLocked.
//
// The lock range is one byte at offset 0, which Windows allows to extend past
// end-of-file; LOCK stays zero-length exactly as on POSIX.
//
// Known platform difference, enforced by Windows rather than by us: byte-range
// locks belong to the process, and closing any other handle this process holds
// on the same file releases them. Nothing in ridstore opens LOCK except this
// package, so the difference is not reachable in practice.
func openLockFile(path string, create bool) (*os.File, error) {
	access := uint32(windows.GENERIC_READ)
	disposition := uint32(windows.OPEN_EXISTING)
	if create {
		access |= windows.GENERIC_WRITE
		disposition = windows.OPEN_ALWAYS
	}
	name, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return nil, err
	}
	handle, err := windows.CreateFile(name, access,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		nil, disposition, windows.FILE_ATTRIBUTE_NORMAL|windows.FILE_FLAG_OPEN_REPARSE_POINT, 0)
	if err != nil {
		return nil, err
	}
	var info windows.ByHandleFileInformation
	if err := windows.GetFileInformationByHandle(handle, &info); err != nil {
		_ = windows.CloseHandle(handle)
		return nil, err
	}
	if info.FileAttributes&windows.FILE_ATTRIBUTE_REPARSE_POINT != 0 {
		_ = windows.CloseHandle(handle)
		return nil, fmt.Errorf("LOCK must not be a symlink: %w", base.ErrCorrupt)
	}
	if info.FileAttributes&windows.FILE_ATTRIBUTE_DIRECTORY != 0 {
		_ = windows.CloseHandle(handle)
		return nil, fmt.Errorf("LOCK is not a regular file: %w", base.ErrCorrupt)
	}
	if err := windows.LockFileEx(handle, windows.LOCKFILE_EXCLUSIVE_LOCK|windows.LOCKFILE_FAIL_IMMEDIATELY, 0, 1, 0, &windows.Overlapped{}); err != nil {
		_ = windows.CloseHandle(handle)
		if errors.Is(err, windows.ERROR_LOCK_VIOLATION) {
			return nil, base.ErrLocked
		}
		return nil, err
	}
	file := os.NewFile(uintptr(handle), path)
	if file == nil {
		_ = windows.UnlockFileEx(handle, 0, 1, 0, &windows.Overlapped{})
		_ = windows.CloseHandle(handle)
		return nil, fmt.Errorf("open LOCK file")
	}
	return file, nil
}

// unlockFile releases the byte-range lock. Windows also releases it when the
// handle closes or the process exits.
func unlockFile(file *os.File) error {
	if file == nil {
		return nil
	}
	err := windows.UnlockFileEx(windows.Handle(file.Fd()), 0, 1, 0, &windows.Overlapped{})
	if errors.Is(err, windows.ERROR_NOT_LOCKED) {
		return nil
	}
	return err
}

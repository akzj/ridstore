//go:build windows

package backuprestore

import (
	"errors"
	"os"

	"golang.org/x/sys/windows"

	"github.com/akzj/ridstore/internal/base"
)

func publicationSupported() bool { return true }

// renameNoReplace publishes by moving the finished directory onto its final
// name without MOVEFILE_REPLACE_EXISTING, so the call fails instead of
// overwriting a destination that appeared after the check.
//
// This is the Windows equivalent of Linux RENAME_NOREPLACE / macOS RENAME_EXCL:
// the check and the rename happen in one kernel operation. A cross-volume
// destination is refused (ERROR_NOT_SAME_DEVICE) rather than silently copied,
// which keeps the same all-or-nothing meaning the POSIX implementations have.
func renameNoReplace(oldPath, newPath string) error {
	from, err := windows.UTF16PtrFromString(oldPath)
	if err != nil {
		return err
	}
	to, err := windows.UTF16PtrFromString(newPath)
	if err != nil {
		return err
	}
	if err := windows.MoveFileEx(from, to, 0); err != nil {
		if errors.Is(err, windows.ERROR_ALREADY_EXISTS) || errors.Is(err, windows.ERROR_FILE_EXISTS) {
			return errors.Join(base.ErrAlreadyExists, os.ErrExist, err)
		}
		return err
	}
	return nil
}

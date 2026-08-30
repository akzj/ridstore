//go:build darwin

package backuprestore

import (
	"errors"
	"os"

	"github.com/akzj/ridstore/internal/base"
	"golang.org/x/sys/unix"
)

func publicationSupported() bool { return true }

func renameNoReplace(oldPath, newPath string) error {
	err := unix.RenameatxNp(unix.AT_FDCWD, oldPath, unix.AT_FDCWD, newPath, unix.RENAME_EXCL)
	if err == nil {
		return nil
	}
	if errors.Is(err, unix.EEXIST) || errors.Is(err, unix.ENOTEMPTY) {
		return errors.Join(base.ErrAlreadyExists, os.ErrExist, err)
	}
	return err
}

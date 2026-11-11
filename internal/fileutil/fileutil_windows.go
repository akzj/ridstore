//go:build windows

package fileutil

import (
	"errors"
	"io/fs"
	"os"

	"golang.org/x/sys/windows"
)

// createFile is CreateFile with FILE_FLAG_BACKUP_SEMANTICS available for
// directory handles: the store opens directories too (to flush them and to
// enumerate entries), and Windows refuses those without that flag.
func createFile(path *uint16, access, share, disposition, attributes uint32) (windows.Handle, error) {
	handle, err := windows.CreateFile(path, access, share, nil, disposition, attributes, 0)
	if err == nil {
		return handle, nil
	}
	if !errors.Is(err, windows.ERROR_ACCESS_DENIED) {
		return 0, err
	}
	return windows.CreateFile(path, access, share, nil, disposition,
		attributes|windows.FILE_FLAG_BACKUP_SEMANTICS, 0)
}

// difference: FILE_SHARE_DELETE is part of the share mode, so a handle may
// survive a rename of the file it points at, exactly as on POSIX.
func openFile(name string, flag int, perm fs.FileMode) (*os.File, error) {
	_ = perm // Windows file attributes are not derived from the POSIX mode.

	access := uint32(windows.GENERIC_READ)
	switch {
	case flag&os.O_RDWR != 0:
		access = windows.GENERIC_READ | windows.GENERIC_WRITE
	case flag&os.O_WRONLY != 0:
		access = windows.GENERIC_WRITE
	}
	disposition := uint32(windows.OPEN_EXISTING)
	switch {
	case flag&os.O_CREATE != 0 && flag&os.O_EXCL != 0:
		disposition = windows.CREATE_NEW
	case flag&os.O_CREATE != 0 && flag&os.O_TRUNC != 0:
		disposition = windows.CREATE_ALWAYS
	case flag&os.O_CREATE != 0:
		disposition = windows.OPEN_ALWAYS
	case flag&os.O_TRUNC != 0:
		disposition = windows.TRUNCATE_EXISTING
	}

	path, err := windows.UTF16PtrFromString(name)
	if err != nil {
		return nil, &os.PathError{Op: "open", Path: name, Err: err}
	}
	handle, err := createFile(path, access,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		disposition, windows.FILE_ATTRIBUTE_NORMAL)
	if err != nil {
		return nil, &os.PathError{Op: "open", Path: name, Err: err}
	}
	file := os.NewFile(uintptr(handle), name)
	if file == nil {
		_ = windows.CloseHandle(handle)
		return nil, &os.PathError{Op: "open", Path: name, Err: windows.ERROR_INVALID_HANDLE}
	}
	if flag&os.O_APPEND != 0 {
		if _, err := file.Seek(0, 2); err != nil {
			_ = file.Close()
			return nil, &os.PathError{Op: "open", Path: name, Err: err}
		}
	}
	return file, nil
}

func open(name string) (*os.File, error) {
	path, err := windows.UTF16PtrFromString(name)
	if err != nil {
		return nil, &os.PathError{Op: "open", Path: name, Err: err}
	}
	handle, err := createFile(path, windows.GENERIC_READ,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		windows.OPEN_EXISTING, windows.FILE_ATTRIBUTE_NORMAL)
	if err != nil {
		return nil, &os.PathError{Op: "open", Path: name, Err: err}
	}
	file := os.NewFile(uintptr(handle), name)
	if file == nil {
		_ = windows.CloseHandle(handle)
		return nil, &os.PathError{Op: "open", Path: name, Err: windows.ERROR_INVALID_HANDLE}
	}
	return file, nil
}

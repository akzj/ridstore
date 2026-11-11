// Package fileutil opens store files with the sharing semantics ridstore's
// publication protocol assumes.
//
// ridstore publishes a segment by writing it under a temporary name, fsyncing
// it, renaming it into place and only then continuing to append through the
// same handle. POSIX allows that because a rename changes only the directory
// entry. Windows refuses to rename a file while a handle is open unless that
// handle was created with FILE_SHARE_DELETE, which Go's os.OpenFile does not
// request. OpenFile here requests it, so the same publication sequence works
// unchanged on every platform.
package fileutil

import (
	"io/fs"
	"os"
)

// OpenFile mirrors os.OpenFile: same flag semantics, same error wrapping.
func OpenFile(name string, flag int, perm fs.FileMode) (*os.File, error) {
	return openFile(name, flag, perm)
}

// Open mirrors os.Open: a read-only handle that also permits the file to be
// renamed or removed while it stays open.
func Open(name string) (*os.File, error) {
	return open(name)
}

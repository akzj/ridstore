//go:build unix

package fileutil

import (
	"io/fs"
	"os"
)

func openFile(name string, flag int, perm fs.FileMode) (*os.File, error) {
	return os.OpenFile(name, flag, perm)
}

func open(name string) (*os.File, error) {
	return os.Open(name)
}

package backuprestore

import (
	"io/fs"
	"os"
)

// fileBackend keeps durable filesystem operations injectable without changing
// the public Backup/Restore API. Verification deliberately continues to use
// the real read-only filesystem path after the writer has completed.
type fileBackend interface {
	lstat(string) (fs.FileInfo, error)
	readDir(string) ([]os.DirEntry, error)
	open(string) (fileHandle, error)
	openFile(string, int, fs.FileMode) (fileHandle, error)
	mkdir(string, fs.FileMode) error
	mkdirTemp(string, string) (string, error)
	remove(string) error
	removeAll(string) error
	renameNoReplace(string, string) error
}

type fileHandle interface {
	Read([]byte) (int, error)
	Write([]byte) (int, error)
	Stat() (fs.FileInfo, error)
	Sync() error
	Close() error
}

type osFileBackend struct{}

func (osFileBackend) lstat(path string) (fs.FileInfo, error)     { return os.Lstat(path) }
func (osFileBackend) readDir(path string) ([]os.DirEntry, error) { return os.ReadDir(path) }
func (osFileBackend) open(path string) (fileHandle, error)       { return os.Open(path) }
func (osFileBackend) openFile(path string, flag int, mode fs.FileMode) (fileHandle, error) {
	return os.OpenFile(path, flag, mode)
}
func (osFileBackend) mkdir(path string, mode fs.FileMode) error { return os.Mkdir(path, mode) }
func (osFileBackend) mkdirTemp(directory, pattern string) (string, error) {
	return os.MkdirTemp(directory, pattern)
}
func (osFileBackend) remove(path string) error    { return os.Remove(path) }
func (osFileBackend) removeAll(path string) error { return os.RemoveAll(path) }
func (osFileBackend) renameNoReplace(oldPath, newPath string) error {
	return renameNoReplace(oldPath, newPath)
}

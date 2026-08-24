package v2

import (
	"errors"
	"io"
	"io/fs"
	"os"
)

// fileHandle is the minimum random-access file contract used by the physical
// log. It stays internal so fault injection does not become part of the public
// append API.
type fileHandle interface {
	io.ReaderAt
	io.WriterAt
	Stat() (fs.FileInfo, error)
	Sync() error
	Truncate(int64) error
	Close() error
}

type fileBackend interface {
	open(string) (fileHandle, error)
	openFile(string, int, fs.FileMode) (fileHandle, error)
	stat(string) (fs.FileInfo, error)
	readDir(string) ([]fs.DirEntry, error)
	remove(string) error
	rename(string, string) error
	syncDir(string) error
}

type osFileBackend struct{}

func (osFileBackend) open(name string) (fileHandle, error) {
	return os.Open(name)
}

func (osFileBackend) openFile(name string, flag int, perm fs.FileMode) (fileHandle, error) {
	return os.OpenFile(name, flag, perm)
}

func (osFileBackend) stat(name string) (fs.FileInfo, error) {
	return os.Stat(name)
}

func (osFileBackend) readDir(name string) ([]fs.DirEntry, error) {
	return os.ReadDir(name)
}

func (osFileBackend) remove(name string) error {
	return os.Remove(name)
}

func (osFileBackend) rename(oldPath, newPath string) error {
	return os.Rename(oldPath, newPath)
}

func (b osFileBackend) syncDir(dir string) error {
	f, err := b.open(dir)
	if err != nil {
		return err
	}
	return errors.Join(f.Sync(), f.Close())
}

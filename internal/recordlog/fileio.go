package recordlog

import (
	"errors"
	"io"
	"io/fs"
	"os"
)

// fileHandle and fileBackend keep fault injection below the RecordLog API.
// They are deliberately package-private: storage backends are not a product
// extension point.
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
	mkdir(string, fs.FileMode) error
	stat(string) (fs.FileInfo, error)
	lstat(string) (fs.FileInfo, error)
	remove(string) error
	rename(string, string) error
	syncDir(string) error
}

type osFileBackend struct{}

func (osFileBackend) open(name string) (fileHandle, error) { return os.Open(name) }
func (osFileBackend) openFile(name string, flag int, mode fs.FileMode) (fileHandle, error) {
	return os.OpenFile(name, flag, mode)
}
func (osFileBackend) mkdir(name string, mode fs.FileMode) error { return os.Mkdir(name, mode) }
func (osFileBackend) stat(name string) (fs.FileInfo, error)     { return os.Stat(name) }
func (osFileBackend) lstat(name string) (fs.FileInfo, error)    { return os.Lstat(name) }
func (osFileBackend) remove(name string) error                  { return os.Remove(name) }
func (osFileBackend) rename(oldPath, newPath string) error      { return os.Rename(oldPath, newPath) }
func (b osFileBackend) syncDir(path string) error {
	dir, err := b.open(path)
	if err != nil {
		return err
	}
	return errors.Join(dir.Sync(), dir.Close())
}

func writeFullAt(writer io.WriterAt, value []byte, offset int64) (int, error) {
	written := 0
	for written < len(value) {
		n, err := writer.WriteAt(value[written:], offset+int64(written))
		written += n
		if err != nil {
			return written, err
		}
		if n == 0 {
			return written, io.ErrShortWrite
		}
	}
	return written, nil
}

func readFullAt(reader io.ReaderAt, value []byte, offset int64) error {
	_, err := reader.ReadAt(value, offset)
	return err
}

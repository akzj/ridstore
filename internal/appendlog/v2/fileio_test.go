package v2

import (
	"context"
	"errors"
	"io/fs"
	"sync/atomic"
	"testing"
)

type wrappingBackend struct {
	fileBackend
	wrap func(string, fileHandle) fileHandle
}

func (b *wrappingBackend) open(name string) (fileHandle, error) {
	f, err := b.fileBackend.open(name)
	if err != nil || b.wrap == nil {
		return f, err
	}
	return b.wrap(name, f), nil
}

func (b *wrappingBackend) openFile(name string, flag int, perm fs.FileMode) (fileHandle, error) {
	f, err := b.fileBackend.openFile(name, flag, perm)
	if err != nil || b.wrap == nil {
		return f, err
	}
	return b.wrap(name, f), nil
}

type shortWriteFile struct {
	fileHandle
	armed    *atomic.Bool
	maxWrite int
	writes   *atomic.Uint64
}

func (f *shortWriteFile) WriteAt(p []byte, offset int64) (int, error) {
	if f.armed.Load() && len(p) > f.maxWrite {
		p = p[:f.maxWrite]
	}
	f.writes.Add(1)
	return f.fileHandle.WriteAt(p, offset)
}

type partialFailureFile struct {
	fileHandle
	armed   *atomic.Bool
	failed  *atomic.Bool
	failure error
}

func (f *partialFailureFile) WriteAt(p []byte, offset int64) (int, error) {
	if f.armed.Load() && f.failed.CompareAndSwap(false, true) {
		const partial = 7
		n, err := f.fileHandle.WriteAt(p[:min(partial, len(p))], offset)
		return n, errors.Join(err, f.failure)
	}
	return f.fileHandle.WriteAt(p, offset)
}

type syncFailureFile struct {
	fileHandle
	armed   *atomic.Bool
	failure error
}

type metadataFailureBackend struct {
	fileBackend
	armed      *atomic.Bool
	renameErr  error
	syncDirErr error
}

func (b *metadataFailureBackend) rename(oldPath, newPath string) error {
	if b.armed.Load() && b.renameErr != nil {
		return b.renameErr
	}
	return b.fileBackend.rename(oldPath, newPath)
}

func (b *metadataFailureBackend) syncDir(dir string) error {
	if b.armed.Load() && b.syncDirErr != nil {
		return b.syncDirErr
	}
	return b.fileBackend.syncDir(dir)
}

func (f *syncFailureFile) Sync() error {
	if f.armed.Load() {
		return f.failure
	}
	return f.fileHandle.Sync()
}

func TestBackendRetriesShortWritesThroughAppendPath(t *testing.T) {
	cfg := testConfig(t)
	var armed atomic.Bool
	var writes atomic.Uint64
	cfg.files = &wrappingBackend{
		fileBackend: osFileBackend{},
		wrap: func(_ string, f fileHandle) fileHandle {
			return &shortWriteFile{fileHandle: f, armed: &armed, maxWrite: 7, writes: &writes}
		},
	}
	log, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	armed.Store(true)
	value := []byte("a record larger than one injected short write")
	addr, err := log.Append(context.Background(), value, true)
	if err != nil {
		t.Fatal(err)
	}
	if writes.Load() <= 1 {
		t.Fatalf("physical writes = %d, want multiple short writes", writes.Load())
	}
	got, err := log.Read(context.Background(), addr)
	if err != nil || string(got) != string(value) {
		t.Fatalf("read = %q, %v", got, err)
	}
	if err := log.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestBackendPartialWritePoisonsAndRecoveryRepairsTail(t *testing.T) {
	cfg := testConfig(t)
	injected := errors.New("injected partial write")
	var armed atomic.Bool
	var failed atomic.Bool
	cfg.files = &wrappingBackend{
		fileBackend: osFileBackend{},
		wrap: func(_ string, f fileHandle) fileHandle {
			return &partialFailureFile{fileHandle: f, armed: &armed, failed: &failed, failure: injected}
		},
	}
	log, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	armed.Store(true)
	if _, err := log.Append(context.Background(), []byte("partial"), true); !errors.Is(err, injected) || !errors.Is(err, ErrPoisoned) {
		t.Fatalf("append error = %v", err)
	}
	if err := log.Close(); !errors.Is(err, injected) {
		t.Fatalf("close error = %v", err)
	}

	cfg.files = osFileBackend{}
	reopened, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if _, err := reopened.Append(context.Background(), []byte("after-repair"), true); err != nil {
		t.Fatal(err)
	}
}

func TestBackendSyncFailureDoesNotAcknowledgeAppend(t *testing.T) {
	cfg := testConfig(t)
	injected := errors.New("injected sync syscall failure")
	var armed atomic.Bool
	cfg.files = &wrappingBackend{
		fileBackend: osFileBackend{},
		wrap: func(_ string, f fileHandle) fileHandle {
			return &syncFailureFile{fileHandle: f, armed: &armed, failure: injected}
		},
	}
	log, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	armed.Store(true)
	if _, err := log.Append(context.Background(), []byte("not-acknowledged"), true); !errors.Is(err, injected) || !errors.Is(err, ErrPoisoned) {
		t.Fatalf("append error = %v", err)
	}
	if err := log.Close(); !errors.Is(err, injected) {
		t.Fatalf("close error = %v", err)
	}
}

func TestBackendCreationMetadataFailuresCanRetry(t *testing.T) {
	tests := []struct {
		name       string
		renameErr  bool
		syncDirErr bool
	}{
		{name: "rename", renameErr: true},
		{name: "directory-sync", syncDirErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := testConfig(t)
			injected := errors.New("injected metadata syscall failure")
			var armed atomic.Bool
			armed.Store(true)
			backend := &metadataFailureBackend{fileBackend: osFileBackend{}, armed: &armed}
			if tc.renameErr {
				backend.renameErr = injected
			}
			if tc.syncDirErr {
				backend.syncDirErr = injected
			}
			cfg.files = backend
			if log, err := Open(cfg); !errors.Is(err, injected) {
				if log != nil {
					_ = log.Close()
				}
				t.Fatalf("open error = %v", err)
			}

			armed.Store(false)
			log, err := Open(cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer log.Close()
			if _, err := log.Append(context.Background(), []byte("after-retry"), true); err != nil {
				t.Fatal(err)
			}
		})
	}
}

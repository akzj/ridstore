package backuprestore

import (
	"context"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/akzj/ridstore/internal/coordinator"
	"github.com/akzj/ridstore/internal/engine"
	"github.com/akzj/ridstore/internal/recordlog"
	"github.com/akzj/ridstore/internal/storecatalog"
	"github.com/akzj/ridstore/internal/verifier"
)

const (
	opLstat     = "lstat"
	opReadDir   = "readdir"
	opOpen      = "open"
	opOpenFile  = "openfile"
	opMkdir     = "mkdir"
	opMkdirTemp = "mkdirtemp"
	opRemove    = "remove"
	opRename    = "rename"
	opRead      = "read"
	opWrite     = "write"
	opStat      = "stat"
	opSync      = "sync"
	opClose     = "close"
)

func TestBackupFilesystemFaultMatrix(t *testing.T) {
	root := t.TempDir()
	source := createEngineStore(t, filepath.Join(root, "source"))
	counts := make(map[string]int)
	control := &faultFileBackend{counts: counts}
	controlDestination := filepath.Join(root, "control")
	if _, err := Backup(context.Background(), Config{SourceDir: source, DestDir: controlDestination, Verify: testVerifyConfig(), files: control}); err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(controlDestination); err != nil {
		t.Fatal(err)
	}
	for _, operation := range backupFaultOperations() {
		if counts[operation] == 0 {
			t.Fatalf("operation %s was not exercised", operation)
		}
		for ordinal := 1; ordinal <= counts[operation]; ordinal++ {
			for _, failure := range faultFailures() {
				name := operation + "-" + decimal(ordinal) + "-" + failure.name
				t.Run(name, func(t *testing.T) {
					destination := filepath.Join(root, "backup-"+name)
					backend := &faultFileBackend{failOperation: operation, failOrdinal: ordinal, injected: failure.err}
					_, err := Backup(context.Background(), Config{SourceDir: source, DestDir: destination, Verify: testVerifyConfig(), files: backend})
					if !errors.Is(err, failure.err) {
						t.Fatalf("err=%v", err)
					}
					assertArtifactAbsentOrExact(t, destination)
					if report, err := verifier.Verify(context.Background(), source, testVerifyConfig()); err != nil || report.Stage != verifier.StageExact {
						t.Fatalf("source report=%+v err=%v", report, err)
					}
				})
			}
		}
	}
}

func TestRestoreFilesystemFaultMatrix(t *testing.T) {
	root := t.TempDir()
	source := createEngineStore(t, filepath.Join(root, "source"))
	artifact := filepath.Join(root, "artifact")
	if _, err := Backup(context.Background(), Config{SourceDir: source, DestDir: artifact, Verify: testVerifyConfig()}); err != nil {
		t.Fatal(err)
	}
	counts := make(map[string]int)
	control := &faultFileBackend{counts: counts}
	controlDestination := filepath.Join(root, "control")
	if _, err := Restore(context.Background(), Config{SourceDir: artifact, DestDir: controlDestination, Verify: testVerifyConfig(), files: control}); err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(controlDestination); err != nil {
		t.Fatal(err)
	}
	for _, operation := range restoreFaultOperations() {
		if counts[operation] == 0 {
			t.Fatalf("operation %s was not exercised", operation)
		}
		for ordinal := 1; ordinal <= counts[operation]; ordinal++ {
			for _, failure := range faultFailures() {
				name := operation + "-" + decimal(ordinal) + "-" + failure.name
				t.Run(name, func(t *testing.T) {
					destination := filepath.Join(root, "restore-"+name)
					backend := &faultFileBackend{failOperation: operation, failOrdinal: ordinal, injected: failure.err}
					_, err := Restore(context.Background(), Config{SourceDir: artifact, DestDir: destination, Verify: testVerifyConfig(), files: backend})
					if !errors.Is(err, failure.err) {
						t.Fatalf("err=%v", err)
					}
					assertStoreAbsentOrExact(t, destination)
				})
			}
		}
	}
}

func faultFailures() []struct {
	name string
	err  error
} {
	return []struct {
		name string
		err  error
	}{{"eio", syscall.EIO}, {"enospc", syscall.ENOSPC}, {"eacces", syscall.EACCES}}
}

func backupFaultOperations() []string {
	return []string{opLstat, opOpen, opOpenFile, opMkdir, opMkdirTemp, opRemove, opRename, opRead, opWrite, opStat, opSync, opClose}
}

func restoreFaultOperations() []string {
	return []string{opLstat, opReadDir, opOpen, opOpenFile, opMkdir, opMkdirTemp, opRemove, opRename, opRead, opWrite, opStat, opSync, opClose}
}

func TestCleanupFailurePreservesPrimaryCause(t *testing.T) {
	root := t.TempDir()
	source := createEngineStore(t, filepath.Join(root, "source"))
	artifact := filepath.Join(root, "artifact")
	if _, err := Backup(context.Background(), Config{SourceDir: source, DestDir: artifact, Verify: testVerifyConfig()}); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name        string
		source      string
		destination string
		run         func(Config) error
	}{
		{"backup", source, filepath.Join(root, "failed-backup"), func(config Config) error { _, err := Backup(context.Background(), config); return err }},
		{"restore", artifact, filepath.Join(root, "failed-restore"), func(config Config) error { _, err := Restore(context.Background(), config); return err }},
	} {
		t.Run(test.name, func(t *testing.T) {
			primary := errors.New("primary write failure")
			cleanup := errors.New("cleanup failure")
			backend := &faultFileBackend{failOperation: opOpenFile, failOrdinal: 1, injected: primary, removeAllInjected: cleanup}
			err := test.run(Config{SourceDir: test.source, DestDir: test.destination, Verify: testVerifyConfig(), files: backend})
			if !errors.Is(err, primary) || !errors.Is(err, cleanup) {
				t.Fatalf("err=%v", err)
			}
			if _, err := os.Lstat(test.destination); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("destination published: %v", err)
			}
		})
	}
}

func TestShortWriteNeverPublishes(t *testing.T) {
	root := t.TempDir()
	source := createEngineStore(t, filepath.Join(root, "source"))
	artifact := filepath.Join(root, "artifact")
	if _, err := Backup(context.Background(), Config{SourceDir: source, DestDir: artifact, Verify: testVerifyConfig()}); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name        string
		source      string
		destination string
		run         func(Config) error
	}{
		{"backup", source, filepath.Join(root, "short-backup"), func(config Config) error { _, err := Backup(context.Background(), config); return err }},
		{"restore", artifact, filepath.Join(root, "short-restore"), func(config Config) error { _, err := Restore(context.Background(), config); return err }},
	} {
		t.Run(test.name, func(t *testing.T) {
			backend := &faultFileBackend{shortWriteOrdinal: 1}
			err := test.run(Config{SourceDir: test.source, DestDir: test.destination, Verify: testVerifyConfig(), files: backend})
			if !errors.Is(err, io.ErrShortWrite) {
				t.Fatalf("err=%v", err)
			}
			if _, err := os.Lstat(test.destination); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("destination published: %v", err)
			}
		})
	}
}

func assertArtifactAbsentOrExact(t *testing.T, artifact string) {
	t.Helper()
	if _, err := os.Lstat(artifact); errors.Is(err, os.ErrNotExist) {
		return
	} else if err != nil {
		t.Fatal(err)
	}
	destination := artifact + "-restore"
	if _, err := Restore(context.Background(), Config{SourceDir: artifact, DestDir: destination, Verify: testVerifyConfig()}); err != nil {
		t.Fatalf("published artifact is not restorable: %v", err)
	}
}

func assertStoreAbsentOrExact(t *testing.T, root string) {
	t.Helper()
	if _, err := os.Lstat(root); errors.Is(err, os.ErrNotExist) {
		return
	} else if err != nil {
		t.Fatal(err)
	}
	if report, err := verifier.Verify(context.Background(), root, testVerifyConfig()); err != nil || report.Stage != verifier.StageExact {
		t.Fatalf("published store report=%+v err=%v", report, err)
	}
}

type faultFileBackend struct {
	osFileBackend
	counts            map[string]int
	failOperation     string
	failOrdinal       int
	injected          error
	removeAllInjected error
	shortWriteOrdinal int
	seen              map[string]int
}

func (f *faultFileBackend) fail(operation string) error {
	if f.seen == nil {
		f.seen = make(map[string]int)
	}
	f.seen[operation]++
	if f.counts != nil {
		f.counts[operation]++
	}
	if operation == f.failOperation && f.seen[operation] == f.failOrdinal {
		return f.injected
	}
	return nil
}

func (f *faultFileBackend) lstat(path string) (fs.FileInfo, error) {
	if err := f.fail(opLstat); err != nil {
		return nil, err
	}
	return f.osFileBackend.lstat(path)
}

func (f *faultFileBackend) readDir(path string) ([]os.DirEntry, error) {
	if err := f.fail(opReadDir); err != nil {
		return nil, err
	}
	return f.osFileBackend.readDir(path)
}

func (f *faultFileBackend) open(path string) (fileHandle, error) {
	if err := f.fail(opOpen); err != nil {
		return nil, err
	}
	file, err := f.osFileBackend.open(path)
	if err != nil {
		return nil, err
	}
	return &faultFile{fileHandle: file, backend: f}, nil
}

func (f *faultFileBackend) openFile(path string, flag int, mode fs.FileMode) (fileHandle, error) {
	if err := f.fail(opOpenFile); err != nil {
		return nil, err
	}
	file, err := f.osFileBackend.openFile(path, flag, mode)
	if err != nil {
		return nil, err
	}
	return &faultFile{fileHandle: file, backend: f}, nil
}

func (f *faultFileBackend) mkdir(path string, mode fs.FileMode) error {
	if err := f.fail(opMkdir); err != nil {
		return err
	}
	return f.osFileBackend.mkdir(path, mode)
}

func (f *faultFileBackend) mkdirTemp(directory, pattern string) (string, error) {
	if err := f.fail(opMkdirTemp); err != nil {
		return "", err
	}
	return f.osFileBackend.mkdirTemp(directory, pattern)
}

func (f *faultFileBackend) remove(path string) error {
	if err := f.fail(opRemove); err != nil {
		return err
	}
	return f.osFileBackend.remove(path)
}

func (f *faultFileBackend) removeAll(path string) error {
	if f.removeAllInjected != nil {
		return f.removeAllInjected
	}
	return f.osFileBackend.removeAll(path)
}

func (f *faultFileBackend) renameNoReplace(oldPath, newPath string) error {
	if err := f.fail(opRename); err != nil {
		return err
	}
	return f.osFileBackend.renameNoReplace(oldPath, newPath)
}

type faultFile struct {
	fileHandle
	backend *faultFileBackend
}

func (f *faultFile) Read(value []byte) (int, error) {
	if err := f.backend.fail(opRead); err != nil {
		return 0, err
	}
	return f.fileHandle.Read(value)
}

func (f *faultFile) Write(value []byte) (int, error) {
	if err := f.backend.fail(opWrite); err != nil {
		return 0, err
	}
	if f.backend.shortWriteOrdinal != 0 && f.backend.seen[opWrite] == f.backend.shortWriteOrdinal && len(value) > 1 {
		return f.fileHandle.Write(value[:len(value)/2])
	}
	return f.fileHandle.Write(value)
}

func (f *faultFile) Stat() (fs.FileInfo, error) {
	if err := f.backend.fail(opStat); err != nil {
		return nil, err
	}
	return f.fileHandle.Stat()
}

func (f *faultFile) Sync() error {
	if err := f.backend.fail(opSync); err != nil {
		return err
	}
	return f.fileHandle.Sync()
}

func (f *faultFile) Close() error {
	closeErr := f.fileHandle.Close()
	if err := f.backend.fail(opClose); err != nil {
		return errors.Join(err, closeErr)
	}
	return closeErr
}

func createEngineStore(t *testing.T, root string) string {
	t.Helper()
	store, err := engine.Create(context.Background(), root, engine.CreateConfig{
		HardLimits: storecatalog.HardLimits{
			SegmentSize: 1 << 20, MaxValueSize: 1024, MaxBatchBytes: 4096,
			MaxBatchMutations: 16, MaxBatchConditions: 16, MaxOpenBatches: 4,
			MaxRecordLogPayload: 64 << 10, IDReserveSize: 16, BatchIDReserveSize: 16,
		},
		Runtime: engine.OpenConfig{
			RecordLog:         recordlog.Config{MaxQueuedBytes: 1 << 20, QueueCapacity: 32, BufferBytes: 64 << 10, BufferRecords: 32},
			Commit:            coordinator.Config{QueueCapacity: 16, MaxGroupBatches: 8, MaxGroupPayload: 64 << 10},
			MappingCacheBytes: 1 << 20, CheckpointSortBytes: 24 << 10, MaxSegmentStats: 1024,
			DeltaSoftLimitBytes: 32 << 10, DeltaHardLimitBytes: 64 << 10,
			StatusRetention: 64, WriteStopFreeBytes: 1, SpaceCheckInterval: time.Second,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	batch, err := store.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := batch.Create(context.Background(), []byte("fault matrix")); err != nil {
		t.Fatal(err)
	}
	if _, err := batch.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	return root
}

func testVerifyConfig() verifier.Config {
	return verifier.Config{MappingCacheBytes: 1 << 20, MaxLiveIDs: 1024, MaxReplayStatuses: 1024}
}

func decimal(value int) string {
	if value == 0 {
		return "0"
	}
	buffer := [20]byte{}
	index := len(buffer)
	for value != 0 {
		index--
		buffer[index] = byte('0' + value%10)
		value /= 10
	}
	return string(buffer[index:])
}

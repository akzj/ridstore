package backuprestore_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	ridstore "github.com/akzj/ridstore"
	"github.com/akzj/ridstore/internal/backuprestore"
	"github.com/akzj/ridstore/internal/base"
	"github.com/akzj/ridstore/internal/filelock"
	"github.com/akzj/ridstore/internal/verifier"
)

func TestBackupFaultsDoNotPublishPartialArtifacts(t *testing.T) {
	root := t.TempDir()
	source := createClosedStore(t, filepath.Join(root, "source"))
	points := []backuprestore.FaultPoint{
		backuprestore.FaultBackupBeforeStaging,
		backuprestore.FaultBackupAfterIncomplete,
		backuprestore.FaultBackupBeforeFileCreate,
		backuprestore.FaultBackupBeforeFileWrite,
		backuprestore.FaultBackupBeforeFileSync,
		backuprestore.FaultBackupAfterPayload,
		backuprestore.FaultBackupAfterVerify,
		backuprestore.FaultBackupBeforeMetadata,
		backuprestore.FaultBackupAfterMetadata,
		backuprestore.FaultBackupBeforeMarkerRemove,
		backuprestore.FaultBackupBeforePublish,
	}
	injected := errors.New("injected backup fault")
	for _, point := range points {
		t.Run(string(point), func(t *testing.T) {
			destination := filepath.Join(root, "backup-"+safeName(string(point)))
			_, err := backuprestore.Backup(context.Background(), backuprestore.Config{
				SourceDir: source, DestDir: destination, Verify: verifyConfig(),
				Hook: func(got backuprestore.FaultPoint) error {
					if got == point {
						return injected
					}
					return nil
				},
			})
			if !errors.Is(err, injected) {
				t.Fatalf("err=%v", err)
			}
			if _, err := os.Lstat(destination); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("partial artifact published: %v", err)
			}
		})
	}
}

func TestBackupPostRenameFaultLeavesCompleteArtifact(t *testing.T) {
	root := t.TempDir()
	source := createClosedStore(t, filepath.Join(root, "source"))
	artifact := filepath.Join(root, "backup")
	injected := errors.New("publication uncertain")
	_, err := backuprestore.Backup(context.Background(), backuprestore.Config{
		SourceDir: source, DestDir: artifact, Verify: verifyConfig(),
		Hook: func(point backuprestore.FaultPoint) error {
			if point == backuprestore.FaultBackupAfterPublish {
				return injected
			}
			return nil
		},
	})
	if !errors.Is(err, injected) {
		t.Fatalf("err=%v", err)
	}
	destination := filepath.Join(root, "restored")
	if _, err := backuprestore.Restore(context.Background(), backuprestore.Config{SourceDir: artifact, DestDir: destination, Verify: verifyConfig()}); err != nil {
		t.Fatalf("published artifact is incomplete: %v", err)
	}
}

func TestRestoreFaultsDoNotPublishPartialStores(t *testing.T) {
	root := t.TempDir()
	source := createClosedStore(t, filepath.Join(root, "source"))
	artifact := filepath.Join(root, "backup")
	if _, err := backuprestore.Backup(context.Background(), backuprestore.Config{SourceDir: source, DestDir: artifact, Verify: verifyConfig()}); err != nil {
		t.Fatal(err)
	}
	points := []backuprestore.FaultPoint{
		backuprestore.FaultRestoreBeforeStaging,
		backuprestore.FaultRestoreBeforeFileCreate,
		backuprestore.FaultRestoreBeforeFileWrite,
		backuprestore.FaultRestoreBeforeFileSync,
		backuprestore.FaultRestoreAfterPayload,
		backuprestore.FaultRestoreAfterVerify,
		backuprestore.FaultRestoreBeforeMarkerRemove,
		backuprestore.FaultRestoreBeforePublish,
	}
	injected := errors.New("injected restore fault")
	for _, point := range points {
		t.Run(string(point), func(t *testing.T) {
			destination := filepath.Join(root, "restore-"+safeName(string(point)))
			_, err := backuprestore.Restore(context.Background(), backuprestore.Config{
				SourceDir: artifact, DestDir: destination, Verify: verifyConfig(),
				Hook: func(got backuprestore.FaultPoint) error {
					if got == point {
						return injected
					}
					return nil
				},
			})
			if !errors.Is(err, injected) {
				t.Fatalf("err=%v", err)
			}
			if _, err := os.Lstat(destination); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("partial store published: %v", err)
			}
		})
	}
}

func TestRestorePostRenameFaultLeavesExactStore(t *testing.T) {
	root := t.TempDir()
	source := createClosedStore(t, filepath.Join(root, "source"))
	artifact := filepath.Join(root, "backup")
	if _, err := backuprestore.Backup(context.Background(), backuprestore.Config{SourceDir: source, DestDir: artifact, Verify: verifyConfig()}); err != nil {
		t.Fatal(err)
	}
	destination := filepath.Join(root, "restored")
	injected := errors.New("publication uncertain")
	_, err := backuprestore.Restore(context.Background(), backuprestore.Config{
		SourceDir: artifact, DestDir: destination, Verify: verifyConfig(),
		Hook: func(point backuprestore.FaultPoint) error {
			if point == backuprestore.FaultRestoreAfterPublish {
				return injected
			}
			return nil
		},
	})
	if !errors.Is(err, injected) {
		t.Fatalf("err=%v", err)
	}
	if report, err := ridstore.Verify(context.Background(), ridstore.VerifyConfig{Dir: destination}); err != nil || report.Stage != ridstore.VerifyStageExact {
		t.Fatalf("published store report=%+v err=%v", report, err)
	}
}

func TestPublicationNeverReplacesRacingDestination(t *testing.T) {
	root := t.TempDir()
	source := createClosedStore(t, filepath.Join(root, "source"))
	destination := filepath.Join(root, "backup")
	_, err := backuprestore.Backup(context.Background(), backuprestore.Config{
		SourceDir: source, DestDir: destination, Verify: verifyConfig(),
		Hook: func(point backuprestore.FaultPoint) error {
			if point != backuprestore.FaultBackupBeforePublish {
				return nil
			}
			if err := os.Mkdir(destination, 0o700); err != nil {
				return err
			}
			return os.WriteFile(filepath.Join(destination, "keep"), []byte("keep"), 0o600)
		},
	})
	if !errors.Is(err, ridstore.ErrAlreadyExists) {
		t.Fatalf("err=%v", err)
	}
	if value, err := os.ReadFile(filepath.Join(destination, "keep")); err != nil || string(value) != "keep" {
		t.Fatalf("racing destination changed value=%q err=%v", value, err)
	}
}

func TestBackupKeepsSourceLeaseThroughPublication(t *testing.T) {
	root := t.TempDir()
	source := createClosedStore(t, filepath.Join(root, "source"))
	checked := false
	_, err := backuprestore.Backup(context.Background(), backuprestore.Config{
		SourceDir: source, DestDir: filepath.Join(root, "backup"), Verify: verifyConfig(),
		Hook: func(point backuprestore.FaultPoint) error {
			if point != backuprestore.FaultBackupBeforePublish {
				return nil
			}
			checked = true
			lock, err := filelock.AcquireExisting(source)
			if lock != nil {
				_ = lock.Close()
			}
			if !errors.Is(err, base.ErrLocked) {
				return errors.New("source lease was released before publication")
			}
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !checked {
		t.Fatal("publication hook not reached")
	}
}

func createClosedStore(t *testing.T, path string) string {
	t.Helper()
	store, err := ridstore.Create(context.Background(), ridstore.CreateConfig{Dir: path})
	if err != nil {
		t.Fatal(err)
	}
	batch, err := store.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := batch.Create(context.Background(), []byte("value")); err != nil {
		t.Fatal(err)
	}
	if _, err := batch.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	return path
}

func verifyConfig() verifier.Config {
	return verifier.Config{MappingCacheBytes: 1 << 20, MaxLiveIDs: 1024, MaxReplayStatuses: 1024}
}

func safeName(value string) string {
	value = filepath.Base(value)
	result := make([]byte, len(value))
	for index := range value {
		if value[index] == '.' {
			result[index] = '-'
		} else {
			result[index] = value[index]
		}
	}
	return string(result)
}

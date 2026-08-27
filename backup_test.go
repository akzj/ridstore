package ridstore

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestPublicBackupRestoreRoundTrip(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	config := testCreateConfig(filepath.Join(root, "source"))
	store, err := Create(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	batch, err := store.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	id, err := batch.Create(ctx, []byte("preserved"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := batch.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	original, err := store.Get(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Checkpoint(ctx); err != nil {
		t.Fatal(err)
	}
	if err := store.CompactMapping(ctx); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	backupDir := filepath.Join(root, "backup")
	backupReport, err := Backup(ctx, BackupConfig{SourceDir: config.Dir, DestDir: backupDir})
	if err != nil {
		t.Fatal(err)
	}
	if backupReport.StoreID == ([16]byte{}) || backupReport.Files < 3 || backupReport.Bytes == 0 {
		t.Fatalf("backup report=%+v", backupReport)
	}
	if _, err := os.Stat(filepath.Join(backupDir, "payload", "LOCK")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("artifact LOCK err=%v", err)
	}
	if _, err := os.Stat(filepath.Join(backupDir, "INCOMPLETE-v2")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("incomplete marker err=%v", err)
	}

	for _, name := range []string{"restored-a", "restored-b"} {
		destination := filepath.Join(root, name)
		restoreReport, err := Restore(ctx, RestoreConfig{BackupDir: backupDir, DestDir: destination})
		if err != nil {
			t.Fatal(err)
		}
		if restoreReport != backupReport {
			t.Fatalf("restore report=%+v backup report=%+v", restoreReport, backupReport)
		}
		verified, err := Verify(ctx, VerifyConfig{Dir: destination})
		if err != nil || verified.Stage != VerifyStageExact || verified.StoreID != backupReport.StoreID {
			t.Fatalf("verify=%+v err=%v", verified, err)
		}
		restored, err := Open(ctx, OpenConfig{Dir: destination, Runtime: config.Runtime})
		if err != nil {
			t.Fatal(err)
		}
		got, err := restored.Get(ctx, id)
		if err != nil || string(got.Value) != "preserved" {
			t.Fatalf("record=%+v err=%v", got, err)
		}
		update, err := restored.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if err := update.CompareAndPut(ctx, id, original.Token, []byte("updated")); err != nil {
			t.Fatal(err)
		}
		if _, err := update.Commit(ctx); err != nil {
			t.Fatal(err)
		}
		if err := restored.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestPublicBackupRequiresClosedSourceAndNewDestination(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	config := testCreateConfig(filepath.Join(root, "source"))
	store, err := Create(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	backupDir := filepath.Join(root, "backup")
	if _, err := Backup(ctx, BackupConfig{SourceDir: config.Dir, DestDir: backupDir}); !errors.Is(err, ErrLocked) {
		t.Fatalf("open source err=%v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(backupDir, 0o700); err != nil {
		t.Fatal(err)
	}
	marker := filepath.Join(backupDir, "keep")
	if err := os.WriteFile(marker, []byte("keep"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Backup(ctx, BackupConfig{SourceDir: config.Dir, DestDir: backupDir}); !errors.Is(err, ErrAlreadyExists) {
		t.Fatalf("existing destination err=%v", err)
	}
	if value, err := os.ReadFile(marker); err != nil || string(value) != "keep" {
		t.Fatalf("destination changed value=%q err=%v", value, err)
	}
}

func TestPublicRestoreRejectsArtifactMutationWithoutPublishing(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	config := testCreateConfig(filepath.Join(root, "source"))
	store, err := Create(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	backupDir := filepath.Join(root, "backup")
	if _, err := Backup(ctx, BackupConfig{SourceDir: config.Dir, DestDir: backupDir}); err != nil {
		t.Fatal(err)
	}
	manifest, err := filepath.Glob(filepath.Join(backupDir, "payload", "MANIFEST-v2-*"))
	if err != nil || len(manifest) != 1 {
		t.Fatalf("manifest=%v err=%v", manifest, err)
	}
	file, err := os.OpenFile(manifest[0], os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteAt([]byte{0xff}, 0); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	destination := filepath.Join(root, "restored")
	if _, err := Restore(ctx, RestoreConfig{BackupDir: backupDir, DestDir: destination}); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("mutated artifact err=%v", err)
	}
	if _, err := os.Lstat(destination); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("destination published err=%v", err)
	}
}

func TestPublicRestoreRejectsUnknownArtifactEntry(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	config := testCreateConfig(filepath.Join(root, "source"))
	store, err := Create(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	backupDir := filepath.Join(root, "backup")
	if _, err := Backup(ctx, BackupConfig{SourceDir: config.Dir, DestDir: backupDir}); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(backupDir, "unknown"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Restore(ctx, RestoreConfig{BackupDir: backupDir, DestDir: filepath.Join(root, "restored")}); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("unknown artifact entry err=%v", err)
	}
}

func TestPublicRestoreRejectsIncompleteAndSymlinkArtifacts(t *testing.T) {
	for _, mutation := range []string{"incomplete", "symlink"} {
		t.Run(mutation, func(t *testing.T) {
			ctx := context.Background()
			root := t.TempDir()
			config := testCreateConfig(filepath.Join(root, "source"))
			store, err := Create(ctx, config)
			if err != nil {
				t.Fatal(err)
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			backupDir := filepath.Join(root, "backup")
			if _, err := Backup(ctx, BackupConfig{SourceDir: config.Dir, DestDir: backupDir}); err != nil {
				t.Fatal(err)
			}
			switch mutation {
			case "incomplete":
				if err := os.WriteFile(filepath.Join(backupDir, "INCOMPLETE-v2"), nil, 0o600); err != nil {
					t.Fatal(err)
				}
			case "symlink":
				files, err := filepath.Glob(filepath.Join(backupDir, "payload", "records", "*.active"))
				if err != nil || len(files) != 1 {
					t.Fatalf("files=%v err=%v", files, err)
				}
				if err := os.Remove(files[0]); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(filepath.Join(config.Dir, "records", filepath.Base(files[0])), files[0]); err != nil {
					t.Fatal(err)
				}
			}
			destination := filepath.Join(root, "restored")
			if _, err := Restore(ctx, RestoreConfig{BackupDir: backupDir, DestDir: destination}); !errors.Is(err, ErrCorrupt) {
				t.Fatalf("mutation=%s err=%v", mutation, err)
			}
			if _, err := os.Lstat(destination); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("destination published err=%v", err)
			}
		})
	}
}

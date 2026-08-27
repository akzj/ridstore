package backuprestore_test

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	ridstore "github.com/akzj/ridstore"
	"github.com/akzj/ridstore/internal/backuprestore"
)

const crashExitCode = 77

func TestBackupRestoreRecoveryAcrossProcessExit(t *testing.T) {
	if mode := os.Getenv("RIDSTORE_BACKUP_CRASH_MODE"); mode != "" {
		runCrashChild(mode)
		return
	}
	t.Run("backup", func(t *testing.T) {
		for _, test := range []struct {
			point     backuprestore.FaultPoint
			published bool
			marked    bool
		}{
			{backuprestore.FaultBackupAfterIncomplete, false, true},
			{backuprestore.FaultBackupBeforeFileSync, false, true},
			{backuprestore.FaultBackupAfterMetadata, false, true},
			{backuprestore.FaultBackupBeforePublish, false, false},
			{backuprestore.FaultBackupAfterPublish, true, false},
		} {
			t.Run(string(test.point), func(t *testing.T) {
				root := t.TempDir()
				source := createClosedStore(t, filepath.Join(root, "source"))
				destination := filepath.Join(root, "backup")
				runCrashProcess(t, "backup", test.point, source, destination)
				assertPublishedArtifactState(t, root, destination, test.published, test.marked)
			})
		}
	})
	t.Run("restore", func(t *testing.T) {
		root := t.TempDir()
		source := createClosedStore(t, filepath.Join(root, "source"))
		artifact := filepath.Join(root, "backup")
		if _, err := backuprestore.Backup(context.Background(), backuprestore.Config{SourceDir: source, DestDir: artifact, Verify: verifyConfig()}); err != nil {
			t.Fatal(err)
		}
		for _, test := range []struct {
			point     backuprestore.FaultPoint
			published bool
			marked    bool
		}{
			{backuprestore.FaultRestoreBeforeFileSync, false, true},
			{backuprestore.FaultRestoreAfterPayload, false, true},
			{backuprestore.FaultRestoreAfterVerify, false, true},
			{backuprestore.FaultRestoreBeforePublish, false, false},
			{backuprestore.FaultRestoreAfterPublish, true, false},
		} {
			t.Run(string(test.point), func(t *testing.T) {
				destination := filepath.Join(root, "restore-"+safeName(string(test.point)))
				runCrashProcess(t, "restore", test.point, artifact, destination)
				assertPublishedStoreState(t, root, destination, test.published, test.marked)
			})
		}
	})
}

func runCrashChild(mode string) {
	point := backuprestore.FaultPoint(os.Getenv("RIDSTORE_BACKUP_CRASH_POINT"))
	config := backuprestore.Config{
		SourceDir: os.Getenv("RIDSTORE_BACKUP_CRASH_SOURCE"),
		DestDir:   os.Getenv("RIDSTORE_BACKUP_CRASH_DEST"),
		Verify:    verifyConfig(),
		Hook: func(got backuprestore.FaultPoint) error {
			if got == point {
				os.Exit(crashExitCode)
			}
			return nil
		},
	}
	var err error
	if mode == "backup" {
		_, err = backuprestore.Backup(context.Background(), config)
	} else {
		_, err = backuprestore.Restore(context.Background(), config)
	}
	if err != nil {
		os.Exit(78)
	}
	os.Exit(79)
}

func runCrashProcess(t *testing.T, mode string, point backuprestore.FaultPoint, source, destination string) {
	t.Helper()
	command := exec.Command(os.Args[0], "-test.run=^TestBackupRestoreRecoveryAcrossProcessExit$")
	command.Env = append(os.Environ(),
		"RIDSTORE_BACKUP_CRASH_MODE="+mode,
		"RIDSTORE_BACKUP_CRASH_POINT="+string(point),
		"RIDSTORE_BACKUP_CRASH_SOURCE="+source,
		"RIDSTORE_BACKUP_CRASH_DEST="+destination,
	)
	err := command.Run()
	var exit *exec.ExitError
	if !errors.As(err, &exit) || exit.ExitCode() != crashExitCode {
		t.Fatalf("child err=%v", err)
	}
}

func assertPublishedArtifactState(t *testing.T, root, destination string, published, marked bool) {
	t.Helper()
	_, err := os.Lstat(destination)
	if !published {
		if !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("artifact unexpectedly published: %v", err)
		}
		assertCrashStaging(t, root, ".backup.backup-", backuprestore.IncompleteName, marked)
		return
	}
	if err != nil {
		t.Fatal(err)
	}
	restored := filepath.Join(root, "from-published-artifact")
	if _, err := backuprestore.Restore(context.Background(), backuprestore.Config{SourceDir: destination, DestDir: restored, Verify: verifyConfig()}); err != nil {
		t.Fatalf("published artifact invalid: %v", err)
	}
}

func assertPublishedStoreState(t *testing.T, root, destination string, published, marked bool) {
	t.Helper()
	_, err := os.Lstat(destination)
	if !published {
		if !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("store unexpectedly published: %v", err)
		}
		assertCrashStaging(t, root, "."+filepath.Base(destination)+".restore-", backuprestore.RestoreIncompleteName, marked)
		return
	}
	if err != nil {
		t.Fatal(err)
	}
	if report, err := ridstore.Verify(context.Background(), ridstore.VerifyConfig{Dir: destination}); err != nil || report.Stage != ridstore.VerifyStageExact {
		t.Fatalf("published store report=%+v err=%v", report, err)
	}
}

func assertCrashStaging(t *testing.T, root, prefix, marker string, marked bool) {
	t.Helper()
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	found := 0
	for _, entry := range entries {
		if !strings.HasPrefix(entry.Name(), prefix) {
			continue
		}
		found++
		path := filepath.Join(root, entry.Name())
		_, markerErr := os.Lstat(filepath.Join(path, marker))
		if marked && markerErr != nil {
			t.Fatalf("missing crash marker: %v", markerErr)
		}
		if !marked && !errors.Is(markerErr, os.ErrNotExist) {
			t.Fatalf("unexpected crash marker: %v", markerErr)
		}
	}
	if found != 1 {
		t.Fatalf("staging directories=%d entries=%v", found, entries)
	}
}

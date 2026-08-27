package storecatalog

import (
	"errors"
	"os"
	"testing"
)

func TestInstallAndLoadNewestManifest(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	first := testManifest()
	if err := Install(root, first, nil); err != nil {
		t.Fatal(err)
	}
	second := first.Clone()
	second.Generation = 2
	second.ReservedIDHigh++
	if err := Install(root, second, nil); err != nil {
		t.Fatal(err)
	}
	got, err := Load(root)
	if err != nil || got.Generation != 2 || got.ReservedIDHigh != second.ReservedIDHigh {
		t.Fatalf("got generation=%d high=%d err=%v", got.Generation, got.ReservedIDHigh, err)
	}
}

func TestFailedInstallLeavesPreviousManifestAuthoritative(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	first := testManifest()
	if err := Install(root, first, nil); err != nil {
		t.Fatal(err)
	}
	second := first.Clone()
	second.Generation = 2
	wantErr := errors.New("injected")
	err := Install(root, second, func(point FaultPoint) error {
		if point == FaultBeforeManifestRename {
			return wantErr
		}
		return nil
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("install err=%v", err)
	}
	got, err := Load(root)
	if err != nil || got.Generation != 1 {
		t.Fatalf("got generation=%d err=%v", got.Generation, err)
	}
	if _, err := os.Stat(manifestTempPath(root, 0)); err != nil {
		t.Fatalf("expected recoverable temp file: %v", err)
	}
}

func TestLoadRecoveringRemovesUnpublishedManifestTemp(t *testing.T) {
	root := t.TempDir()
	manifest := testManifest()
	if err := Install(root, manifest, nil); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(manifestTempPath(root, 0), []byte("unpublished"), 0o600); err != nil {
		t.Fatal(err)
	}
	got, err := LoadRecovering(root, nil)
	if err != nil || got.Generation != manifest.Generation {
		t.Fatalf("generation=%d err=%v", got.Generation, err)
	}
	if _, err := os.Lstat(manifestTempPath(root, 0)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("temp err=%v", err)
	}
	if _, err := LoadStrict(root); err != nil {
		t.Fatalf("strict load after recovery: %v", err)
	}
}

func TestLoadRecoveringRejectsUnsafeManifestTemp(t *testing.T) {
	root := t.TempDir()
	manifest := testManifest()
	if err := Install(root, manifest, nil); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(manifestTempPath(root, 0), 0o700); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadRecovering(root, nil); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("recover err=%v", err)
	}
}

func TestLoadRecoveringCleanupFaultsConvergeOnRetry(t *testing.T) {
	for _, point := range []FaultPoint{FaultBeforeTempRemove, FaultBeforeTempDirSync} {
		t.Run(string(point), func(t *testing.T) {
			root := t.TempDir()
			manifest := testManifest()
			if err := Install(root, manifest, nil); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(manifestTempPath(root, 0), []byte("unpublished"), 0o600); err != nil {
				t.Fatal(err)
			}
			injected := errors.New("injected")
			if _, err := LoadRecovering(root, func(got FaultPoint) error {
				if got == point {
					return injected
				}
				return nil
			}); !errors.Is(err, injected) {
				t.Fatalf("recover err=%v", err)
			}
			if _, err := LoadRecovering(root, nil); err != nil {
				t.Fatal(err)
			}
			if _, err := LoadStrict(root); err != nil {
				t.Fatalf("strict load after retry: %v", err)
			}
		})
	}
}

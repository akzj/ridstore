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

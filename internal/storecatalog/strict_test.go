package storecatalog

import (
	"errors"
	"os"
	"testing"
)

func TestLoadStrictRejectsCorruptInactiveSlot(t *testing.T) {
	root := t.TempDir()
	first := testManifest()
	if err := Install(root, first, nil); err != nil {
		t.Fatal(err)
	}
	second := first.Clone()
	second.Generation++
	if err := Install(root, second, nil); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(manifestSlotPath(root, first.Generation&1), []byte("broken"), 0o600); err != nil {
		t.Fatal(err)
	}
	if got, err := Load(root); err != nil || got.Generation != second.Generation {
		t.Fatalf("normal load=%d err=%v", got.Generation, err)
	}
	if _, err := LoadStrict(root); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("strict load err=%v", err)
	}
}

func TestLoadStrictRejectsManifestTemp(t *testing.T) {
	root := t.TempDir()
	manifest := testManifest()
	if err := Install(root, manifest, nil); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(manifestTempPath(root, 0), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadStrict(root); !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("strict load err=%v", err)
	}
}

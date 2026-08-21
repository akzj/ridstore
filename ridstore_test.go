package ridstore

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestCreateOpenLockAndConfig(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "store")
	store, err := Create(Config{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Open(Config{Dir: dir}); !errors.Is(err, ErrLocked) {
		t.Fatalf("concurrent Open error=%v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); !errors.Is(err, ErrClosed) {
		t.Fatalf("second Close error=%v", err)
	}
	if _, err := Create(Config{Dir: dir}); !errors.Is(err, ErrAlreadyExists) {
		t.Fatalf("second Create error=%v", err)
	}
	reopened, err := Open(Config{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	if reopened.config.SegmentSize != 256*mib || reopened.manifest.Generation != 1 {
		t.Fatalf("config=%+v manifest=%+v", reopened.config, reopened.manifest)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(Config{Dir: dir, SegmentSize: 128 * mib}); !errors.Is(err, ErrConfigMismatch) {
		t.Fatalf("mismatched Open error=%v", err)
	}
}

func TestOpenDoesNotCreate(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "missing")
	if _, err := Open(Config{Dir: dir}); !errors.Is(err, ErrNotInitialized) {
		t.Fatalf("error=%v", err)
	}
	if _, err := os.Stat(dir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("Open created directory: %v", err)
	}
}

func TestCreateRejectsNonEmptyAndSymlinkDirectory(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "foreign"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Create(Config{Dir: dir}); !errors.Is(err, ErrAlreadyExists) {
		t.Fatalf("non-empty Create error=%v", err)
	}
	link := filepath.Join(t.TempDir(), "store-link")
	if err := os.Symlink(dir, link); err != nil {
		t.Fatal(err)
	}
	if _, err := Create(Config{Dir: link}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("symlink Create error=%v", err)
	}
}

func TestConfigValidation(t *testing.T) {
	cases := []Config{
		{Dir: t.TempDir(), SegmentSize: -1},
		{Dir: t.TempDir(), SegmentSize: 8 << 20, MaxValueSize: 16 << 20},
		{Dir: t.TempDir(), DeltaSoftLimitBytes: 2, DeltaHardLimitBytes: 1},
		{Dir: t.TempDir(), MaxBatchBytes: 1 << 20, GCBatchBytes: 2 << 20},
		{Dir: t.TempDir(), MaxGroupDelay: -1},
	}
	for i, cfg := range cases {
		if _, _, err := normalizeCreateConfig(cfg); !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("case %d error=%v", i, err)
		}
	}
}

func TestConfigAllowsExactFourGiBSegment(t *testing.T) {
	cfg, hard, err := normalizeCreateConfig(Config{Dir: t.TempDir(), SegmentSize: int64(1) << 32})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.SegmentSize != int64(1)<<32 || hard.SegmentSize != uint64(1)<<32 {
		t.Fatalf("config=%d hard=%d", cfg.SegmentSize, hard.SegmentSize)
	}
}

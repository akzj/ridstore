package filelock

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/akzj/ridstore/internal/base"
)

func TestAcquireExclusiveAndRelease(t *testing.T) {
	dir := t.TempDir()
	first, err := Acquire(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Acquire(dir); !errors.Is(err, base.ErrLocked) {
		t.Fatalf("second acquire error=%v", err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	second, err := Acquire(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := second.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestAcquireRejectsNonRegularLock(t *testing.T) {
	dir := t.TempDir()
	if err := os.Mkdir(filepath.Join(dir, FileName), 0o700); err != nil {
		t.Fatal(err)
	}
	if _, err := Acquire(dir); err == nil {
		t.Fatal("directory LOCK accepted as a lease")
	}
}

func TestAcquireExistingDoesNotCreateLock(t *testing.T) {
	dir := t.TempDir()
	if _, err := AcquireExisting(dir); err == nil {
		t.Fatal("AcquireExisting accepted a store without LOCK")
	}
	if _, err := os.Stat(filepath.Join(dir, FileName)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("AcquireExisting must not create LOCK, stat error=%v", err)
	}
}

func TestAcquireRejectsSymlinkLock(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "target")
	if err := os.WriteFile(target, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, filepath.Join(dir, FileName)); err != nil {
		// Windows only creates symlinks with SeCreateSymbolicLinkPrivilege or
		// Developer Mode; the rejection itself is still enforced by opening
		// LOCK with FILE_FLAG_OPEN_REPARSE_POINT on any host that can create one.
		t.Skipf("cannot create symlink on this platform: %v", err)
	}
	if _, err := Acquire(dir); err == nil {
		t.Fatal("symlink LOCK accepted")
	}
}

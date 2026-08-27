package storecatalog

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

type FaultPoint string

const (
	FaultBeforeManifestWrite   FaultPoint = "storecatalog.before-write"
	FaultBeforeManifestSync    FaultPoint = "storecatalog.before-sync"
	FaultBeforeManifestRename  FaultPoint = "storecatalog.before-rename"
	FaultBeforeManifestDirSync FaultPoint = "storecatalog.before-dir-sync"
	FaultBeforeTempRemove      FaultPoint = "storecatalog.before-temp-remove"
	FaultBeforeTempDirSync     FaultPoint = "storecatalog.before-temp-dir-sync"
)

type FaultHook func(FaultPoint) error

func manifestSlotPath(root string, slot uint64) string {
	return filepath.Join(root, fmt.Sprintf("MANIFEST-v2-%d", slot))
}

func manifestTempPath(root string, slot uint64) string {
	return manifestSlotPath(root, slot) + ".tmp"
}

func Install(root string, manifest Manifest, hook FaultHook) error {
	if root == "" {
		return ErrInvalid
	}
	encoded, err := Encode(manifest)
	if err != nil {
		return err
	}
	slot := manifest.Generation & 1
	tempPath := manifestTempPath(root, slot)
	finalPath := manifestSlotPath(root, slot)
	removed, err := removeManifestTemp(tempPath, hook)
	if err != nil {
		return err
	}
	if removed {
		if err := syncManifestDir(root, hook, FaultBeforeTempDirSync); err != nil {
			return err
		}
	}
	file, err := os.OpenFile(tempPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	fail := func(cause error) error { return errors.Join(cause, file.Close()) }
	if err := hit(hook, FaultBeforeManifestWrite); err != nil {
		return fail(err)
	}
	if _, err := writeFull(file, encoded); err != nil {
		return fail(err)
	}
	if err := hit(hook, FaultBeforeManifestSync); err != nil {
		return fail(err)
	}
	if err := file.Sync(); err != nil {
		return fail(err)
	}
	if err := file.Close(); err != nil {
		return err
	}
	if err := hit(hook, FaultBeforeManifestRename); err != nil {
		return err
	}
	if err := os.Rename(tempPath, finalPath); err != nil {
		return err
	}
	dir, err := os.Open(root)
	if err != nil {
		return err
	}
	if err := hit(hook, FaultBeforeManifestDirSync); err != nil {
		return errors.Join(err, dir.Close())
	}
	return errors.Join(dir.Sync(), dir.Close())
}

func Load(root string) (Manifest, error) {
	return load(root, false)
}

// LoadRecovering loads the authoritative Manifest and removes unpublished
// regular temp slots left by an interrupted install. Callers must hold the
// directory lease so cleanup cannot race another writer.
func LoadRecovering(root string, hook FaultHook) (Manifest, error) {
	manifest, err := Load(root)
	if err != nil {
		return Manifest{}, err
	}
	for slot := uint64(0); slot < 2; slot++ {
		_, err := removeManifestTemp(manifestTempPath(root, slot), hook)
		if err != nil {
			return Manifest{}, err
		}
	}
	// Always sync: a prior process may have removed a temp slot and then
	// observed an uncertain directory-sync result. Absence alone does not prove
	// that deletion durable.
	if err := syncManifestDir(root, hook, FaultBeforeTempDirSync); err != nil {
		return Manifest{}, err
	}
	return manifest, nil
}

// LoadStrict loads the authoritative Manifest for offline verification. Unlike
// normal recovery Load, every present slot must decode and no unpublished temp
// slot may remain.
func LoadStrict(root string) (Manifest, error) {
	if root == "" {
		return Manifest{}, ErrInvalid
	}
	return load(root, true)
}

// InspectLatestHeader strictly reads the two durable Manifest slots without
// requiring a decoder for their declared format version.
func InspectLatestHeader(root string) (ContainerHeader, error) {
	if root == "" {
		return ContainerHeader{}, ErrInvalid
	}
	type candidate struct {
		header  ContainerHeader
		encoded []byte
	}
	values := make([]candidate, 0, 2)
	for slot := uint64(0); slot < 2; slot++ {
		if _, err := os.Lstat(manifestTempPath(root, slot)); err == nil {
			return ContainerHeader{}, ErrRecoveryRequired
		} else if !errors.Is(err, os.ErrNotExist) {
			return ContainerHeader{}, err
		}
		info, err := os.Lstat(manifestSlotPath(root, slot))
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return ContainerHeader{}, err
		}
		if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
			return ContainerHeader{}, ErrCorrupt
		}
		if info.Size() < containerHeaderSize || info.Size() > containerHeaderSize+maxManifestPayload {
			return ContainerHeader{}, ErrCorrupt
		}
		encoded, err := os.ReadFile(manifestSlotPath(root, slot))
		if err != nil {
			return ContainerHeader{}, err
		}
		header, err := InspectHeader(encoded)
		if err != nil {
			return ContainerHeader{}, err
		}
		if header.Generation&1 != slot {
			return ContainerHeader{}, fmt.Errorf("manifest slot generation mismatch: %w", ErrCorrupt)
		}
		values = append(values, candidate{header: header, encoded: encoded})
	}
	if len(values) == 0 {
		return ContainerHeader{}, os.ErrNotExist
	}
	if len(values) == 2 {
		if values[0].header.StoreUUID != values[1].header.StoreUUID {
			return ContainerHeader{}, fmt.Errorf("manifest slots disagree on store identity: %w", ErrCorrupt)
		}
		if values[0].header.Generation == values[1].header.Generation {
			if string(values[0].encoded) != string(values[1].encoded) {
				return ContainerHeader{}, fmt.Errorf("equal manifest generations differ: %w", ErrCorrupt)
			}
			return values[0].header, nil
		}
		if values[1].header.Generation > values[0].header.Generation {
			return values[1].header, nil
		}
	}
	return values[0].header, nil
}

func load(root string, strict bool) (Manifest, error) {
	type candidate struct {
		manifest Manifest
		encoded  []byte
	}
	values := make([]candidate, 0, 2)
	var failures error
	for slot := uint64(0); slot < 2; slot++ {
		if strict {
			if _, err := os.Lstat(manifestTempPath(root, slot)); err == nil {
				return Manifest{}, ErrRecoveryRequired
			} else if !errors.Is(err, os.ErrNotExist) {
				return Manifest{}, err
			}
			info, err := os.Lstat(manifestSlotPath(root, slot))
			if err == nil && (!info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0) {
				return Manifest{}, ErrCorrupt
			}
			if err != nil && !errors.Is(err, os.ErrNotExist) {
				return Manifest{}, err
			}
		}
		encoded, err := os.ReadFile(manifestSlotPath(root, slot))
		if err != nil {
			if strict && !errors.Is(err, os.ErrNotExist) {
				return Manifest{}, err
			}
			if !errors.Is(err, os.ErrNotExist) {
				failures = errors.Join(failures, err)
			}
			continue
		}
		manifest, err := Decode(encoded)
		if err != nil {
			if strict {
				return Manifest{}, err
			}
			failures = errors.Join(failures, err)
			continue
		}
		values = append(values, candidate{manifest: manifest, encoded: encoded})
	}
	if len(values) == 0 {
		if failures != nil {
			return Manifest{}, failures
		}
		return Manifest{}, os.ErrNotExist
	}
	if len(values) == 2 && values[0].manifest.Generation == values[1].manifest.Generation {
		if string(values[0].encoded) != string(values[1].encoded) {
			return Manifest{}, fmt.Errorf("equal manifest generations differ: %w", ErrCorrupt)
		}
		return values[0].manifest.Clone(), nil
	}
	if len(values) == 2 && values[1].manifest.Generation > values[0].manifest.Generation {
		return values[1].manifest.Clone(), nil
	}
	return values[0].manifest.Clone(), nil
}

func writeFull(writer io.Writer, value []byte) (int, error) {
	written := 0
	for written < len(value) {
		n, err := writer.Write(value[written:])
		written += n
		if err != nil {
			return written, err
		}
		if n == 0 {
			return written, io.ErrShortWrite
		}
	}
	return written, nil
}

func hit(hook FaultHook, point FaultPoint) error {
	if hook == nil {
		return nil
	}
	return hook(point)
}

func removeManifestTemp(path string, hook FaultHook) (bool, error) {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
		return false, ErrCorrupt
	}
	if err := hit(hook, FaultBeforeTempRemove); err != nil {
		return false, err
	}
	return true, os.Remove(path)
}

func syncManifestDir(root string, hook FaultHook, point FaultPoint) error {
	if err := hit(hook, point); err != nil {
		return err
	}
	dir, err := os.Open(root)
	if err != nil {
		return err
	}
	return errors.Join(dir.Sync(), dir.Close())
}

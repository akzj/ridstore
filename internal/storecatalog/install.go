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
	file, err := os.OpenFile(tempPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
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
	type candidate struct {
		manifest Manifest
		encoded  []byte
	}
	values := make([]candidate, 0, 2)
	var failures error
	for slot := uint64(0); slot < 2; slot++ {
		encoded, err := os.ReadFile(manifestSlotPath(root, slot))
		if err != nil {
			if !errors.Is(err, os.ErrNotExist) {
				failures = errors.Join(failures, err)
			}
			continue
		}
		manifest, err := Decode(encoded)
		if err != nil {
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

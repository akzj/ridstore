package manifest

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/akzj/ridstore/internal/base"
	"github.com/akzj/ridstore/internal/failpoint"
	storeformat "github.com/akzj/ridstore/internal/format"
)

const (
	ManifestDirName = "manifests"
	CurrentFileName = "CURRENT"
)

type Step string

const (
	StepManifestWritten    Step = "manifest-written"
	StepManifestFileSynced Step = "manifest-file-synced"
	StepManifestRenamed    Step = "manifest-renamed"
	StepManifestDirSynced  Step = "manifest-dir-synced"
	StepCurrentWritten     Step = "current-written"
	StepCurrentFileSynced  Step = "current-file-synced"
	StepCurrentRenamed     Step = "current-renamed"
	StepRootDirSynced      Step = "root-dir-synced"
)

type Hook func(Step) error

const (
	PointBeforeManifestWrite    failpoint.Point = "manifest.before-manifest-write"
	PointBeforeManifestFileSync failpoint.Point = "manifest.before-manifest-file-sync"
	PointBeforeManifestRename   failpoint.Point = "manifest.before-manifest-rename"
	PointBeforeManifestDirSync  failpoint.Point = "manifest.before-manifest-dir-sync"
	PointBeforeCurrentWrite     failpoint.Point = "manifest.before-current-write"
	PointBeforeCurrentFileSync  failpoint.Point = "manifest.before-current-file-sync"
	PointBeforeCurrentRename    failpoint.Point = "manifest.before-current-rename"
	PointBeforeRootDirSync      failpoint.Point = "manifest.before-root-dir-sync"
)

type Installer struct {
	Dir           string
	Hook          Hook
	FailpointHook failpoint.Hook
}

func ManifestFileName(generation uint64) string {
	return fmt.Sprintf("MANIFEST-%020d", generation)
}

func (i Installer) Install(m storeformat.Manifest) error {
	encoded, err := storeformat.EncodeManifest(m)
	if err != nil {
		return err
	}
	if err := ensureDirectory(i.Dir); err != nil {
		return err
	}
	manifestDir := filepath.Join(i.Dir, ManifestDirName)
	if err := ensureDirectory(manifestDir); err != nil {
		return err
	}
	if currentName, currentErr := ReadCurrentName(i.Dir); currentErr == nil {
		currentGeneration, _ := ParseManifestFileName(currentName)
		if currentGeneration > m.Generation {
			return fmt.Errorf("manifest generation rollback from %d to %d: %w", currentGeneration, m.Generation, base.ErrCorrupt)
		}
		currentManifest, loadErr := LoadCurrent(i.Dir)
		if loadErr != nil {
			return loadErr
		}
		if currentManifest.StoreUUID != m.StoreUUID {
			return fmt.Errorf("manifest StoreUUID mismatch: %w", base.ErrCorrupt)
		}
		if currentGeneration < m.Generation {
			next, addErr := base.AddUint64(currentGeneration, 1)
			if addErr != nil || next != m.Generation {
				return fmt.Errorf("non-consecutive manifest generation: %w", base.ErrCorrupt)
			}
		}
	} else if !errors.Is(currentErr, os.ErrNotExist) {
		return currentErr
	} else if m.Generation != 1 {
		return fmt.Errorf("initial manifest generation must be 1: %w", base.ErrCorrupt)
	}
	name := ManifestFileName(m.Generation)
	finalPath := filepath.Join(manifestDir, name)
	tempPath := finalPath + ".tmp"
	if existing, readErr := readRegularFile(finalPath, storeformat.MaxManifestPayloadSize+storeformat.ContainerHeaderSize); readErr == nil {
		if !bytes.Equal(existing, encoded) {
			return fmt.Errorf("manifest generation %d already exists with different content: %w", m.Generation, base.ErrCorrupt)
		}
	} else if !errors.Is(readErr, os.ErrNotExist) {
		return readErr
	} else {
		if err := writeSyncedFile(tempPath, encoded, 0o600, i.before, i.hit,
			PointBeforeManifestWrite, StepManifestWritten, PointBeforeManifestFileSync, StepManifestFileSynced); err != nil {
			return err
		}
		if err := i.before(PointBeforeManifestRename); err != nil {
			return err
		}
		if err := os.Rename(tempPath, finalPath); err != nil {
			return err
		}
		if err := i.hit(StepManifestRenamed); err != nil {
			return err
		}
	}
	// Always sync the directory, including idempotent recovery after a crash
	// between rename and directory fsync.
	if err := i.before(PointBeforeManifestDirSync); err != nil {
		return err
	}
	if err := syncDirectory(manifestDir); err != nil {
		return err
	}
	if err := i.hit(StepManifestDirSynced); err != nil {
		return err
	}

	current := []byte(name + "\n")
	currentTemp := filepath.Join(i.Dir, ".CURRENT.tmp")
	if err := writeSyncedFile(currentTemp, current, 0o600, i.before, i.hit,
		PointBeforeCurrentWrite, StepCurrentWritten, PointBeforeCurrentFileSync, StepCurrentFileSynced); err != nil {
		return err
	}
	if err := i.before(PointBeforeCurrentRename); err != nil {
		return err
	}
	if err := os.Rename(currentTemp, filepath.Join(i.Dir, CurrentFileName)); err != nil {
		return err
	}
	if err := i.hit(StepCurrentRenamed); err != nil {
		return err
	}
	if err := i.before(PointBeforeRootDirSync); err != nil {
		return err
	}
	if err := syncDirectory(i.Dir); err != nil {
		return err
	}
	return i.hit(StepRootDirSynced)
}

func LoadCurrent(dir string) (storeformat.Manifest, error) {
	name, err := ReadCurrentName(dir)
	if err != nil {
		return storeformat.Manifest{}, err
	}
	generation, err := ParseManifestFileName(name)
	if err != nil {
		return storeformat.Manifest{}, err
	}
	data, err := readRegularFile(filepath.Join(dir, ManifestDirName, name), storeformat.MaxManifestPayloadSize+storeformat.ContainerHeaderSize)
	if err != nil {
		return storeformat.Manifest{}, err
	}
	m, err := storeformat.DecodeManifest(data)
	if err != nil {
		return storeformat.Manifest{}, err
	}
	if m.Generation != generation {
		return storeformat.Manifest{}, fmt.Errorf("CURRENT manifest generation mismatch: %w", base.ErrCorrupt)
	}
	return m, nil
}

// CleanupInterruptedInstall removes unpublished temp names and final Manifest
// generations newer than a valid CURRENT. Older/final published generations
// are retained; CURRENT is the sole publication authority.
func CleanupInterruptedInstall(dir string) error {
	current, err := LoadCurrent(dir)
	if err != nil {
		return err
	}
	rootDirty := false
	currentTemp := filepath.Join(dir, ".CURRENT.tmp")
	if found, err := removeRegularTemp(currentTemp); err != nil {
		return err
	} else {
		rootDirty = found
	}
	manifestDir := filepath.Join(dir, ManifestDirName)
	entries, err := os.ReadDir(manifestDir)
	if err != nil {
		return err
	}
	manifestDirty := false
	for _, entry := range entries {
		name := entry.Name()
		candidate := name
		isTemp := strings.HasSuffix(name, ".tmp")
		if isTemp {
			candidate = strings.TrimSuffix(name, ".tmp")
		}
		generation, err := ParseManifestFileName(candidate)
		if err != nil || !isTemp && generation <= current.Generation {
			continue
		}
		found, err := removeRegularTemp(filepath.Join(manifestDir, name))
		if err != nil {
			return err
		}
		manifestDirty = manifestDirty || found
	}
	if manifestDirty {
		if err := syncDirectory(manifestDir); err != nil {
			return err
		}
	}
	if rootDirty {
		return syncDirectory(dir)
	}
	return nil
}

func removeRegularTemp(path string) (bool, error) {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
		return false, fmt.Errorf("install temp is not a regular file: %s: %w", path, base.ErrCorrupt)
	}
	if err := os.Remove(path); err != nil {
		return false, err
	}
	return true, nil
}

func ReadCurrentName(dir string) (string, error) {
	data, err := readRegularFile(filepath.Join(dir, CurrentFileName), 128)
	if err != nil {
		return "", err
	}
	if len(data) == 0 || len(data) > 128 || data[len(data)-1] != '\n' || bytes.Count(data, []byte{'\n'}) != 1 {
		return "", fmt.Errorf("invalid CURRENT: %w", base.ErrCorrupt)
	}
	name := string(data[:len(data)-1])
	if filepath.Base(name) != name {
		return "", fmt.Errorf("invalid CURRENT path: %w", base.ErrCorrupt)
	}
	if _, err := ParseManifestFileName(name); err != nil {
		return "", err
	}
	return name, nil
}

func ParseManifestFileName(name string) (uint64, error) {
	const prefix = "MANIFEST-"
	if !strings.HasPrefix(name, prefix) || len(name) != len(prefix)+20 {
		return 0, fmt.Errorf("invalid manifest filename: %w", base.ErrCorrupt)
	}
	generation, err := strconv.ParseUint(name[len(prefix):], 10, 64)
	if err != nil || generation == 0 || ManifestFileName(generation) != name {
		return 0, fmt.Errorf("invalid manifest generation filename: %w", base.ErrCorrupt)
	}
	return generation, nil
}

func writeSyncedFile(path string, data []byte, mode os.FileMode, before func(failpoint.Point) error, after Hook,
	beforeWrite failpoint.Point, written Step, beforeSync failpoint.Point, synced Step,
) (retErr error) {
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, mode)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := file.Close(); retErr == nil && closeErr != nil {
			retErr = closeErr
		}
	}()
	if err := before(beforeWrite); err != nil {
		return err
	}
	if _, err := io.Copy(file, bytes.NewReader(data)); err != nil {
		return err
	}
	if err := runHook(after, written); err != nil {
		return err
	}
	if err := before(beforeSync); err != nil {
		return err
	}
	if err := file.Sync(); err != nil {
		return err
	}
	return runHook(after, synced)
}

func readRegularFile(path string, maxSize int64) ([]byte, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
		return nil, fmt.Errorf("not a regular file: %s", path)
	}
	if info.Size() < 0 || info.Size() > maxSize {
		return nil, fmt.Errorf("file exceeds size limit: %s: %w", path, base.ErrCorrupt)
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	openedInfo, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if !os.SameFile(info, openedInfo) || !openedInfo.Mode().IsRegular() || openedInfo.Size() > maxSize {
		return nil, fmt.Errorf("file identity changed while opening: %s: %w", path, base.ErrCorrupt)
	}
	data, err := io.ReadAll(io.LimitReader(file, maxSize+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > maxSize {
		return nil, fmt.Errorf("file exceeds size limit while reading: %s: %w", path, base.ErrCorrupt)
	}
	return data, nil
}

func ensureDirectory(path string) error {
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("not a real directory: %s", path)
	}
	return nil
}

func syncDirectory(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	if err := dir.Sync(); err != nil {
		_ = dir.Close()
		return err
	}
	return dir.Close()
}

func runHook(hook Hook, step Step) error {
	if hook == nil {
		return nil
	}
	return hook(step)
}

// FailpointHook adapts the stable manifest step names to the shared failpoint
// interface used by subprocess crash tests.
func (i Installer) hit(step Step) error {
	if err := failpoint.Hit(i.FailpointHook, failpoint.Point("manifest."+string(step))); err != nil {
		return err
	}
	return runHook(i.Hook, step)
}

func (i Installer) before(point failpoint.Point) error {
	return failpoint.Hit(i.FailpointHook, point)
}

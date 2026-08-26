package bootstrap

import (
	"bytes"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/akzj/ridstore/internal/base"
	"github.com/akzj/ridstore/internal/filelock"
	"github.com/akzj/ridstore/internal/mapstore"
	"github.com/akzj/ridstore/internal/recordlog"
	"github.com/akzj/ridstore/internal/storecatalog"
)

const (
	MarkerName     = "INITIALIZING-v2"
	markerTempName = ".INITIALIZING-v2.tmp"
	maxMarkerBytes = 4096
)

type FaultPoint string

const (
	FaultBeforeMarkerWrite   FaultPoint = "bootstrap.before-marker-write"
	FaultBeforeMarkerSync    FaultPoint = "bootstrap.before-marker-sync"
	FaultBeforeMarkerRename  FaultPoint = "bootstrap.before-marker-rename"
	FaultBeforeMarkerDirSync FaultPoint = "bootstrap.before-marker-dir-sync"
	FaultBeforeDataSegment   FaultPoint = "bootstrap.before-data-segment"
	FaultBeforeMapSegment    FaultPoint = "bootstrap.before-map-segment"
	FaultBeforeManifest      FaultPoint = "bootstrap.before-manifest"
	FaultBeforeMarkerRemove  FaultPoint = "bootstrap.before-marker-remove"
	FaultBeforeFinalDirSync  FaultPoint = "bootstrap.before-final-dir-sync"
)

type FaultHook func(FaultPoint) error

func ValidateHardLimits(hard storecatalog.HardLimits) error {
	replayStart, _ := recordlog.NewLogPos(1, recordlog.SegmentHeaderSize)
	manifest := storecatalog.Manifest{
		Generation: 1, StoreUUID: storecatalog.StoreUUID{1}, HardLimits: hard, RecordLogID: recordlog.LogID{1},
		ActiveDataSegmentID: 1, NextDataSegmentID: 2, ActiveMapSegmentID: 1, NextMapSegmentID: 2,
		ReplayStart: replayStart, ReservedIDHigh: 1, ReservedBatchIDHigh: 1, IssuedBatchIDHighAtCut: 1,
	}
	if err := storecatalog.Validate(manifest); err != nil {
		return errors.Join(base.ErrInvalidConfig, err)
	}
	return nil
}

// EnsureRoot creates only the final store directory, never its parents. A
// newly created directory is made durable before callers create LOCK in it.
func EnsureRoot(root string) error {
	if root == "" {
		return base.ErrInvalidConfig
	}
	if err := os.Mkdir(root, 0o700); err == nil {
		return syncDirectory(filepath.Dir(filepath.Clean(root)))
	} else if !errors.Is(err, os.ErrExist) {
		return err
	}
	info, err := os.Lstat(root)
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return base.ErrInvalidConfig
	}
	return nil
}

// Initialize creates or resumes a v2 store while the caller holds the
// directory lock. The marker payload is the exact generation-1 Manifest, so
// recovery has no second copy of identity or hard-limit semantics.
func Initialize(root string, hard storecatalog.HardLimits, hook FaultHook) (storecatalog.Manifest, error) {
	marker, found, err := loadMarker(root)
	if err != nil {
		return storecatalog.Manifest{}, err
	}
	if !found {
		if _, err := storecatalog.Load(root); err == nil {
			return storecatalog.Manifest{}, base.ErrAlreadyExists
		} else if !errors.Is(err, os.ErrNotExist) {
			return storecatalog.Manifest{}, err
		}
		if err := removeMarkerTemp(root); err != nil {
			return storecatalog.Manifest{}, err
		}
		if err := requireFresh(root); err != nil {
			return storecatalog.Manifest{}, err
		}
		marker, err = initialManifest(hard)
		if err != nil {
			return storecatalog.Manifest{}, err
		}
		if err := installMarker(root, marker, hook); err != nil {
			return storecatalog.Manifest{}, err
		}
	} else if marker.HardLimits != hard {
		return storecatalog.Manifest{}, base.ErrConfigMismatch
	}
	return resume(root, marker, hook)
}

// RequireReady prevents Open from treating a published Manifest as usable
// while initialization still has a durable completion marker.
func RequireReady(root string) error {
	if _, found, err := loadMarker(root); err != nil {
		return err
	} else if found {
		return base.ErrRecoveryRequired
	}
	return nil
}

// RecoveryArtifacts reports whether initialization state exists without
// decoding, deleting, or otherwise recovering it.
func RecoveryArtifacts(root string) (bool, error) {
	if root == "" {
		return false, base.ErrInvalidConfig
	}
	for _, name := range []string{MarkerName, markerTempName} {
		if _, err := os.Lstat(filepath.Join(root, name)); err == nil {
			return true, nil
		} else if !errors.Is(err, os.ErrNotExist) {
			return false, err
		}
	}
	return false, nil
}

func initialManifest(hard storecatalog.HardLimits) (storecatalog.Manifest, error) {
	if err := ValidateHardLimits(hard); err != nil {
		return storecatalog.Manifest{}, err
	}
	var storeID storecatalog.StoreUUID
	var logID recordlog.LogID
	if err := randomNonZero(storeID[:]); err != nil {
		return storecatalog.Manifest{}, err
	}
	if err := randomNonZero(logID[:]); err != nil {
		return storecatalog.Manifest{}, err
	}
	replayStart, err := recordlog.NewLogPos(1, recordlog.SegmentHeaderSize)
	if err != nil {
		return storecatalog.Manifest{}, err
	}
	manifest := storecatalog.Manifest{
		Generation: 1, StoreUUID: storeID, HardLimits: hard, RecordLogID: logID,
		ActiveDataSegmentID: 1, NextDataSegmentID: 2,
		ActiveMapSegmentID: 1, NextMapSegmentID: 2,
		ReplayStart: replayStart, ReservedIDHigh: 1, ReservedBatchIDHigh: 1,
		IssuedBatchIDHighAtCut: 1,
	}
	if err := storecatalog.Validate(manifest); err != nil {
		return storecatalog.Manifest{}, errors.Join(base.ErrInvalidConfig, err)
	}
	return manifest, nil
}

func resume(root string, want storecatalog.Manifest, hook FaultHook) (storecatalog.Manifest, error) {
	if err := hit(hook, FaultBeforeDataSegment); err != nil {
		return storecatalog.Manifest{}, err
	}
	if err := recordlog.EnsureInitialSegment(root, want.RecordLogID, uint32(want.HardLimits.SegmentSize)); err != nil {
		return storecatalog.Manifest{}, err
	}
	if err := hit(hook, FaultBeforeMapSegment); err != nil {
		return storecatalog.Manifest{}, err
	}
	if err := mapstore.EnsureInitialSegment(root, mapstore.StoreID(want.StoreUUID), uint32(want.HardLimits.SegmentSize)); err != nil {
		return storecatalog.Manifest{}, err
	}
	if err := hit(hook, FaultBeforeManifest); err != nil {
		return storecatalog.Manifest{}, err
	}
	got, err := storecatalog.Load(root)
	if errors.Is(err, os.ErrNotExist) {
		if err := storecatalog.Install(root, want, nil); err != nil {
			return storecatalog.Manifest{}, err
		}
		got, err = storecatalog.Load(root)
	}
	if err != nil {
		return storecatalog.Manifest{}, err
	}
	wantBytes, encodeErr := storecatalog.Encode(want)
	gotBytes, gotEncodeErr := storecatalog.Encode(got)
	if encodeErr != nil || gotEncodeErr != nil || !bytes.Equal(gotBytes, wantBytes) {
		return storecatalog.Manifest{}, fmt.Errorf("initial manifest differs from marker: %w", errors.Join(base.ErrCorrupt, encodeErr, gotEncodeErr))
	}
	if err := hit(hook, FaultBeforeMarkerRemove); err != nil {
		return storecatalog.Manifest{}, err
	}
	if err := os.Remove(filepath.Join(root, MarkerName)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return storecatalog.Manifest{}, err
	}
	if err := removeMarkerTemp(root); err != nil {
		return storecatalog.Manifest{}, err
	}
	if err := hit(hook, FaultBeforeFinalDirSync); err != nil {
		return storecatalog.Manifest{}, err
	}
	if err := syncDirectory(root); err != nil {
		return storecatalog.Manifest{}, err
	}
	return got, nil
}

func installMarker(root string, manifest storecatalog.Manifest, hook FaultHook) error {
	encoded, err := storecatalog.Encode(manifest)
	if err != nil {
		return err
	}
	temp := filepath.Join(root, markerTempName)
	marker := filepath.Join(root, MarkerName)
	file, err := os.OpenFile(temp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	fail := func(cause error) error { return errors.Join(cause, file.Close()) }
	if err := hit(hook, FaultBeforeMarkerWrite); err != nil {
		return fail(err)
	}
	if err := writeFull(file, encoded); err != nil {
		return fail(err)
	}
	if err := hit(hook, FaultBeforeMarkerSync); err != nil {
		return fail(err)
	}
	if err := file.Sync(); err != nil {
		return fail(err)
	}
	if err := file.Close(); err != nil {
		return err
	}
	if err := hit(hook, FaultBeforeMarkerRename); err != nil {
		return err
	}
	if err := os.Rename(temp, marker); err != nil {
		return err
	}
	if err := hit(hook, FaultBeforeMarkerDirSync); err != nil {
		return err
	}
	return syncDirectory(root)
}

func loadMarker(root string) (storecatalog.Manifest, bool, error) {
	path := filepath.Join(root, MarkerName)
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return storecatalog.Manifest{}, false, nil
	}
	if err != nil {
		return storecatalog.Manifest{}, false, err
	}
	if !info.Mode().IsRegular() || info.Size() <= 0 || info.Size() > maxMarkerBytes {
		return storecatalog.Manifest{}, false, errors.Join(base.ErrCorrupt, storecatalog.ErrCorrupt)
	}
	encoded, err := os.ReadFile(path)
	if err != nil {
		return storecatalog.Manifest{}, false, err
	}
	manifest, err := storecatalog.Decode(encoded)
	if err != nil || !isInitial(manifest) {
		return storecatalog.Manifest{}, false, errors.Join(base.ErrCorrupt, err)
	}
	return manifest, true, nil
}

func isInitial(manifest storecatalog.Manifest) bool {
	wantReplay, _ := recordlog.NewLogPos(1, recordlog.SegmentHeaderSize)
	return manifest.Generation == 1 && manifest.ActiveDataSegmentID == 1 && manifest.NextDataSegmentID == 2 &&
		manifest.ActiveMapSegmentID == 1 && manifest.NextMapSegmentID == 2 && manifest.MappingRoot == 0 &&
		manifest.CoveredCommitSeq == 0 && manifest.ReplayStart == wantReplay && manifest.ReservedIDHigh == 1 &&
		manifest.ReservedBatchIDHigh == 1 && manifest.IssuedBatchIDHighAtCut == 1 && len(manifest.SealedDataSegments) == 0 &&
		len(manifest.SealedMapSegments) == 0 && len(manifest.OpenBatchIDsAtCut) == 0 && manifest.StatsCoveredCommitSeq == 0 &&
		len(manifest.SegmentStats) == 0
}

func requireFresh(root string) error {
	entries, err := os.ReadDir(root)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if entry.Name() != filelock.FileName {
			return base.ErrAlreadyExists
		}
	}
	return nil
}

func removeMarkerTemp(root string) error {
	if err := os.Remove(filepath.Join(root, markerTempName)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

func randomNonZero(dst []byte) error {
	for {
		if _, err := io.ReadFull(rand.Reader, dst); err != nil {
			return err
		}
		zero := true
		for _, value := range dst {
			zero = zero && value == 0
		}
		if !zero {
			return nil
		}
	}
}

func writeFull(file *os.File, value []byte) error {
	for len(value) != 0 {
		written, err := file.Write(value)
		value = value[written:]
		if err != nil {
			return err
		}
		if written == 0 {
			return io.ErrShortWrite
		}
	}
	return nil
}

func syncDirectory(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	return errors.Join(dir.Sync(), dir.Close())
}

func hit(hook FaultHook, point FaultPoint) error {
	if hook == nil {
		return nil
	}
	return hook(point)
}

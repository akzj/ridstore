package initialize

import (
	"bytes"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"time"

	"github.com/akzj/ridstore/internal/base"
	"github.com/akzj/ridstore/internal/failpoint"
	storeformat "github.com/akzj/ridstore/internal/format"
	"github.com/akzj/ridstore/internal/manifest"
)

const (
	MarkerFileName          = "INITIALIZING"
	RestoringMarkerFileName = "RESTORING"
	markerTempFileName      = ".INITIALIZING.tmp"
)

var storeDirectories = []string{"manifests", "data", "mapping", "journal", "trash", "tmp"}

const (
	PointMarkerWritten       failpoint.Point = "initialize.marker-written"
	PointMarkerFileSynced    failpoint.Point = "initialize.marker-file-synced"
	PointMarkerRenamed       failpoint.Point = "initialize.marker-renamed"
	PointMarkerDirSynced     failpoint.Point = "initialize.marker-dir-synced"
	PointDirectoriesCreated  failpoint.Point = "initialize.directories-created"
	PointDirectoriesSynced   failpoint.Point = "initialize.directories-synced"
	PointDataHeaderWritten   failpoint.Point = "initialize.data-header-written"
	PointDataHeaderSynced    failpoint.Point = "initialize.data-header-synced"
	PointDataDirectorySynced failpoint.Point = "initialize.data-directory-synced"
	PointMapHeaderWritten    failpoint.Point = "initialize.map-header-written"
	PointMapHeaderSynced     failpoint.Point = "initialize.map-header-synced"
	PointMapDirectorySynced  failpoint.Point = "initialize.map-directory-synced"
	PointMarkerRemoved       failpoint.Point = "initialize.marker-removed"
	PointFinalDirSynced      failpoint.Point = "initialize.final-dir-synced"

	PointBeforeMarkerTempRemove     failpoint.Point = "initialize.before-marker-temp-remove"
	PointBeforeMarkerWrite          failpoint.Point = "initialize.before-marker-write"
	PointBeforeMarkerFileSync       failpoint.Point = "initialize.before-marker-file-sync"
	PointBeforeMarkerRename         failpoint.Point = "initialize.before-marker-rename"
	PointBeforeMarkerDirSync        failpoint.Point = "initialize.before-marker-dir-sync"
	PointBeforeMarkerRecoveryRemove failpoint.Point = "initialize.before-marker-recovery-remove"
	PointBeforeMarkerRecoverySync   failpoint.Point = "initialize.before-marker-recovery-dir-sync"
	PointBeforeDirectoryCreate      failpoint.Point = "initialize.before-directory-create"
	PointBeforeDirectoriesSync      failpoint.Point = "initialize.before-directories-sync"
	PointBeforeInitialSegmentRemove failpoint.Point = "initialize.before-initial-segment-remove"
	PointBeforeDataHeaderWrite      failpoint.Point = "initialize.before-data-header-write"
	PointBeforeDataHeaderFileSync   failpoint.Point = "initialize.before-data-header-file-sync"
	PointBeforeDataDirectorySync    failpoint.Point = "initialize.before-data-directory-sync"
	PointBeforeMapHeaderWrite       failpoint.Point = "initialize.before-map-header-write"
	PointBeforeMapHeaderFileSync    failpoint.Point = "initialize.before-map-header-file-sync"
	PointBeforeMapDirectorySync     failpoint.Point = "initialize.before-map-directory-sync"
	PointBeforeFinalMarkerRemove    failpoint.Point = "initialize.before-final-marker-remove"
	PointBeforeFinalTempRemove      failpoint.Point = "initialize.before-final-temp-remove"
	PointBeforeFinalDirSync         failpoint.Point = "initialize.before-final-dir-sync"
)

type Options struct {
	Hook failpoint.Hook
}

func Create(dir string, hard storeformat.HardLimits) (storeformat.Manifest, error) {
	return CreateWithOptions(dir, hard, Options{})
}

func CreateWithOptions(dir string, hard storeformat.HardLimits, opts Options) (storeformat.Manifest, error) {
	if err := rejectRestoring(dir); err != nil {
		return storeformat.Manifest{}, err
	}
	marker, found, err := loadRecoverableMarker(dir, opts.Hook)
	if err != nil {
		return storeformat.Manifest{}, err
	}
	if !found {
		if _, err := manifest.LoadCurrent(dir); err == nil {
			return storeformat.Manifest{}, base.ErrAlreadyExists
		} else if !errors.Is(err, os.ErrNotExist) {
			return storeformat.Manifest{}, err
		}
		if err := requireFreshDirectory(dir); err != nil {
			return storeformat.Manifest{}, err
		}
		marker = storeformat.InitializingMarker{HardLimits: hard, Phase: storeformat.InitializingPrepared}
		for marker.StoreUUID == (base.StoreUUID{}) {
			if _, err := io.ReadFull(rand.Reader, marker.StoreUUID[:]); err != nil {
				return storeformat.Manifest{}, err
			}
		}
		if err := installMarker(dir, marker, opts.Hook); err != nil {
			return storeformat.Manifest{}, err
		}
	} else if marker.HardLimits != hard {
		return storeformat.Manifest{}, base.ErrConfigMismatch
	}
	return resume(dir, marker, opts)
}

func Open(dir string) (storeformat.Manifest, error) {
	return OpenWithOptions(dir, Options{})
}

func OpenWithOptions(dir string, opts Options) (storeformat.Manifest, error) {
	if err := rejectRestoring(dir); err != nil {
		return storeformat.Manifest{}, err
	}
	marker, found, err := loadRecoverableMarker(dir, opts.Hook)
	if err != nil {
		return storeformat.Manifest{}, err
	}
	if found {
		return resume(dir, marker, opts)
	}
	m, err := manifest.LoadCurrent(dir)
	if errors.Is(err, os.ErrNotExist) {
		return storeformat.Manifest{}, base.ErrNotInitialized
	}
	if err == nil {
		if err = manifest.CleanupInterruptedInstall(dir); err == nil {
			// A previous initialization may have removed INITIALIZING but
			// failed to make that removal durable. Re-sync the root before a
			// marker-free Open is allowed to succeed.
			if err = failpoint.Hit(opts.Hook, PointBeforeFinalDirSync); err == nil {
				err = syncDirectory(dir)
			}
		}
	}
	return m, err
}

func rejectRestoring(dir string) error {
	if _, err := os.Lstat(filepath.Join(dir, RestoringMarkerFileName)); err == nil {
		return base.ErrRecoveryRequired
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

func resume(dir string, marker storeformat.InitializingMarker, opts Options) (storeformat.Manifest, error) {
	if err := ensureDirectories(dir, marker.Phase >= storeformat.InitializingDirectoriesDurable, opts.Hook); err != nil {
		return storeformat.Manifest{}, err
	}
	if marker.Phase < storeformat.InitializingDirectoriesDurable {
		marker.Phase = storeformat.InitializingDirectoriesDurable
		if err := installMarker(dir, marker, opts.Hook); err != nil {
			return storeformat.Manifest{}, err
		}
	}

	createdUnixNano := uint64(time.Now().UnixNano())
	dataHeader := storeformat.SegmentHeader{Kind: storeformat.SegmentKindData, StoreUUID: marker.StoreUUID, FileID: 1, CreatedUnixNano: createdUnixNano, FirstSeq: 1}
	if err := ensureInitialSegment(dir, "data", "DATA-00000001.active", dataHeader, marker.Phase >= storeformat.InitializingDataHeaderDurable, opts.Hook, PointDataHeaderWritten, PointDataHeaderSynced, PointDataDirectorySynced); err != nil {
		return storeformat.Manifest{}, err
	}
	if marker.Phase < storeformat.InitializingDataHeaderDurable {
		marker.Phase = storeformat.InitializingDataHeaderDurable
		if err := installMarker(dir, marker, opts.Hook); err != nil {
			return storeformat.Manifest{}, err
		}
	}

	mapHeader := storeformat.SegmentHeader{Kind: storeformat.SegmentKindMapping, StoreUUID: marker.StoreUUID, FileID: 1, CreatedUnixNano: createdUnixNano, FirstSeq: 1}
	if err := ensureInitialSegment(dir, "mapping", "MAP-00000001.active", mapHeader, marker.Phase >= storeformat.InitializingMapHeaderDurable, opts.Hook, PointMapHeaderWritten, PointMapHeaderSynced, PointMapDirectorySynced); err != nil {
		return storeformat.Manifest{}, err
	}
	if marker.Phase < storeformat.InitializingMapHeaderDurable {
		marker.Phase = storeformat.InitializingMapHeaderDurable
		if err := installMarker(dir, marker, opts.Hook); err != nil {
			return storeformat.Manifest{}, err
		}
	}

	want, err := initialManifest(marker)
	if err != nil {
		return storeformat.Manifest{}, err
	}
	if err := (manifest.Installer{Dir: dir, FailpointHook: opts.Hook}).Install(want); err != nil {
		return storeformat.Manifest{}, err
	}
	if marker.Phase < storeformat.InitializingManifestInstalled {
		marker.Phase = storeformat.InitializingManifestInstalled
		if err := installMarker(dir, marker, opts.Hook); err != nil {
			return storeformat.Manifest{}, err
		}
	}
	got, err := manifest.LoadCurrent(dir)
	if err != nil {
		return storeformat.Manifest{}, err
	}
	if !reflect.DeepEqual(got, want) {
		return storeformat.Manifest{}, fmt.Errorf("initial manifest does not match INITIALIZING marker: %w", base.ErrCorrupt)
	}
	if err := failpoint.Hit(opts.Hook, PointBeforeFinalMarkerRemove); err != nil {
		return storeformat.Manifest{}, err
	}
	if err := os.Remove(filepath.Join(dir, MarkerFileName)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return storeformat.Manifest{}, err
	}
	if err := failpoint.Hit(opts.Hook, PointMarkerRemoved); err != nil {
		return storeformat.Manifest{}, err
	}
	if err := failpoint.Hit(opts.Hook, PointBeforeFinalTempRemove); err != nil {
		return storeformat.Manifest{}, err
	}
	if err := os.Remove(filepath.Join(dir, markerTempFileName)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return storeformat.Manifest{}, err
	}
	if err := failpoint.Hit(opts.Hook, PointBeforeFinalDirSync); err != nil {
		return storeformat.Manifest{}, err
	}
	if err := syncDirectory(dir); err != nil {
		return storeformat.Manifest{}, err
	}
	if err := failpoint.Hit(opts.Hook, PointFinalDirSynced); err != nil {
		return storeformat.Manifest{}, err
	}
	return got, nil
}

func initialManifest(marker storeformat.InitializingMarker) (storeformat.Manifest, error) {
	replay, err := base.NewLogPos(1, storeformat.SegmentHeaderSize)
	if err != nil {
		return storeformat.Manifest{}, err
	}
	return storeformat.Manifest{
		Generation: 1, StoreUUID: marker.StoreUUID, HardLimits: marker.HardLimits,
		NextDataSegmentID: 2, NextMapSegmentID: 2,
		ActiveDataSegmentID: 1, ActiveMapSegmentID: 1,
		ReplayStart: replay, ReservedIDHighExclusive: 1,
		ReservedBatchIDHighExclusive: 1, IssuedBatchIDHighExclusiveAtCut: 1,
		NextFrameSeq: 1, NextCommitSeq: 1,
	}, nil
}

func ensureDirectories(root string, mustExist bool, hook failpoint.Hook) error {
	for _, name := range storeDirectories {
		path := filepath.Join(root, name)
		info, err := os.Lstat(path)
		if errors.Is(err, os.ErrNotExist) && !mustExist {
			if err := failpoint.Hit(hook, PointBeforeDirectoryCreate); err != nil {
				return err
			}
			if err := os.Mkdir(path, 0o700); err != nil && !errors.Is(err, os.ErrExist) {
				return err
			}
			info, err = os.Lstat(path)
		}
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return fmt.Errorf("durable initialization directory missing: %s: %w", name, base.ErrCorrupt)
			}
			return err
		}
		if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("initialization path is not a directory: %s: %w", name, base.ErrCorrupt)
		}
	}
	if err := failpoint.Hit(hook, PointDirectoriesCreated); err != nil {
		return err
	}
	if err := failpoint.Hit(hook, PointBeforeDirectoriesSync); err != nil {
		return err
	}
	if err := syncDirectory(root); err != nil {
		return err
	}
	return failpoint.Hit(hook, PointDirectoriesSynced)
}

func ensureInitialSegment(root, directory, name string, want storeformat.SegmentHeader, mustBeDurable bool, hook failpoint.Hook, written, synced, directorySynced failpoint.Point) error {
	path := filepath.Join(root, directory, name)
	data, err := readRegularFile(path, storeformat.SegmentHeaderSize)
	if err == nil {
		got, decodeErr := storeformat.DecodeSegmentHeader(data)
		if decodeErr == nil && got.Kind == want.Kind && got.StoreUUID == want.StoreUUID && got.FileID == want.FileID && got.FirstSeq == want.FirstSeq {
			if !mustBeDurable {
				if err := failpoint.Hit(hook, beforeSyncPoint(synced)); err != nil {
					return err
				}
				if err := syncRegularFile(path); err != nil {
					return err
				}
				if err := failpoint.Hit(hook, synced); err != nil {
					return err
				}
			}
			if err := failpoint.Hit(hook, beforeDirectorySyncPoint(directorySynced)); err != nil {
				return err
			}
			if err := syncDirectory(filepath.Join(root, directory)); err != nil {
				return err
			}
			return failpoint.Hit(hook, directorySynced)
		}
		if mustBeDurable {
			if decodeErr != nil {
				return decodeErr
			}
			return fmt.Errorf("durable initial segment identity mismatch: %w", base.ErrCorrupt)
		}
		if err := failpoint.Hit(hook, PointBeforeInitialSegmentRemove); err != nil {
			return err
		}
		if err := os.Remove(path); err != nil {
			return err
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		if mustBeDurable {
			return err
		}
		if hookErr := failpoint.Hit(hook, PointBeforeInitialSegmentRemove); hookErr != nil {
			return errors.Join(err, hookErr)
		}
		if removeErr := os.Remove(path); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
			return errors.Join(err, removeErr)
		}
	} else if mustBeDurable {
		return fmt.Errorf("durable initial segment missing: %s: %w", name, base.ErrCorrupt)
	}
	header, err := storeformat.EncodeSegmentHeader(want)
	if err != nil {
		return err
	}
	if err := writeExclusiveSynced(path, header[:], hook, written, synced); err != nil {
		return err
	}
	if err := failpoint.Hit(hook, beforeDirectorySyncPoint(directorySynced)); err != nil {
		return err
	}
	if err := syncDirectory(filepath.Join(root, directory)); err != nil {
		return err
	}
	return failpoint.Hit(hook, directorySynced)
}

func loadRecoverableMarker(dir string, hook failpoint.Hook) (storeformat.InitializingMarker, bool, error) {
	path := filepath.Join(dir, MarkerFileName)
	data, err := readRegularFile(path, storeformat.MaxJournalPayloadSize+storeformat.ContainerHeaderSize)
	if err == nil {
		marker, decodeErr := storeformat.DecodeInitializingMarker(data)
		if decodeErr != nil {
			return storeformat.InitializingMarker{}, false, decodeErr
		}
		// A previous marker rename may have returned a directory-sync error.
		// Re-sync both the file and root before trusting the phase on retry.
		if err := failpoint.Hit(hook, PointBeforeMarkerFileSync); err != nil {
			return storeformat.InitializingMarker{}, false, err
		}
		if err := syncRegularFile(path); err != nil {
			return storeformat.InitializingMarker{}, false, err
		}
		if err := failpoint.Hit(hook, PointBeforeMarkerDirSync); err != nil {
			return storeformat.InitializingMarker{}, false, err
		}
		if err := syncDirectory(dir); err != nil {
			return storeformat.InitializingMarker{}, false, err
		}
		return marker, true, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return storeformat.InitializingMarker{}, false, err
	}
	tempPath := filepath.Join(dir, markerTempFileName)
	data, err = readRegularFile(tempPath, storeformat.MaxJournalPayloadSize+storeformat.ContainerHeaderSize)
	if errors.Is(err, os.ErrNotExist) {
		return storeformat.InitializingMarker{}, false, nil
	}
	if err != nil {
		return storeformat.InitializingMarker{}, false, err
	}
	marker, err := storeformat.DecodeInitializingMarker(data)
	if err != nil {
		if hookErr := failpoint.Hit(hook, PointBeforeMarkerRecoveryRemove); hookErr != nil {
			return storeformat.InitializingMarker{}, false, errors.Join(err, hookErr)
		}
		if removeErr := os.Remove(tempPath); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
			return storeformat.InitializingMarker{}, false, errors.Join(err, removeErr)
		}
		if syncErr := failpoint.Hit(hook, PointBeforeMarkerRecoverySync); syncErr != nil {
			return storeformat.InitializingMarker{}, false, errors.Join(err, syncErr)
		}
		if syncErr := syncDirectory(dir); syncErr != nil {
			return storeformat.InitializingMarker{}, false, errors.Join(err, syncErr)
		}
		return storeformat.InitializingMarker{}, false, nil
	}
	if err := failpoint.Hit(hook, PointBeforeMarkerFileSync); err != nil {
		return storeformat.InitializingMarker{}, false, err
	}
	if err := syncRegularFile(tempPath); err != nil {
		return storeformat.InitializingMarker{}, false, err
	}
	if err := failpoint.Hit(hook, PointBeforeMarkerRename); err != nil {
		return storeformat.InitializingMarker{}, false, err
	}
	if err := os.Rename(tempPath, path); err != nil {
		return storeformat.InitializingMarker{}, false, err
	}
	if err := failpoint.Hit(hook, PointMarkerRenamed); err != nil {
		return storeformat.InitializingMarker{}, false, err
	}
	if err := failpoint.Hit(hook, PointBeforeMarkerDirSync); err != nil {
		return storeformat.InitializingMarker{}, false, err
	}
	if err := syncDirectory(dir); err != nil {
		return storeformat.InitializingMarker{}, false, err
	}
	if err := failpoint.Hit(hook, PointMarkerDirSynced); err != nil {
		return storeformat.InitializingMarker{}, false, err
	}
	return marker, true, nil
}

func installMarker(dir string, marker storeformat.InitializingMarker, hook failpoint.Hook) error {
	data, err := storeformat.EncodeInitializingMarker(marker)
	if err != nil {
		return err
	}
	tempPath := filepath.Join(dir, markerTempFileName)
	if err := failpoint.Hit(hook, PointBeforeMarkerTempRemove); err != nil {
		return err
	}
	if err := os.Remove(tempPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := writeExclusiveSynced(tempPath, data, hook, PointMarkerWritten, PointMarkerFileSynced); err != nil {
		return err
	}
	if err := failpoint.Hit(hook, PointBeforeMarkerRename); err != nil {
		return err
	}
	if err := os.Rename(tempPath, filepath.Join(dir, MarkerFileName)); err != nil {
		return err
	}
	if err := failpoint.Hit(hook, PointMarkerRenamed); err != nil {
		return err
	}
	if err := failpoint.Hit(hook, PointBeforeMarkerDirSync); err != nil {
		return err
	}
	if err := syncDirectory(dir); err != nil {
		return err
	}
	return failpoint.Hit(hook, PointMarkerDirSynced)
}

func requireFreshDirectory(dir string) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if entry.Name() != "LOCK" {
			return base.ErrAlreadyExists
		}
	}
	return nil
}

func writeExclusiveSynced(path string, data []byte, hook failpoint.Hook, written, synced failpoint.Point) (retErr error) {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := file.Close(); retErr == nil && closeErr != nil {
			retErr = closeErr
		}
	}()
	if err := hitOptional(hook, beforeWritePoint(written)); err != nil {
		return err
	}
	if _, err := io.Copy(file, bytes.NewReader(data)); err != nil {
		return err
	}
	if err := failpoint.Hit(hook, written); err != nil {
		return err
	}
	if err := hitOptional(hook, beforeSyncPoint(synced)); err != nil {
		return err
	}
	if err := file.Sync(); err != nil {
		return err
	}
	return failpoint.Hit(hook, synced)
}

func hitOptional(hook failpoint.Hook, point failpoint.Point) error {
	if point == "" {
		return nil
	}
	return failpoint.Hit(hook, point)
}

func beforeWritePoint(after failpoint.Point) failpoint.Point {
	switch after {
	case PointMarkerWritten:
		return PointBeforeMarkerWrite
	case PointDataHeaderWritten:
		return PointBeforeDataHeaderWrite
	case PointMapHeaderWritten:
		return PointBeforeMapHeaderWrite
	default:
		return ""
	}
}

func beforeSyncPoint(after failpoint.Point) failpoint.Point {
	switch after {
	case PointMarkerFileSynced:
		return PointBeforeMarkerFileSync
	case PointDataHeaderSynced:
		return PointBeforeDataHeaderFileSync
	case PointMapHeaderSynced:
		return PointBeforeMapHeaderFileSync
	default:
		return ""
	}
}

func beforeDirectorySyncPoint(after failpoint.Point) failpoint.Point {
	switch after {
	case PointDataDirectorySynced:
		return PointBeforeDataDirectorySync
	case PointMapDirectorySynced:
		return PointBeforeMapDirectorySync
	default:
		return ""
	}
}

func readRegularFile(path string, maxSize int64) ([]byte, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Size() < 0 || info.Size() > maxSize {
		return nil, fmt.Errorf("invalid initialization file: %s: %w", path, base.ErrCorrupt)
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	opened, err := file.Stat()
	if err != nil || !os.SameFile(info, opened) || opened.Size() > maxSize {
		return nil, fmt.Errorf("initialization file changed while opening: %s: %w", path, base.ErrCorrupt)
	}
	data, err := io.ReadAll(io.LimitReader(file, maxSize+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > maxSize {
		return nil, fmt.Errorf("initialization file exceeds limit: %w", base.ErrCorrupt)
	}
	return data, nil
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

func syncRegularFile(path string) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return err
	}
	return file.Close()
}

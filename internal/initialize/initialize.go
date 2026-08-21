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
	storeformat "github.com/akzj/ridstore/internal/format"
	"github.com/akzj/ridstore/internal/manifest"
)

const (
	MarkerFileName     = "INITIALIZING"
	markerTempFileName = ".INITIALIZING.tmp"
)

var storeDirectories = []string{"manifests", "data", "mapping", "journal", "trash", "tmp"}

func Create(dir string, hard storeformat.HardLimits) (storeformat.Manifest, error) {
	marker, found, err := loadRecoverableMarker(dir)
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
		if err := installMarker(dir, marker); err != nil {
			return storeformat.Manifest{}, err
		}
	} else if marker.HardLimits != hard {
		return storeformat.Manifest{}, base.ErrConfigMismatch
	}
	return resume(dir, marker)
}

func Open(dir string) (storeformat.Manifest, error) {
	marker, found, err := loadRecoverableMarker(dir)
	if err != nil {
		return storeformat.Manifest{}, err
	}
	if found {
		return resume(dir, marker)
	}
	m, err := manifest.LoadCurrent(dir)
	if errors.Is(err, os.ErrNotExist) {
		return storeformat.Manifest{}, base.ErrNotInitialized
	}
	return m, err
}

func resume(dir string, marker storeformat.InitializingMarker) (storeformat.Manifest, error) {
	if err := ensureDirectories(dir, marker.Phase >= storeformat.InitializingDirectoriesDurable); err != nil {
		return storeformat.Manifest{}, err
	}
	if marker.Phase < storeformat.InitializingDirectoriesDurable {
		marker.Phase = storeformat.InitializingDirectoriesDurable
		if err := installMarker(dir, marker); err != nil {
			return storeformat.Manifest{}, err
		}
	}

	createdUnixNano := uint64(time.Now().UnixNano())
	dataHeader := storeformat.SegmentHeader{Kind: storeformat.SegmentKindData, StoreUUID: marker.StoreUUID, FileID: 1, CreatedUnixNano: createdUnixNano, FirstSeq: 1}
	if err := ensureInitialSegment(dir, "data", "DATA-00000001.active", dataHeader, marker.Phase >= storeformat.InitializingDataHeaderDurable); err != nil {
		return storeformat.Manifest{}, err
	}
	if marker.Phase < storeformat.InitializingDataHeaderDurable {
		marker.Phase = storeformat.InitializingDataHeaderDurable
		if err := installMarker(dir, marker); err != nil {
			return storeformat.Manifest{}, err
		}
	}

	mapHeader := storeformat.SegmentHeader{Kind: storeformat.SegmentKindMapping, StoreUUID: marker.StoreUUID, FileID: 1, CreatedUnixNano: createdUnixNano, FirstSeq: 1}
	if err := ensureInitialSegment(dir, "mapping", "MAP-00000001.active", mapHeader, marker.Phase >= storeformat.InitializingMapHeaderDurable); err != nil {
		return storeformat.Manifest{}, err
	}
	if marker.Phase < storeformat.InitializingMapHeaderDurable {
		marker.Phase = storeformat.InitializingMapHeaderDurable
		if err := installMarker(dir, marker); err != nil {
			return storeformat.Manifest{}, err
		}
	}

	want, err := initialManifest(marker)
	if err != nil {
		return storeformat.Manifest{}, err
	}
	if err := (manifest.Installer{Dir: dir}).Install(want); err != nil {
		return storeformat.Manifest{}, err
	}
	if marker.Phase < storeformat.InitializingManifestInstalled {
		marker.Phase = storeformat.InitializingManifestInstalled
		if err := installMarker(dir, marker); err != nil {
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
	if err := os.Remove(filepath.Join(dir, MarkerFileName)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return storeformat.Manifest{}, err
	}
	if err := os.Remove(filepath.Join(dir, markerTempFileName)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return storeformat.Manifest{}, err
	}
	if err := syncDirectory(dir); err != nil {
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

func ensureDirectories(root string, mustExist bool) error {
	for _, name := range storeDirectories {
		path := filepath.Join(root, name)
		info, err := os.Lstat(path)
		if errors.Is(err, os.ErrNotExist) && !mustExist {
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
	return syncDirectory(root)
}

func ensureInitialSegment(root, directory, name string, want storeformat.SegmentHeader, mustBeDurable bool) error {
	path := filepath.Join(root, directory, name)
	data, err := readRegularFile(path, storeformat.SegmentHeaderSize)
	if err == nil {
		got, decodeErr := storeformat.DecodeSegmentHeader(data)
		if decodeErr == nil && got.Kind == want.Kind && got.StoreUUID == want.StoreUUID && got.FileID == want.FileID && got.FirstSeq == want.FirstSeq {
			return syncDirectory(filepath.Join(root, directory))
		}
		if mustBeDurable {
			if decodeErr != nil {
				return decodeErr
			}
			return fmt.Errorf("durable initial segment identity mismatch: %w", base.ErrCorrupt)
		}
		if err := os.Remove(path); err != nil {
			return err
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		if mustBeDurable {
			return err
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
	if err := writeExclusiveSynced(path, header[:]); err != nil {
		return err
	}
	return syncDirectory(filepath.Join(root, directory))
}

func loadRecoverableMarker(dir string) (storeformat.InitializingMarker, bool, error) {
	path := filepath.Join(dir, MarkerFileName)
	data, err := readRegularFile(path, storeformat.MaxJournalPayloadSize+storeformat.ContainerHeaderSize)
	if err == nil {
		marker, decodeErr := storeformat.DecodeInitializingMarker(data)
		return marker, decodeErr == nil, decodeErr
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
		return storeformat.InitializingMarker{}, false, err
	}
	if err := os.Rename(tempPath, path); err != nil {
		return storeformat.InitializingMarker{}, false, err
	}
	if err := syncDirectory(dir); err != nil {
		return storeformat.InitializingMarker{}, false, err
	}
	return marker, true, nil
}

func installMarker(dir string, marker storeformat.InitializingMarker) error {
	data, err := storeformat.EncodeInitializingMarker(marker)
	if err != nil {
		return err
	}
	tempPath := filepath.Join(dir, markerTempFileName)
	if err := os.Remove(tempPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := writeExclusiveSynced(tempPath, data); err != nil {
		return err
	}
	if err := os.Rename(tempPath, filepath.Join(dir, MarkerFileName)); err != nil {
		return err
	}
	return syncDirectory(dir)
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

func writeExclusiveSynced(path string, data []byte) (retErr error) {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := file.Close(); retErr == nil && closeErr != nil {
			retErr = closeErr
		}
	}()
	if _, err := io.Copy(file, bytes.NewReader(data)); err != nil {
		return err
	}
	return file.Sync()
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

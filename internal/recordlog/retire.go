package recordlog

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

func (l *Log) RetireSegment(ctx context.Context, id SegmentID, expectGeneration uint64) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if l.catalog == nil || l.root == "" || l.files == nil {
		return ErrInvalidConfig
	}
	snapshot := l.catalog.SnapshotRecordLog()
	if snapshot.Generation != expectGeneration || snapshot.ActiveSegmentID == id {
		return ErrInvalidConfig
	}
	summary, ok := snapshot.sealedSummary(id)
	if !ok {
		return ErrSegmentMissing
	}
	if err := l.registry.beginRetire(id); err != nil {
		return err
	}
	cancelRetire := true
	defer func() {
		if cancelRetire {
			_ = l.registry.cancelRetire(id)
		}
	}()
	if err := l.registry.waitNoReaders(ctx, id); err != nil {
		return err
	}
	installed, err := l.catalog.RemoveRecordLogSegment(expectGeneration, summary)
	if err != nil {
		return err
	}
	cancelRetire = false
	if installed.Generation <= snapshot.Generation {
		return l.setTerminal(fmt.Errorf("catalog retirement generation: %w", ErrCorrupt))
	}
	if _, exists := installed.sealedSummary(id); exists {
		return l.setTerminal(fmt.Errorf("catalog retained removed segment: %w", ErrCorrupt))
	}
	segment, err := l.registry.detachRetired(id)
	if err != nil {
		return l.setTerminal(err)
	}
	if err := segment.close(); err != nil {
		return err
	}
	if err := cleanupRetiredSegment(l.root, id, installed.Generation, l.files, l.hook); err != nil {
		return l.setTerminal(err)
	}
	return nil
}

// CleanupRetiredSegment completes the physical half of a retirement whose
// Catalog removal is already durable. The caller must prove that id is absent
// from the authoritative Catalog before calling it.
func CleanupRetiredSegment(root string, id SegmentID, catalogGeneration uint64) error {
	if root == "" || id == 0 || catalogGeneration == 0 {
		return ErrInvalidConfig
	}
	return CleanupRetiredSegmentWithFaultHook(root, id, catalogGeneration, nil)
}

// CleanupRetiredSegmentWithFaultHook exposes durable cleanup boundaries to
// recovery tests without making the filesystem backend configurable.
func CleanupRetiredSegmentWithFaultHook(root string, id SegmentID, catalogGeneration uint64, hook FaultHook) error {
	if root == "" || id == 0 || catalogGeneration == 0 {
		return ErrInvalidConfig
	}
	return cleanupRetiredSegment(root, id, catalogGeneration, osFileBackend{}, hook)
}

func cleanupRetiredSegment(root string, id SegmentID, catalogGeneration uint64, files fileBackend, hook FaultHook) error {
	trash, err := ensureTrashDirectory(root, files, hook)
	if err != nil {
		return err
	}
	source := filepath.Join(recordsPath(root), sealedSegmentName(id))
	destination := filepath.Join(trash, fmt.Sprintf("%s.g%d.trash", sealedSegmentName(id), catalogGeneration))
	_, sourceErr := files.stat(source)
	_, destinationErr := files.stat(destination)
	sourceExists, destinationExists := sourceErr == nil, destinationErr == nil
	if sourceErr != nil && !errors.Is(sourceErr, os.ErrNotExist) {
		return sourceErr
	}
	if destinationErr != nil && !errors.Is(destinationErr, os.ErrNotExist) {
		return destinationErr
	}
	if sourceExists && destinationExists {
		return fmt.Errorf("retired source and trash both exist: %w", ErrCorrupt)
	}
	if sourceExists {
		if err := hitSegmentFault(hook, faultBeforeRetireRename); err != nil {
			return err
		}
		if err := files.rename(source, destination); err != nil {
			return err
		}
		destinationExists = true
	}
	if destinationExists {
		// A previous call may have observed an error after cross-directory
		// rename. Stabilize both directory entries before unlinking the only
		// remaining copy.
		if err := hitSegmentFault(hook, faultBeforeRecordsDirSync); err != nil {
			return err
		}
		if err := files.syncDir(recordsPath(root)); err != nil {
			return err
		}
		if err := hitSegmentFault(hook, faultBeforeTrashDirSync); err != nil {
			return err
		}
		if err := files.syncDir(trash); err != nil {
			return err
		}
		if err := hitSegmentFault(hook, faultBeforeTrashRemove); err != nil {
			return err
		}
		if err := files.remove(destination); err != nil {
			return err
		}
		if err := hitSegmentFault(hook, faultBeforeTrashFinalSync); err != nil {
			return err
		}
		return files.syncDir(trash)
	}
	return nil
}

func ensureTrashDirectory(root string, files fileBackend, hook FaultHook) (string, error) {
	dir := filepath.Join(root, "trash")
	err := files.mkdir(dir, 0o700)
	if err == nil {
		if err := hitSegmentFault(hook, faultBeforeTrashRootSync); err != nil {
			return "", err
		}
		if err := files.syncDir(root); err != nil {
			return "", err
		}
		return dir, nil
	}
	if !errors.Is(err, os.ErrExist) {
		return "", err
	}
	info, statErr := files.lstat(dir)
	if statErr != nil {
		return "", statErr
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return "", fmt.Errorf("trash path is not a directory: %w", ErrCorrupt)
	}
	return dir, nil
}

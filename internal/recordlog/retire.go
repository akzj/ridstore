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
	trash, err := ensureTrashDirectory(l.root, l.files)
	if err != nil {
		return err
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
	source := filepath.Join(recordsPath(l.root), sealedSegmentName(id))
	destination := filepath.Join(trash, fmt.Sprintf("%s.g%d.trash", sealedSegmentName(id), installed.Generation))
	if err := l.files.rename(source, destination); err != nil {
		return err
	}
	if err := l.files.syncDir(recordsPath(l.root)); err != nil {
		return err
	}
	if err := l.files.syncDir(trash); err != nil {
		return err
	}
	if err := l.files.remove(destination); err != nil {
		return err
	}
	return l.files.syncDir(trash)
}

func ensureTrashDirectory(root string, files fileBackend) (string, error) {
	dir := filepath.Join(root, "trash")
	err := files.mkdir(dir, 0o700)
	if err == nil {
		if err := files.syncDir(root); err != nil {
			return "", err
		}
		return dir, nil
	}
	if !errors.Is(err, os.ErrExist) {
		return "", err
	}
	info, statErr := files.stat(dir)
	if statErr != nil {
		return "", statErr
	}
	if !info.IsDir() {
		return "", fmt.Errorf("trash path is not a directory: %w", ErrCorrupt)
	}
	return dir, nil
}

package mapstore

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/akzj/ridstore/internal/model"
)

type PhysicalReport struct {
	Segments       uint64
	SealedSegments uint64
	Nodes          uint64
	PhysicalBytes  uint64
	ActiveEnd      uint32
}

// VerifyFiles validates the authoritative Mapping file set without opening a
// writer or repairing the active tail.
func VerifyFiles(ctx context.Context, root string, snapshot CatalogSnapshot) (PhysicalReport, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return PhysicalReport{}, err
	}
	if root == "" || snapshot.validate() != nil {
		return PhysicalReport{}, ErrInvalid
	}
	if found, err := mappingRecoveryArtifacts(root); err != nil {
		return PhysicalReport{}, err
	} else if found {
		return PhysicalReport{}, ErrRecoveryRequired
	}
	if err := verifyMappingFileSet(root, snapshot); err != nil {
		return PhysicalReport{}, err
	}

	report := PhysicalReport{Segments: uint64(len(snapshot.SealedSegments)) + 1, SealedSegments: uint64(len(snapshot.SealedSegments))}
	var lastSeq uint64
	for _, ref := range snapshot.SealedSegments {
		if err := ctx.Err(); err != nil {
			return report, err
		}
		segment, err := openSealed(root, snapshot.headerFor(ref.SegmentID), ref)
		if err != nil {
			return report, err
		}
		if segment.summary.NodeCount != 0 && lastSeq != 0 && segment.summary.FirstSeq <= lastSeq {
			_ = segment.file.Close()
			return report, ErrCorrupt
		}
		if segment.summary.NodeCount != 0 {
			lastSeq = segment.summary.LastSeq
		}
		closeErr := segment.file.Close()
		if closeErr != nil {
			return report, closeErr
		}
		report.Nodes += segment.summary.NodeCount
		report.PhysicalBytes += uint64(ref.ValidEnd) + uint64(SegmentFooterSize)
	}

	activePath := filepath.Join(root, mappingDirectory, activeName(snapshot.ActiveSegment))
	file, err := openMappingRegularReadOnly(activePath)
	if err != nil {
		return report, err
	}
	if err := verifyHeader(file, snapshot.headerFor(snapshot.ActiveSegment)); err != nil {
		_ = file.Close()
		return report, err
	}
	info, err := file.Stat()
	if err != nil || info.Size() < int64(SegmentHeaderSize) || info.Size() > int64(snapshot.SegmentSize-SegmentFooterSize) {
		_ = file.Close()
		return report, errors.Join(ErrCorrupt, err)
	}
	summary, partial, scanErr := scanNodesWithVisitor(file, snapshot.headerFor(snapshot.ActiveSegment), uint32(info.Size()), snapshot.Root,
		func(model.MapAddr, Node, uint32) error { return ctx.Err() })
	closeErr := file.Close()
	if err := errors.Join(scanErr, closeErr); err != nil {
		return report, err
	}
	if partial {
		return report, ErrRecoveryRequired
	}
	if summary.NodeCount != 0 && lastSeq != 0 && summary.FirstSeq <= lastSeq {
		return report, ErrCorrupt
	}
	report.Nodes += summary.NodeCount
	report.PhysicalBytes += uint64(summary.ValidEnd)
	report.ActiveEnd = summary.ValidEnd
	return report, nil
}

func mappingRecoveryArtifacts(root string) (bool, error) {
	for _, path := range []string{rotationPath(root), rotationTempPath(root)} {
		if _, err := os.Lstat(path); err == nil {
			return true, nil
		} else if !errors.Is(err, os.ErrNotExist) {
			return false, err
		}
	}
	return false, nil
}

func verifyMappingFileSet(root string, snapshot CatalogSnapshot) error {
	dir := filepath.Join(root, mappingDirectory)
	info, err := os.Lstat(dir)
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return errors.Join(ErrCorrupt, errors.New("mapping path is not a directory"))
	}
	expected := make(map[string]struct{}, len(snapshot.SealedSegments)+1)
	expected[activeName(snapshot.ActiveSegment)] = struct{}{}
	for _, summary := range snapshot.SealedSegments {
		expected[sealedName(summary.SegmentID)] = struct{}{}
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		name := entry.Name()
		if _, ok := expected[name]; ok {
			if entry.Type()&os.ModeSymlink != 0 || entry.IsDir() {
				return fmt.Errorf("mapping file %q is not regular: %w", name, ErrCorrupt)
			}
			delete(expected, name)
			continue
		}
		if strings.HasSuffix(name, ".creating") || name == "ROTATION" || name == "ROTATION.tmp" {
			return ErrRecoveryRequired
		}
		return fmt.Errorf("unexpected mapping file %q: %w", name, ErrCorrupt)
	}
	if len(expected) != 0 {
		return errors.Join(ErrCorrupt, errors.New("mapping file set is incomplete"))
	}
	return nil
}

func openMappingRegularReadOnly(path string) (*os.File, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
		return nil, fmt.Errorf("file %q is not regular: %w", path, ErrCorrupt)
	}
	return os.Open(path)
}

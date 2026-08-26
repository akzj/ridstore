package recordlog

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

type PhysicalReport struct {
	Segments       uint64
	SealedSegments uint64
	Records        uint64
	PhysicalBytes  uint64
	ActiveEnd      LogPos
}

// VerifyFiles validates the authoritative RecordLog file set without starting
// a writer or modifying repairable state. A partial active tail is reported as
// ErrRecoveryRequired; corruption in a complete record is ErrCorrupt.
func VerifyFiles(ctx context.Context, root string, snapshot CatalogSnapshot) (PhysicalReport, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return PhysicalReport{}, err
	}
	if root == "" || snapshot.validate() != nil {
		return PhysicalReport{}, ErrInvalidConfig
	}
	if found, err := recordLogRecoveryArtifacts(root); err != nil {
		return PhysicalReport{}, err
	} else if found {
		return PhysicalReport{}, ErrRecoveryRequired
	}
	if err := verifyRecordFileSet(root, snapshot); err != nil {
		return PhysicalReport{}, err
	}

	report := PhysicalReport{Segments: uint64(len(snapshot.SealedSegments)) + 1, SealedSegments: uint64(len(snapshot.SealedSegments))}
	for _, summary := range snapshot.SealedSegments {
		if err := ctx.Err(); err != nil {
			return report, err
		}
		segment, err := openSealedSegment(root, snapshot.headerFor(summary.SegmentID), summary, nil)
		if err != nil {
			return report, err
		}
		scanErr := segment.scan(SegmentHeaderSize, func(AppendResult, []byte) error { return ctx.Err() })
		closeErr := segment.close()
		if err := errors.Join(scanErr, closeErr); err != nil {
			return report, err
		}
		report.Records += summary.RecordCount
		report.PhysicalBytes += uint64(summary.ValidEnd) + uint64(SegmentFooterSize)
	}

	activePath := filepath.Join(recordsPath(root), activeSegmentName(snapshot.ActiveSegmentID))
	file, err := openRegularReadOnly(activePath)
	if err != nil {
		return report, err
	}
	summary, partial, scanErr := scanActiveSegmentWithVisitor(file, snapshot.headerFor(snapshot.ActiveSegmentID), func(AppendResult, []byte) error {
		return ctx.Err()
	})
	closeErr := file.Close()
	if err := errors.Join(scanErr, closeErr); err != nil {
		return report, err
	}
	if partial {
		return report, ErrRecoveryRequired
	}
	report.Records += summary.RecordCount
	report.PhysicalBytes += uint64(summary.ValidEnd)
	report.ActiveEnd = LogPos{SegmentID: snapshot.ActiveSegmentID, Offset: summary.ValidEnd}
	return report, nil
}

func recordLogRecoveryArtifacts(root string) (bool, error) {
	for _, path := range []string{rotationJournalPath(root), rotationJournalTempPath(root)} {
		if _, err := os.Lstat(path); err == nil {
			return true, nil
		} else if !errors.Is(err, os.ErrNotExist) {
			return false, err
		}
	}
	return false, nil
}

func verifyRecordFileSet(root string, snapshot CatalogSnapshot) error {
	dir := recordsPath(root)
	info, err := os.Lstat(dir)
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("records path is not a directory: %w", ErrCorrupt)
	}
	expected := make(map[string]struct{}, len(snapshot.SealedSegments)+1)
	expected[activeSegmentName(snapshot.ActiveSegmentID)] = struct{}{}
	for _, summary := range snapshot.SealedSegments {
		expected[sealedSegmentName(summary.SegmentID)] = struct{}{}
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		name := entry.Name()
		if _, ok := expected[name]; ok {
			if entry.Type()&os.ModeSymlink != 0 || entry.IsDir() {
				return fmt.Errorf("record file %q is not regular: %w", name, ErrCorrupt)
			}
			delete(expected, name)
			continue
		}
		if strings.HasSuffix(name, ".creating") {
			return ErrRecoveryRequired
		}
		return fmt.Errorf("unexpected record file %q: %w", name, ErrCorrupt)
	}
	if len(expected) != 0 {
		return fmt.Errorf("record file set is incomplete: %w", ErrCorrupt)
	}
	return nil
}

func openRegularReadOnly(path string) (*os.File, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
		return nil, fmt.Errorf("file %q is not regular: %w", path, ErrCorrupt)
	}
	return os.Open(path)
}

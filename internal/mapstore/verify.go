package mapstore

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/akzj/ridstore/internal/model"
)

type PhysicalReport struct {
	Segments       uint64
	SealedSegments uint64
	Nodes          uint64
	PhysicalBytes  uint64
	ActiveEnd      uint32
}

// ReadOnly is a verified, immutable view of the Mapping files named by one
// Catalog snapshot. It owns its file descriptors and cannot append or repair.
type ReadOnly struct {
	mu     sync.RWMutex
	files  map[model.MapSegmentID]*segmentFile
	closed bool
}

// VerifyFiles validates the authoritative Mapping file set without opening a
// writer or repairing the active tail.
func VerifyFiles(ctx context.Context, root string, snapshot CatalogSnapshot) (PhysicalReport, error) {
	reader, report, err := OpenVerifiedReader(ctx, root, snapshot)
	if reader == nil {
		return report, err
	}
	return report, errors.Join(err, reader.Close())
}

// OpenVerifiedReader performs the full physical verification and retains a
// read-only handle for reachable-tree validation.
func OpenVerifiedReader(ctx context.Context, root string, snapshot CatalogSnapshot) (*ReadOnly, PhysicalReport, error) {
	return openVerifiedReader(ctx, root, snapshot, true)
}

// OpenVerifiedGeneration verifies and opens exactly the files named by
// snapshot while allowing other Mapping files to coexist. Mapping GC recovery
// uses it after publishing a new generation but before retiring the old one.
// The caller must derive snapshot from a durable marker and Catalog state.
func OpenVerifiedGeneration(ctx context.Context, root string, snapshot CatalogSnapshot) (*ReadOnly, PhysicalReport, error) {
	return openVerifiedReader(ctx, root, snapshot, false)
}

func openVerifiedReader(ctx context.Context, root string, snapshot CatalogSnapshot, requireExactDirectory bool) (*ReadOnly, PhysicalReport, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, PhysicalReport{}, err
	}
	if root == "" || snapshot.validate() != nil {
		return nil, PhysicalReport{}, ErrInvalid
	}
	if found, err := mappingRecoveryArtifacts(root); err != nil {
		return nil, PhysicalReport{}, err
	} else if found {
		return nil, PhysicalReport{}, ErrRecoveryRequired
	}
	if err := verifyExpectedMappingFiles(root, snapshot); err != nil {
		return nil, PhysicalReport{}, err
	}
	if requireExactDirectory {
		if err := verifyMappingFileSet(root, snapshot); err != nil {
			return nil, PhysicalReport{}, err
		}
	}

	report := PhysicalReport{Segments: uint64(len(snapshot.SealedSegments)) + 1, SealedSegments: uint64(len(snapshot.SealedSegments))}
	reader := &ReadOnly{files: make(map[model.MapSegmentID]*segmentFile, len(snapshot.SealedSegments)+1)}
	fail := func(cause error) (*ReadOnly, PhysicalReport, error) {
		return nil, report, errors.Join(cause, reader.Close())
	}
	var lastSeq uint64
	for _, ref := range snapshot.SealedSegments {
		if err := ctx.Err(); err != nil {
			return fail(err)
		}
		segment, err := openSealed(root, snapshot.headerFor(ref.SegmentID), ref)
		if err != nil {
			return fail(err)
		}
		if segment.summary.NodeCount != 0 && lastSeq != 0 && segment.summary.FirstSeq <= lastSeq {
			_ = segment.file.Close()
			return fail(ErrCorrupt)
		}
		if segment.summary.NodeCount != 0 {
			lastSeq = segment.summary.LastSeq
		}
		reader.files[ref.SegmentID] = segment
		report.Nodes += segment.summary.NodeCount
		report.PhysicalBytes += uint64(ref.ValidEnd) + uint64(SegmentFooterSize)
	}

	activePath := filepath.Join(root, mappingDirectory, activeName(snapshot.ActiveSegment))
	file, err := openMappingRegularReadOnly(activePath)
	if err != nil {
		return fail(err)
	}
	if err := verifyHeader(file, snapshot.headerFor(snapshot.ActiveSegment)); err != nil {
		_ = file.Close()
		return fail(err)
	}
	info, err := file.Stat()
	if err != nil || info.Size() < int64(SegmentHeaderSize) || info.Size() > int64(snapshot.SegmentSize-SegmentFooterSize) {
		_ = file.Close()
		return fail(errors.Join(ErrCorrupt, err))
	}
	summary, partial, scanErr := scanNodesWithVisitor(file, snapshot.headerFor(snapshot.ActiveSegment), uint32(info.Size()), snapshot.Root,
		func(model.MapAddr, Node, uint32) error { return ctx.Err() })
	if scanErr != nil {
		_ = file.Close()
		return fail(scanErr)
	}
	if partial {
		_ = file.Close()
		return fail(ErrRecoveryRequired)
	}
	if summary.NodeCount != 0 && lastSeq != 0 && summary.FirstSeq <= lastSeq {
		_ = file.Close()
		return fail(ErrCorrupt)
	}
	reader.files[snapshot.ActiveSegment] = &segmentFile{file: file, header: snapshot.headerFor(snapshot.ActiveSegment), summary: summary}
	report.Nodes += summary.NodeCount
	report.PhysicalBytes += uint64(summary.ValidEnd)
	report.ActiveEnd = summary.ValidEnd
	return reader, report, nil
}

func (r *ReadOnly) Read(addr model.MapAddr) (Node, error) {
	if r == nil || !addr.Valid() {
		return Node{}, ErrInvalid
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.closed {
		return Node{}, ErrClosed
	}
	segment := r.files[addr.SegmentID()]
	if segment == nil || addr.Offset() > segment.summary.ValidEnd || segment.summary.ValidEnd-addr.Offset() < NodeHeaderSize {
		return Node{}, ErrInvalid
	}
	return readNode(segment, addr)
}

func (r *ReadOnly) Close() error {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil
	}
	r.closed = true
	var result error
	for _, segment := range r.files {
		result = errors.Join(result, segment.file.Close())
	}
	return result
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

// RecoveryArtifacts reports incomplete normal Mapping rotation state. Mapping
// GC must never start or recover concurrently with it.
func RecoveryArtifacts(root string) (bool, error) { return mappingRecoveryArtifacts(root) }

func verifyMappingFileSet(root string, snapshot CatalogSnapshot) error {
	dir := filepath.Join(root, mappingDirectory)
	expected := expectedMappingNames(snapshot)
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

func verifyExpectedMappingFiles(root string, snapshot CatalogSnapshot) error {
	dir := filepath.Join(root, mappingDirectory)
	info, err := os.Lstat(dir)
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return errors.Join(ErrCorrupt, errors.New("mapping path is not a directory"))
	}
	for name := range expectedMappingNames(snapshot) {
		info, err := os.Lstat(filepath.Join(dir, name))
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("mapping file %q is not regular: %w", name, ErrCorrupt)
		}
	}
	return nil
}

func expectedMappingNames(snapshot CatalogSnapshot) map[string]struct{} {
	expected := make(map[string]struct{}, len(snapshot.SealedSegments)+1)
	expected[activeName(snapshot.ActiveSegment)] = struct{}{}
	for _, summary := range snapshot.SealedSegments {
		expected[sealedName(summary.SegmentID)] = struct{}{}
	}
	return expected
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

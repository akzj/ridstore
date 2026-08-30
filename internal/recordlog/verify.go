package recordlog

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
)

type PhysicalReport struct {
	Segments       uint64
	SealedSegments uint64
	Records        uint64
	PhysicalBytes  uint64
	ActiveEnd      LogPos
}

// ReadOnly is a verified, immutable view of the RecordLog files named by one
// Catalog snapshot. It owns its file descriptors and cannot append or repair.
type ReadOnly struct {
	mu      sync.Mutex
	changed *sync.Cond
	files   map[SegmentID]*sealedSegment
	refs    uint64
	closed  bool
	active  SegmentID
}

// VerifyFiles validates the authoritative RecordLog file set without starting
// a writer or modifying repairable state. A partial active tail is reported as
// ErrRecoveryRequired; corruption in a complete record is ErrCorrupt.
func VerifyFiles(ctx context.Context, root string, snapshot CatalogSnapshot) (PhysicalReport, error) {
	reader, report, err := OpenVerifiedReader(ctx, root, snapshot)
	if reader == nil {
		return report, err
	}
	return report, errors.Join(err, reader.Close())
}

// OpenVerifiedReader performs the full physical verification and retains
// read-only handles for semantic replay and record identity validation.
func OpenVerifiedReader(ctx context.Context, root string, snapshot CatalogSnapshot) (*ReadOnly, PhysicalReport, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, PhysicalReport{}, err
	}
	if root == "" || snapshot.validate() != nil {
		return nil, PhysicalReport{}, ErrInvalidConfig
	}
	if found, err := recordLogRecoveryArtifacts(root); err != nil {
		return nil, PhysicalReport{}, err
	} else if found {
		return nil, PhysicalReport{}, ErrRecoveryRequired
	}
	if err := verifyRecordFileSet(root, snapshot); err != nil {
		return nil, PhysicalReport{}, err
	}

	report := PhysicalReport{Segments: uint64(len(snapshot.SealedSegments)) + 1, SealedSegments: uint64(len(snapshot.SealedSegments))}
	reader := &ReadOnly{files: make(map[SegmentID]*sealedSegment, len(snapshot.SealedSegments)+1), active: snapshot.ActiveSegmentID}
	reader.changed = sync.NewCond(&reader.mu)
	fail := func(cause error) (*ReadOnly, PhysicalReport, error) {
		return nil, report, errors.Join(cause, reader.Close())
	}
	for _, summary := range snapshot.SealedSegments {
		if err := ctx.Err(); err != nil {
			return fail(err)
		}
		segment, err := openSealedSegment(root, snapshot.headerFor(summary.SegmentID), summary, nil)
		if err != nil {
			return fail(err)
		}
		scanErr := verifySealedRecords(ctx, segment, summary)
		if scanErr != nil {
			return fail(errors.Join(scanErr, segment.close()))
		}
		reader.files[summary.SegmentID] = segment
		report.Records += summary.RecordCount
		report.PhysicalBytes += uint64(summary.ValidEnd) + uint64(SegmentFooterSize)
	}

	activePath := filepath.Join(recordsPath(root), activeSegmentName(snapshot.ActiveSegmentID))
	file, err := openRegularReadOnly(activePath)
	if err != nil {
		return fail(err)
	}
	summary, partial, scanErr := scanActiveSegmentWithVisitor(file, snapshot.headerFor(snapshot.ActiveSegmentID), func(AppendResult, []byte) error {
		return ctx.Err()
	})
	if scanErr != nil {
		return fail(errors.Join(scanErr, file.Close()))
	}
	if partial {
		return fail(errors.Join(ErrRecoveryRequired, file.Close()))
	}
	reader.files[snapshot.ActiveSegmentID] = &sealedSegment{file: file, path: activePath, header: snapshot.headerFor(snapshot.ActiveSegmentID), summary: summary}
	report.Records += summary.RecordCount
	report.PhysicalBytes += uint64(summary.ValidEnd)
	report.ActiveEnd = LogPos{SegmentID: snapshot.ActiveSegmentID, Offset: summary.ValidEnd}
	return reader, report, nil
}

func verifySealedRecords(ctx context.Context, segment *sealedSegment, want SegmentSummary) error {
	got := SegmentSummary{SegmentID: want.SegmentID, ValidEnd: SegmentHeaderSize}
	err := segment.scan(SegmentHeaderSize, func(result AppendResult, _ []byte) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if got.RecordCount == 0 {
			got.FirstAddr = result.Addr
		}
		got.LastAddr = result.Addr
		got.RecordCount++
		got.ValidEnd = result.End.Offset
		return nil
	})
	if err != nil {
		return err
	}
	if got != want {
		return fmt.Errorf("sealed record summary: %w", ErrCorrupt)
	}
	return nil
}

func (r *ReadOnly) Read(ctx context.Context, addr VAddr) ([]byte, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if r == nil || !addr.Valid() {
		return nil, ErrInvalidVAddr
	}
	if !r.acquire() {
		return nil, ErrClosed
	}
	defer r.release()
	segment := r.files[addr.SegmentID()]
	if segment == nil {
		return nil, ErrSegmentMissing
	}
	return segment.read(addr)
}

func (r *ReadOnly) Inspect(ctx context.Context, addr VAddr, prefixBytes uint32) (RecordMetadata, []byte, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return RecordMetadata{}, nil, err
	}
	if r == nil || !addr.Valid() {
		return RecordMetadata{}, nil, ErrInvalidVAddr
	}
	if !r.acquire() {
		return RecordMetadata{}, nil, ErrClosed
	}
	defer r.release()
	segment := r.files[addr.SegmentID()]
	if segment == nil {
		return RecordMetadata{}, nil, ErrSegmentMissing
	}
	header, prefix, err := segment.inspect(addr, prefixBytes)
	return RecordMetadata{PhysicalSize: header.PhysicalSize, PayloadSize: header.PayloadSize, Addr: header.Addr}, prefix, err
}

func (r *ReadOnly) Scan(ctx context.Context, from LogPos, visit func(AppendResult, []byte) error) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if r == nil || !from.Valid() || visit == nil {
		return ErrInvalidConfig
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if !r.acquire() {
		return ErrClosed
	}
	defer r.release()
	ids := make([]SegmentID, 0, len(r.files))
	for id := range r.files {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	if r.files[from.SegmentID] == nil {
		return ErrInvalidLogPos
	}
	for _, id := range ids {
		if id < from.SegmentID || id > r.active {
			continue
		}
		segment := r.files[id]
		start := uint32(SegmentHeaderSize)
		if id == from.SegmentID {
			start = from.Offset
		}
		if start > segment.summary.ValidEnd {
			return ErrInvalidLogPos
		}
		if start == segment.summary.ValidEnd {
			continue
		}
		if err := segment.scan(start, func(result AppendResult, payload []byte) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			return visit(result, payload)
		}); err != nil {
			return err
		}
	}
	return nil
}

func (r *ReadOnly) acquire() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return false
	}
	r.refs++
	return true
}

func (r *ReadOnly) release() {
	r.mu.Lock()
	r.refs--
	if r.refs == 0 {
		r.changed.Broadcast()
	}
	r.mu.Unlock()
}

func (r *ReadOnly) Close() error {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil
	}
	r.closed = true
	for r.refs != 0 {
		r.changed.Wait()
	}
	files := r.files
	r.files = nil
	r.mu.Unlock()
	var result error
	for _, segment := range files {
		result = errors.Join(result, segment.close())
	}
	return result
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

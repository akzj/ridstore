package recordlog

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

const recordsDirectory = "records"

type segmentFaultPoint string

const (
	faultBeforeHeaderWrite   segmentFaultPoint = "recordlog.segment.before-header-write"
	faultBeforeHeaderSync    segmentFaultPoint = "recordlog.segment.before-header-sync"
	faultBeforeCreateRename  segmentFaultPoint = "recordlog.segment.before-create-rename"
	faultBeforeCreateDirSync segmentFaultPoint = "recordlog.segment.before-create-dir-sync"
	faultBeforeAppendWrite   segmentFaultPoint = "recordlog.segment.before-append-write"
	faultBeforeDataSync      segmentFaultPoint = "recordlog.segment.before-data-sync"
	faultBeforeTailTruncate  segmentFaultPoint = "recordlog.segment.before-tail-truncate"
	faultBeforeTailSync      segmentFaultPoint = "recordlog.segment.before-tail-sync"
	faultBeforeFooterWrite   segmentFaultPoint = "recordlog.segment.before-footer-write"
	faultBeforeFooterSync    segmentFaultPoint = "recordlog.segment.before-footer-sync"
	faultBeforeSealRename    segmentFaultPoint = "recordlog.segment.before-seal-rename"
	faultBeforeSealDirSync   segmentFaultPoint = "recordlog.segment.before-seal-dir-sync"
)

type segmentFaultHook func(segmentFaultPoint) error

type SegmentSummary struct {
	SegmentID   SegmentID
	ValidEnd    uint32
	RecordCount uint64
	FirstAddr   VAddr
	LastAddr    VAddr
}

func (s SegmentSummary) validate(segmentSize uint32) error {
	footer := SegmentFooter{SegmentID: s.SegmentID, DataEnd: s.ValidEnd, FirstAddr: s.FirstAddr, LastAddr: s.LastAddr, RecordCount: s.RecordCount}
	if segmentSize <= SegmentFooterSize || validateSegmentFooter(footer) != nil || s.ValidEnd > segmentSize-SegmentFooterSize {
		return ErrInvalidConfig
	}
	return nil
}

type recordExtent struct {
	Result AppendResult
	Size   uint32
}

type activeSegment struct {
	mu          sync.RWMutex
	file        fileHandle
	path        string
	header      SegmentHeader
	end         uint32
	first       VAddr
	last        VAddr
	records     uint64
	sealed      bool
	transferred bool
	files       fileBackend
	hook        segmentFaultHook
}

type sealedSegment struct {
	file    fileHandle
	path    string
	header  SegmentHeader
	summary SegmentSummary
}

func activeSegmentName(id SegmentID) string   { return fmt.Sprintf("record-%010d.active", id) }
func sealedSegmentName(id SegmentID) string   { return fmt.Sprintf("record-%010d.sealed", id) }
func creatingSegmentName(id SegmentID) string { return fmt.Sprintf("record-%010d.creating", id) }

func recordsPath(root string) string { return filepath.Join(root, recordsDirectory) }

func ensureRecordsDirectory(root string, files fileBackend) (string, error) {
	if root == "" {
		return "", ErrInvalidConfig
	}
	dir := recordsPath(root)
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
		return "", fmt.Errorf("records path is not a directory: %w", ErrCorrupt)
	}
	return dir, nil
}

func createActiveSegment(root string, header SegmentHeader, files fileBackend, hook segmentFaultHook) (*activeSegment, error) {
	if files == nil {
		files = osFileBackend{}
	}
	encoded, err := EncodeSegmentHeader(header)
	if err != nil {
		return nil, err
	}
	dir, err := ensureRecordsDirectory(root, files)
	if err != nil {
		return nil, err
	}
	creatingPath := filepath.Join(dir, creatingSegmentName(header.SegmentID))
	activePath := filepath.Join(dir, activeSegmentName(header.SegmentID))
	file, err := files.openFile(creatingPath, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	fail := func(cause error) (*activeSegment, error) {
		return nil, errors.Join(cause, file.Close())
	}
	if err := hitSegmentFault(hook, faultBeforeHeaderWrite); err != nil {
		return fail(err)
	}
	if _, err := writeFullAt(file, encoded[:], 0); err != nil {
		return fail(err)
	}
	if err := hitSegmentFault(hook, faultBeforeHeaderSync); err != nil {
		return fail(err)
	}
	if err := file.Sync(); err != nil {
		return fail(err)
	}
	if err := hitSegmentFault(hook, faultBeforeCreateRename); err != nil {
		return fail(err)
	}
	if err := files.rename(creatingPath, activePath); err != nil {
		return fail(err)
	}
	if err := hitSegmentFault(hook, faultBeforeCreateDirSync); err != nil {
		return fail(err)
	}
	if err := files.syncDir(dir); err != nil {
		return fail(err)
	}
	return &activeSegment{file: file, path: activePath, header: header, end: SegmentHeaderSize, files: files, hook: hook}, nil
}

func openActiveSegment(root string, expected SegmentHeader, files fileBackend, hook segmentFaultHook) (*activeSegment, bool, error) {
	if files == nil {
		files = osFileBackend{}
	}
	dir := recordsPath(root)
	path := filepath.Join(dir, activeSegmentName(expected.SegmentID))
	file, err := files.openFile(path, os.O_RDWR, 0)
	if err != nil {
		return nil, false, err
	}
	fail := func(cause error) (*activeSegment, bool, error) {
		return nil, false, errors.Join(cause, file.Close())
	}
	metadata, repaired, err := scanActiveSegment(file, expected)
	if err != nil {
		return fail(err)
	}
	if repaired {
		if err := hitSegmentFault(hook, faultBeforeTailTruncate); err != nil {
			return fail(err)
		}
		if err := file.Truncate(int64(metadata.ValidEnd)); err != nil {
			return fail(err)
		}
		if err := hitSegmentFault(hook, faultBeforeTailSync); err != nil {
			return fail(err)
		}
		if err := file.Sync(); err != nil {
			return fail(err)
		}
	}
	return &activeSegment{
		file: file, path: path, header: expected, end: metadata.ValidEnd,
		first: metadata.FirstAddr, last: metadata.LastAddr, records: metadata.RecordCount,
		files: files, hook: hook,
	}, repaired, nil
}

func scanActiveSegment(file fileHandle, expected SegmentHeader) (SegmentSummary, bool, error) {
	info, err := file.Stat()
	if err != nil {
		return SegmentSummary{}, false, err
	}
	if !info.Mode().IsRegular() || info.Size() < int64(SegmentHeaderSize) || info.Size() > int64(expected.SegmentSize) {
		return SegmentSummary{}, false, fmt.Errorf("active segment size: %w", ErrCorrupt)
	}
	headerBytes := make([]byte, SegmentHeaderSize)
	if err := readFullAt(file, headerBytes, 0); err != nil {
		return SegmentSummary{}, false, err
	}
	header, err := DecodeSegmentHeader(headerBytes)
	if err != nil {
		return SegmentSummary{}, false, err
	}
	if header != expected {
		return SegmentSummary{}, false, fmt.Errorf("active segment identity: %w", ErrCorrupt)
	}
	result := SegmentSummary{SegmentID: expected.SegmentID, ValidEnd: SegmentHeaderSize}
	fileEnd := uint32(info.Size())
	contentLimit := expected.SegmentSize - SegmentFooterSize
	for result.ValidEnd < fileEnd {
		remaining := fileEnd - result.ValidEnd
		if remaining < RecordHeaderSize {
			return result, true, nil
		}
		headerBytes := make([]byte, RecordHeaderSize)
		if err := readFullAt(file, headerBytes, int64(result.ValidEnd)); err != nil {
			return SegmentSummary{}, false, err
		}
		recordHeader, err := DecodeRecordHeader(headerBytes)
		if err != nil {
			return SegmentSummary{}, false, err
		}
		if result.ValidEnd > contentLimit || recordHeader.Addr.SegmentID() != expected.SegmentID || recordHeader.Addr.Offset() != result.ValidEnd || recordHeader.PhysicalSize > contentLimit-result.ValidEnd {
			return SegmentSummary{}, false, fmt.Errorf("active record boundary: %w", ErrCorrupt)
		}
		if remaining < recordHeader.PhysicalSize {
			return result, true, nil
		}
		record := make([]byte, recordHeader.PhysicalSize)
		if err := readFullAt(file, record, int64(result.ValidEnd)); err != nil {
			return SegmentSummary{}, false, err
		}
		decoded, _, err := DecodeRecord(record)
		if err != nil || decoded != recordHeader {
			if err == nil {
				err = ErrCorrupt
			}
			return SegmentSummary{}, false, err
		}
		if result.RecordCount == 0 {
			result.FirstAddr = recordHeader.Addr
		}
		result.LastAddr = recordHeader.Addr
		result.RecordCount++
		result.ValidEnd += recordHeader.PhysicalSize
	}
	return result, false, nil
}

func (s *activeSegment) remaining() uint32 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.remainingLocked()
}

func (s *activeSegment) remainingLocked() uint32 {
	limit := s.header.SegmentSize - SegmentFooterSize
	if s.end >= limit {
		return 0
	}
	return limit - s.end
}

func (s *activeSegment) appendEncoded(encoded []byte, records []recordExtent) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.file == nil || s.sealed {
		return 0, ErrClosed
	}
	if len(records) == 0 || len(encoded) == 0 {
		if len(records) == 0 && len(encoded) == 0 {
			return 0, nil
		}
		return 0, ErrInvalidConfig
	}
	var total uint32
	for _, record := range records {
		if total > s.remainingLocked() || record.Size == 0 || record.Size > s.remainingLocked()-total {
			return 0, fmt.Errorf("append plan: %w", ErrCorrupt)
		}
		offset := s.end + total
		end := offset + record.Size
		if record.Result.Addr.SegmentID() != s.header.SegmentID || record.Result.Addr.Offset() != offset || record.Result.End != (LogPos{SegmentID: s.header.SegmentID, Offset: end}) {
			return 0, fmt.Errorf("append plan: %w", ErrCorrupt)
		}
		total += record.Size
	}
	if uint32(len(encoded)) != total {
		return 0, fmt.Errorf("encoded append size: %w", ErrCorrupt)
	}
	if err := hitSegmentFault(s.hook, faultBeforeAppendWrite); err != nil {
		return 0, err
	}
	written, err := writeFullAt(s.file, encoded, int64(s.end))
	if err != nil {
		return written, err
	}
	for _, record := range records {
		if s.records == 0 {
			s.first = record.Result.Addr
		}
		s.last = record.Result.Addr
		s.records++
	}
	s.end += total
	return written, nil
}

func (s *activeSegment) sync() error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.file == nil || s.sealed {
		return ErrClosed
	}
	if err := hitSegmentFault(s.hook, faultBeforeDataSync); err != nil {
		return err
	}
	return s.file.Sync()
}

func (s *activeSegment) summary() SegmentSummary {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.summaryLocked()
}

func (s *activeSegment) summaryLocked() SegmentSummary {
	return SegmentSummary{SegmentID: s.header.SegmentID, ValidEnd: s.end, RecordCount: s.records, FirstAddr: s.first, LastAddr: s.last}
}

func (s *activeSegment) seal() (*sealedSegment, SegmentSummary, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.file == nil || s.sealed {
		return nil, SegmentSummary{}, ErrClosed
	}
	summary := s.summaryLocked()
	footer, err := EncodeSegmentFooter(SegmentFooter{SegmentID: summary.SegmentID, DataEnd: summary.ValidEnd, FirstAddr: summary.FirstAddr, LastAddr: summary.LastAddr, RecordCount: summary.RecordCount})
	if err != nil {
		return nil, SegmentSummary{}, err
	}
	if err := hitSegmentFault(s.hook, faultBeforeFooterWrite); err != nil {
		return nil, SegmentSummary{}, err
	}
	if _, err := writeFullAt(s.file, footer[:], int64(s.end)); err != nil {
		return nil, SegmentSummary{}, err
	}
	if err := hitSegmentFault(s.hook, faultBeforeFooterSync); err != nil {
		return nil, SegmentSummary{}, err
	}
	if err := s.file.Sync(); err != nil {
		return nil, SegmentSummary{}, err
	}
	dir := filepath.Dir(s.path)
	destination := filepath.Join(dir, sealedSegmentName(s.header.SegmentID))
	if err := hitSegmentFault(s.hook, faultBeforeSealRename); err != nil {
		return nil, SegmentSummary{}, err
	}
	if err := s.files.rename(s.path, destination); err != nil {
		return nil, SegmentSummary{}, err
	}
	if err := hitSegmentFault(s.hook, faultBeforeSealDirSync); err != nil {
		return nil, SegmentSummary{}, err
	}
	if err := s.files.syncDir(dir); err != nil {
		return nil, SegmentSummary{}, err
	}
	sealed := &sealedSegment{file: s.file, path: destination, header: s.header, summary: summary}
	// Existing reader pins may still hold the active wrapper until Registry
	// publication. Both wrappers therefore share this immutable descriptor;
	// ownership transfers to sealedSegment and activeSegment.close becomes a
	// no-op after this point.
	s.sealed = true
	return sealed, summary, nil
}

func openSealedSegment(root string, expected SegmentHeader, summary SegmentSummary, files fileBackend) (*sealedSegment, error) {
	if files == nil {
		files = osFileBackend{}
	}
	if summary.SegmentID != expected.SegmentID || summary.validate(expected.SegmentSize) != nil {
		return nil, ErrInvalidConfig
	}
	path := filepath.Join(recordsPath(root), sealedSegmentName(expected.SegmentID))
	file, err := files.open(path)
	if err != nil {
		return nil, err
	}
	fail := func(cause error) (*sealedSegment, error) { return nil, errors.Join(cause, file.Close()) }
	info, err := file.Stat()
	if err != nil {
		return fail(err)
	}
	if !info.Mode().IsRegular() || info.Size() != int64(summary.ValidEnd+SegmentFooterSize) {
		return fail(fmt.Errorf("sealed segment size: %w", ErrCorrupt))
	}
	headerBytes := make([]byte, SegmentHeaderSize)
	if err := readFullAt(file, headerBytes, 0); err != nil {
		return fail(err)
	}
	header, err := DecodeSegmentHeader(headerBytes)
	if err != nil {
		return fail(err)
	}
	if header != expected {
		return fail(fmt.Errorf("sealed segment identity: %w", ErrCorrupt))
	}
	footerBytes := make([]byte, SegmentFooterSize)
	if err := readFullAt(file, footerBytes, int64(summary.ValidEnd)); err != nil {
		return fail(err)
	}
	footer, err := DecodeSegmentFooter(footerBytes)
	if err != nil {
		return fail(err)
	}
	want := SegmentFooter{SegmentID: summary.SegmentID, DataEnd: summary.ValidEnd, FirstAddr: summary.FirstAddr, LastAddr: summary.LastAddr, RecordCount: summary.RecordCount}
	if footer != want {
		return fail(fmt.Errorf("sealed segment summary: %w", ErrCorrupt))
	}
	return &sealedSegment{file: file, path: path, header: header, summary: summary}, nil
}

func readRecordAt(file fileHandle, summary SegmentSummary, addr VAddr) ([]byte, error) {
	if !addr.Valid() || addr.SegmentID() != summary.SegmentID || addr.Offset() < SegmentHeaderSize || addr.Offset() >= summary.ValidEnd {
		return nil, ErrInvalidVAddr
	}
	hint, err := addr.ReadHint()
	if err != nil {
		return nil, err
	}
	available := summary.ValidEnd - addr.Offset()
	firstSize := hint
	if firstSize > available {
		firstSize = available
	}
	if firstSize < RecordHeaderSize {
		return nil, fmt.Errorf("record boundary: %w", ErrCorrupt)
	}
	first := make([]byte, firstSize)
	if err := readFullAt(file, first, int64(addr.Offset())); err != nil {
		return nil, err
	}
	header, err := DecodeRecordHeader(first[:RecordHeaderSize])
	if err != nil {
		return nil, err
	}
	if header.Addr != addr || header.PhysicalSize > available {
		return nil, fmt.Errorf("record identity or boundary: %w", ErrCorrupt)
	}
	encoded := first
	if header.PhysicalSize < uint32(len(encoded)) {
		encoded = encoded[:header.PhysicalSize]
	} else if header.PhysicalSize > uint32(len(encoded)) {
		encoded = make([]byte, header.PhysicalSize)
		copy(encoded, first)
		if err := readFullAt(file, encoded[len(first):], int64(addr.Offset())+int64(len(first))); err != nil {
			return nil, err
		}
	}
	_, payload, err := DecodeRecord(encoded)
	if err != nil {
		return nil, err
	}
	return append([]byte(nil), payload...), nil
}

func scanRecordRange(file fileHandle, summary SegmentSummary, from uint32, visit func(AppendResult, []byte) error) error {
	if visit == nil || from < SegmentHeaderSize || from > summary.ValidEnd || from&uint32(RecordAlignment-1) != 0 {
		return ErrInvalidConfig
	}
	for offset := from; offset < summary.ValidEnd; {
		headerBytes := make([]byte, RecordHeaderSize)
		if err := readFullAt(file, headerBytes, int64(offset)); err != nil {
			return err
		}
		header, err := DecodeRecordHeader(headerBytes)
		if err != nil {
			return err
		}
		if header.Addr.SegmentID() != summary.SegmentID || header.Addr.Offset() != offset || header.PhysicalSize > summary.ValidEnd-offset {
			return fmt.Errorf("scan record boundary: %w", ErrCorrupt)
		}
		encoded := make([]byte, header.PhysicalSize)
		if err := readFullAt(file, encoded, int64(offset)); err != nil {
			return err
		}
		decoded, payload, err := DecodeRecord(encoded)
		if err != nil || decoded != header {
			if err == nil {
				err = ErrCorrupt
			}
			return err
		}
		result, err := NewAppendResult(header.Addr, header.PhysicalSize)
		if err != nil {
			return err
		}
		if err := visit(result, payload); err != nil {
			return err
		}
		offset += header.PhysicalSize
	}
	return nil
}

func (s *activeSegment) read(addr VAddr) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.file == nil {
		return nil, ErrClosed
	}
	return readRecordAt(s.file, s.summaryLocked(), addr)
}
func (s *activeSegment) scan(from uint32, visit func(AppendResult, []byte) error) error {
	return s.scanTo(from, s.summary().ValidEnd, visit)
}
func (s *activeSegment) scanTo(from, limit uint32, visit func(AppendResult, []byte) error) error {
	s.mu.RLock()
	if s.file == nil {
		s.mu.RUnlock()
		return ErrClosed
	}
	file := s.file
	summary := s.summaryLocked()
	s.mu.RUnlock()
	if limit > summary.ValidEnd {
		return ErrInvalidLogPos
	}
	summary.ValidEnd = limit
	return scanRecordRange(file, summary, from, visit)
}
func (s *sealedSegment) read(addr VAddr) ([]byte, error) {
	if s.file == nil {
		return nil, ErrClosed
	}
	return readRecordAt(s.file, s.summary, addr)
}
func (s *sealedSegment) scan(from uint32, visit func(AppendResult, []byte) error) error {
	if s.file == nil {
		return ErrClosed
	}
	return scanRecordRange(s.file, s.summary, from, visit)
}
func (s *activeSegment) close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.file == nil {
		return nil
	}
	if s.sealed && s.transferred {
		return nil
	}
	err := s.file.Close()
	s.file = nil
	return err
}

func (s *activeSegment) transferSealedOwnership(sealed *sealedSegment) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.sealed || s.transferred || s.file == nil || sealed == nil || sealed.file != s.file {
		return ErrInvalidConfig
	}
	s.transferred = true
	return nil
}
func (s *sealedSegment) close() error {
	if s.file == nil {
		return nil
	}
	err := s.file.Close()
	s.file = nil
	return err
}

func hitSegmentFault(hook segmentFaultHook, point segmentFaultPoint) error {
	if hook == nil {
		return nil
	}
	return hook(point)
}

package mapstore

import (
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"github.com/akzj/ridstore/internal/model"
	"github.com/akzj/ridstore/internal/recordlog"
)

const mappingDirectory = "mapping-v2"

type segmentFile struct {
	file    *os.File
	header  SegmentHeader
	summary SegmentSummary
}

type Store struct {
	writerMu sync.Mutex
	mu       sync.RWMutex

	root     string
	catalog  CatalogPort
	state    CatalogSnapshot
	active   *segmentFile
	sealed   map[model.MapSegmentID]*segmentFile
	nextSeq  uint64
	closed   bool
	poisoned bool
	readers  uint64
	readDone chan struct{}
	hook     FaultHook
}

func CreateInitialSegment(root string, storeID StoreID, segmentSize uint32) error {
	header := SegmentHeader{StoreID: storeID, SegmentID: 1, SegmentSize: segmentSize}
	if _, err := EncodeSegmentHeader(header); err != nil || root == "" {
		return errors.Join(ErrInvalid, err)
	}
	dir, err := ensureDirectory(root)
	if err != nil {
		return err
	}
	creating := filepath.Join(dir, creatingName(1))
	active := filepath.Join(dir, activeName(1))
	file, err := os.OpenFile(creating, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
	if err != nil {
		return err
	}
	encoded, _ := EncodeSegmentHeader(header)
	if err := writeFullAt(file, encoded[:], 0); err != nil {
		return errors.Join(err, file.Close())
	}
	if err := file.Sync(); err != nil {
		return errors.Join(err, file.Close())
	}
	if err := os.Rename(creating, active); err != nil {
		return errors.Join(err, file.Close())
	}
	if err := syncDirectory(dir); err != nil {
		return errors.Join(err, file.Close())
	}
	return file.Close()
}

// EnsureInitialSegment idempotently completes creation of the first empty
// mapping segment. It is only valid before the initial Catalog generation is
// published; callers must serialize it with the store directory lock.
func EnsureInitialSegment(root string, storeID StoreID, segmentSize uint32) error {
	header := SegmentHeader{StoreID: storeID, SegmentID: 1, SegmentSize: segmentSize}
	if _, err := EncodeSegmentHeader(header); err != nil || root == "" {
		return errors.Join(ErrInvalid, err)
	}
	dir, err := ensureDirectory(root)
	if err != nil {
		return err
	}
	activePath := filepath.Join(dir, activeName(1))
	if _, err := os.Lstat(activePath); err == nil {
		segment, repaired, err := openActive(root, header, 0, nil)
		if err != nil {
			return err
		}
		invalid := repaired || segment.summary.ValidEnd != SegmentHeaderSize || segment.summary.NodeCount != 0
		closeErr := segment.file.Close()
		if invalid {
			return errors.Join(ErrCorrupt, closeErr)
		}
		creatingPath := filepath.Join(dir, creatingName(1))
		if err := os.Remove(creatingPath); err == nil {
			return errors.Join(syncDirectory(dir), closeErr)
		} else if !errors.Is(err, os.ErrNotExist) {
			return errors.Join(err, closeErr)
		}
		return closeErr
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	creatingPath := filepath.Join(dir, creatingName(1))
	if err := os.Remove(creatingPath); err == nil {
		if err := syncDirectory(dir); err != nil {
			return err
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return CreateInitialSegment(root, storeID, segmentSize)
}

func Open(root string, catalog CatalogPort) (*Store, error) {
	return OpenWithFaultHook(root, catalog, nil)
}

func OpenWithFaultHook(root string, catalog CatalogPort, hook FaultHook) (*Store, error) {
	if root == "" || catalog == nil {
		return nil, ErrInvalid
	}
	state, err := recoverRotation(root, catalog, hook)
	if err != nil {
		return nil, err
	}
	if err := state.validate(); err != nil {
		return nil, err
	}
	store := &Store{
		root: root, catalog: catalog, state: state.Clone(), sealed: make(map[model.MapSegmentID]*segmentFile),
		nextSeq: 1, readDone: make(chan struct{}), hook: hook,
	}
	fail := func(cause error) (*Store, error) {
		return nil, errors.Join(cause, store.closeFiles())
	}
	var lastSeq uint64
	for _, summary := range state.SealedSegments {
		segment, err := openSealed(root, state.headerFor(summary.SegmentID), summary)
		if err != nil {
			return fail(err)
		}
		store.sealed[summary.SegmentID] = segment
		if segment.summary.NodeCount != 0 && lastSeq != 0 && segment.summary.FirstSeq <= lastSeq {
			return fail(ErrCorrupt)
		}
		if segment.summary.NodeCount != 0 && segment.summary.LastSeq >= store.nextSeq {
			if segment.summary.LastSeq == math.MaxUint64 {
				return fail(ErrCorrupt)
			}
			store.nextSeq = segment.summary.LastSeq + 1
			lastSeq = segment.summary.LastSeq
		}
	}
	active, repaired, err := openActive(root, state.headerFor(state.ActiveSegment), state.Root, hook)
	if err != nil {
		return fail(err)
	}
	store.active = active
	if active.summary.NodeCount != 0 && lastSeq != 0 && active.summary.FirstSeq <= lastSeq {
		return fail(ErrCorrupt)
	}
	if active.summary.NodeCount != 0 && active.summary.LastSeq >= store.nextSeq {
		if active.summary.LastSeq == math.MaxUint64 {
			return fail(ErrCorrupt)
		}
		store.nextSeq = active.summary.LastSeq + 1
	}
	if state.Root != 0 {
		node, err := store.readLocked(state.Root)
		if err != nil || node.Level != MaxLevel || node.Prefix != 0 || node.CoveredCommitSeq > state.Covered {
			if err == nil {
				err = ErrCorrupt
			}
			return fail(err)
		}
	}
	if repaired {
		// A repaired tail has been truncated and synced by openActive. No
		// catalog field describes an active end, so no metadata install follows.
	}
	return store, nil
}

func (s *Store) Append(level uint8, prefix uint64, covered model.CommitSeq, slots [NodeSlots]uint64) (model.MapAddr, error) {
	return s.appendBuild(NodeBuild{Level: level, Prefix: prefix, CoveredCommitSeq: covered, Slots: slots})
}

func (s *Store) AppendLeaf(prefix uint64, covered model.CommitSeq, refs [NodeSlots]recordlog.RecordRef) (model.MapAddr, error) {
	var build NodeBuild
	build.Level, build.Prefix, build.CoveredCommitSeq = 0, prefix, covered
	for index, ref := range refs {
		build.Slots[index] = uint64(ref.Addr)
		build.Sizes[index] = ref.PhysicalSize
	}
	return s.appendBuild(build)
}

func (s *Store) appendBuild(build NodeBuild) (model.MapAddr, error) {
	s.writerMu.Lock()
	defer s.writerMu.Unlock()
	s.mu.RLock()
	if s.closed {
		s.mu.RUnlock()
		return 0, ErrClosed
	}
	if s.poisoned {
		s.mu.RUnlock()
		return 0, ErrPoisoned
	}
	if s.nextSeq == math.MaxUint64 {
		s.mu.RUnlock()
		return 0, ErrFull
	}
	nextSeq := s.nextSeq
	active := s.active
	segmentSize := s.state.SegmentSize
	s.mu.RUnlock()
	build.NodeSeq = nextSeq
	encoded, err := EncodeNode(build)
	if err != nil {
		return 0, err
	}
	if uint64(active.summary.ValidEnd)+uint64(len(encoded))+uint64(SegmentFooterSize) > uint64(segmentSize) {
		if active.summary.NodeCount == 0 {
			return 0, ErrFull
		}
		if err := s.rotate(); err != nil {
			s.markPoisoned()
			return 0, errors.Join(ErrPoisoned, err)
		}
		s.mu.RLock()
		nextSeq, active, segmentSize = s.nextSeq, s.active, s.state.SegmentSize
		s.mu.RUnlock()
		build.NodeSeq = nextSeq
		encoded, err = EncodeNode(build)
		if err != nil || uint64(active.summary.ValidEnd)+uint64(len(encoded))+uint64(SegmentFooterSize) > uint64(segmentSize) {
			return 0, errors.Join(ErrFull, err)
		}
	}
	offset := active.summary.ValidEnd
	if err := hitFault(s.hook, FaultBeforeAppendWrite); err != nil {
		s.markPoisoned()
		return 0, errors.Join(ErrPoisoned, err)
	}
	if err := writeFullAt(active.file, encoded, int64(offset)); err != nil {
		s.markPoisoned()
		return 0, errors.Join(ErrPoisoned, err)
	}
	addr, err := model.NewMapAddr(active.header.SegmentID, offset)
	if err != nil {
		s.markPoisoned()
		return 0, errors.Join(ErrPoisoned, err)
	}
	s.mu.Lock()
	if s.active != active || s.nextSeq != nextSeq || s.closed {
		s.poisoned = true
		s.mu.Unlock()
		return 0, errors.Join(ErrPoisoned, ErrCorrupt)
	}
	if active.summary.NodeCount == 0 {
		active.summary.FirstSeq = nextSeq
	}
	active.summary.LastSeq = nextSeq
	active.summary.NodeCount++
	active.summary.ValidEnd += uint32(len(encoded))
	s.nextSeq++
	s.mu.Unlock()
	return addr, nil
}

// rotate is serialized by writerMu. The old active descriptor stays open while
// its immutable prefix is sealed and renamed, so readers remain concurrent;
// only the final in-memory pointer swap takes mu exclusively.
func (s *Store) rotate() error {
	s.mu.RLock()
	state := s.state.Clone()
	active := s.active
	s.mu.RUnlock()
	fresh := s.catalog.SnapshotMapStore()
	if err := fresh.validate(); err != nil {
		return err
	}
	if fresh.StoreID != state.StoreID || fresh.SegmentSize != state.SegmentSize || fresh.ActiveSegment != state.ActiveSegment || fresh.NextSegment != state.NextSegment {
		return ErrCorrupt
	}
	journal := rotationJournal{
		BaseGeneration: fresh.Generation, StoreID: fresh.StoreID, SegmentSize: fresh.SegmentSize,
		Old: active.summary, NewActive: fresh.NextSegment, NextSegment: fresh.NextSegment + 1,
	}
	if err := installRotationJournal(s.root, journal, s.hook); err != nil {
		return err
	}
	sealed, err := sealActive(s.root, active, journal.Old, s.hook)
	if err != nil {
		return err
	}
	newActive, err := createActive(s.root, fresh.headerFor(journal.NewActive), s.hook)
	if err != nil {
		return err
	}
	installed, err := installCatalogRotation(s.catalog, fresh, journal)
	if err != nil {
		_ = newActive.file.Close()
		return err
	}
	s.mu.Lock()
	if s.closed || s.active != active || s.state.Generation != state.Generation {
		_ = newActive.file.Close()
		s.mu.Unlock()
		return ErrCorrupt
	}
	s.sealed[journal.Old.SegmentID] = sealed
	s.active, s.state = newActive, installed
	s.mu.Unlock()
	return removeRotationJournal(s.root, s.hook)
}

func (s *Store) Sync() error {
	s.writerMu.Lock()
	defer s.writerMu.Unlock()
	s.mu.RLock()
	if s.closed {
		s.mu.RUnlock()
		return ErrClosed
	}
	if s.poisoned {
		s.mu.RUnlock()
		return ErrPoisoned
	}
	active := s.active
	s.mu.RUnlock()
	if err := hitFault(s.hook, FaultBeforeSync); err != nil {
		s.markPoisoned()
		return errors.Join(ErrPoisoned, err)
	}
	if err := active.file.Sync(); err != nil {
		s.markPoisoned()
		return errors.Join(ErrPoisoned, err)
	}
	return nil
}

func (s *Store) Read(addr model.MapAddr) (Node, error) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return Node{}, ErrClosed
	}
	segment, err := s.segmentForReadLocked(addr)
	if err != nil {
		s.mu.Unlock()
		return Node{}, err
	}
	file, validEnd := segment.file, segment.summary.ValidEnd
	s.readers++
	s.mu.Unlock()
	defer s.releaseReader()
	if err := hitFault(s.hook, FaultBeforeRead); err != nil {
		return Node{}, err
	}
	return readNodeAt(file, validEnd, addr)
}

func (s *Store) readLocked(addr model.MapAddr) (Node, error) {
	segment, err := s.segmentForReadLocked(addr)
	if err != nil {
		return Node{}, err
	}
	return readNode(segment, addr)
}

func (s *Store) segmentForReadLocked(addr model.MapAddr) (*segmentFile, error) {
	if !addr.Valid() {
		return nil, ErrInvalid
	}
	segment := s.sealed[addr.SegmentID()]
	if addr.SegmentID() == s.state.ActiveSegment {
		segment = s.active
	}
	if segment == nil || addr.Offset() > segment.summary.ValidEnd || segment.summary.ValidEnd-addr.Offset() < NodeHeaderSize {
		return nil, ErrInvalid
	}
	return segment, nil
}

func readNode(segment *segmentFile, addr model.MapAddr) (Node, error) {
	return readNodeAt(segment.file, segment.summary.ValidEnd, addr)
}

func readNodeAt(file *os.File, validEnd uint32, addr model.MapAddr) (Node, error) {
	header := make([]byte, NodeHeaderSize)
	if _, err := file.ReadAt(header, int64(addr.Offset())); err != nil {
		return Node{}, err
	}
	_, size, err := decodeNodeHeader(header, validEnd-addr.Offset())
	if err != nil {
		return Node{}, err
	}
	encoded := make([]byte, size)
	if _, err := file.ReadAt(encoded, int64(addr.Offset())); err != nil {
		return Node{}, err
	}
	node, _, err := DecodeNode(encoded, validEnd-addr.Offset())
	return node, err
}

func (s *Store) Close() error {
	s.writerMu.Lock()
	defer s.writerMu.Unlock()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	for s.readers != 0 {
		done := s.readDone
		s.mu.Unlock()
		<-done
		s.mu.Lock()
	}
	return s.closeFiles()
}

func (s *Store) releaseReader() {
	s.mu.Lock()
	if s.readers == 0 {
		s.poisoned = true
	} else {
		s.readers--
		if s.readers == 0 {
			close(s.readDone)
			s.readDone = make(chan struct{})
		}
	}
	s.mu.Unlock()
}

func (s *Store) markPoisoned() {
	s.mu.Lock()
	s.poisoned = true
	s.mu.Unlock()
}

func (s *Store) closeFiles() error {
	var result error
	if s.active != nil && s.active.file != nil {
		result = errors.Join(result, s.active.file.Close())
	}
	ids := make([]int, 0, len(s.sealed))
	for id := range s.sealed {
		ids = append(ids, int(id))
	}
	sort.Ints(ids)
	for _, raw := range ids {
		result = errors.Join(result, s.sealed[model.MapSegmentID(raw)].file.Close())
	}
	return result
}

func ensureDirectory(root string) (string, error) {
	dir := filepath.Join(root, mappingDirectory)
	if err := os.Mkdir(dir, 0o700); err == nil {
		if err := syncDirectory(root); err != nil {
			return "", err
		}
	} else if !errors.Is(err, os.ErrExist) {
		return "", err
	}
	info, err := os.Stat(dir)
	if err != nil || !info.IsDir() {
		return "", errors.Join(ErrCorrupt, err)
	}
	return dir, nil
}

func activeName(id model.MapSegmentID) string   { return fmt.Sprintf("map-%010d.active", id) }
func sealedName(id model.MapSegmentID) string   { return fmt.Sprintf("map-%010d.sealed", id) }
func creatingName(id model.MapSegmentID) string { return fmt.Sprintf("map-%010d.creating", id) }

func openSealed(root string, expected SegmentHeader, expectedRef SegmentRef) (*segmentFile, error) {
	path := filepath.Join(root, mappingDirectory, sealedName(expected.SegmentID))
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	fail := func(cause error) (*segmentFile, error) { return nil, errors.Join(cause, file.Close()) }
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() != int64(expectedRef.ValidEnd+SegmentFooterSize) {
		return fail(errors.Join(ErrCorrupt, err))
	}
	if err := verifyHeader(file, expected); err != nil {
		return fail(err)
	}
	footerBytes := make([]byte, SegmentFooterSize)
	if _, err := file.ReadAt(footerBytes, int64(expectedRef.ValidEnd)); err != nil {
		return fail(err)
	}
	footer, err := DecodeSegmentFooter(footerBytes)
	if err != nil || footer.SegmentID != expectedRef.SegmentID || footer.ValidEnd != expectedRef.ValidEnd {
		return fail(errors.Join(ErrCorrupt, err))
	}
	expectedSummary := SegmentSummary{SegmentID: footer.SegmentID, ValidEnd: footer.ValidEnd, FirstSeq: footer.FirstSeq, LastSeq: footer.LastSeq, NodeCount: footer.NodeCount}
	actual, _, err := scanNodes(file, expected, expectedRef.ValidEnd, 0)
	if err != nil || actual != expectedSummary {
		return fail(errors.Join(ErrCorrupt, err))
	}
	return &segmentFile{file: file, header: expected, summary: expectedSummary}, nil
}

func openActive(root string, expected SegmentHeader, rootAddr model.MapAddr, hook FaultHook) (*segmentFile, bool, error) {
	path := filepath.Join(root, mappingDirectory, activeName(expected.SegmentID))
	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return nil, false, err
	}
	fail := func(cause error) (*segmentFile, bool, error) { return nil, false, errors.Join(cause, file.Close()) }
	if err := verifyHeader(file, expected); err != nil {
		return fail(err)
	}
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() < int64(SegmentHeaderSize) || info.Size() > int64(expected.SegmentSize-SegmentFooterSize) {
		return fail(errors.Join(ErrCorrupt, err))
	}
	summary, repaired, err := scanNodes(file, expected, uint32(info.Size()), rootAddr)
	if err != nil {
		return fail(err)
	}
	if repaired {
		if err := hitFault(hook, FaultBeforeTailTruncate); err != nil {
			return fail(err)
		}
		if err := file.Truncate(int64(summary.ValidEnd)); err != nil {
			return fail(err)
		}
		if err := hitFault(hook, FaultBeforeTailSync); err != nil {
			return fail(err)
		}
		if err := file.Sync(); err != nil {
			return fail(err)
		}
	}
	return &segmentFile{file: file, header: expected, summary: summary}, repaired, nil
}

func verifyHeader(file *os.File, expected SegmentHeader) error {
	encoded := make([]byte, SegmentHeaderSize)
	if _, err := file.ReadAt(encoded, 0); err != nil {
		return err
	}
	header, err := DecodeSegmentHeader(encoded)
	if err != nil {
		return err
	}
	if header != expected {
		return ErrCorrupt
	}
	return nil
}

func scanNodes(file *os.File, header SegmentHeader, end uint32, root model.MapAddr) (SegmentSummary, bool, error) {
	return scanNodesWithVisitor(file, header, end, root, nil)
}

func scanNodesWithVisitor(file *os.File, header SegmentHeader, end uint32, root model.MapAddr, visit func(model.MapAddr, Node, uint32) error) (SegmentSummary, bool, error) {
	result := SegmentSummary{SegmentID: header.SegmentID, ValidEnd: SegmentHeaderSize}
	for result.ValidEnd < end {
		remainingFile := end - result.ValidEnd
		if remainingFile < NodeHeaderSize {
			if root.SegmentID() == header.SegmentID && root.Offset() >= result.ValidEnd {
				return SegmentSummary{}, false, ErrCorrupt
			}
			return result, true, nil
		}
		headerBytes := make([]byte, NodeHeaderSize)
		if _, err := file.ReadAt(headerBytes, int64(result.ValidEnd)); err != nil {
			return SegmentSummary{}, false, err
		}
		node, size, err := decodeNodeHeader(headerBytes, header.SegmentSize-SegmentFooterSize-result.ValidEnd)
		if err != nil {
			return SegmentSummary{}, false, err
		}
		if size > remainingFile {
			if root.SegmentID() == header.SegmentID && root.Offset() >= result.ValidEnd {
				return SegmentSummary{}, false, ErrCorrupt
			}
			return result, true, nil
		}
		encoded := make([]byte, size)
		if _, err := file.ReadAt(encoded, int64(result.ValidEnd)); err != nil {
			return SegmentSummary{}, false, err
		}
		decoded, _, err := DecodeNode(encoded, header.SegmentSize-SegmentFooterSize-result.ValidEnd)
		if err != nil || decoded.NodeSeq != node.NodeSeq {
			return SegmentSummary{}, false, errors.Join(ErrCorrupt, err)
		}
		if visit != nil {
			addr, err := model.NewMapAddr(header.SegmentID, result.ValidEnd)
			if err != nil {
				return SegmentSummary{}, false, err
			}
			if err := visit(addr, decoded, size); err != nil {
				return SegmentSummary{}, false, err
			}
		}
		if result.NodeCount == 0 {
			result.FirstSeq = decoded.NodeSeq
		} else if decoded.NodeSeq != result.LastSeq+1 {
			return SegmentSummary{}, false, ErrCorrupt
		}
		result.LastSeq = decoded.NodeSeq
		result.NodeCount++
		result.ValidEnd += size
	}
	return result, false, nil
}

func writeFullAt(writer io.WriterAt, value []byte, offset int64) error {
	written := 0
	for written < len(value) {
		count, err := writer.WriteAt(value[written:], offset+int64(written))
		written += count
		if err != nil {
			return err
		}
		if count == 0 {
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

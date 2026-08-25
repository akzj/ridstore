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
)

const mappingDirectory = "mapping-v2"

type segmentFile struct {
	file    *os.File
	header  SegmentHeader
	summary SegmentSummary
}

type Store struct {
	mu sync.RWMutex

	root     string
	catalog  CatalogPort
	state    CatalogSnapshot
	active   *segmentFile
	sealed   map[model.MapSegmentID]*segmentFile
	nextSeq  uint64
	closed   bool
	poisoned bool
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

func Open(root string, catalog CatalogPort) (*Store, error) {
	if root == "" || catalog == nil {
		return nil, ErrInvalid
	}
	state, err := recoverRotation(root, catalog)
	if err != nil {
		return nil, err
	}
	if err := state.validate(); err != nil {
		return nil, err
	}
	store := &Store{root: root, catalog: catalog, state: state.Clone(), sealed: make(map[model.MapSegmentID]*segmentFile), nextSeq: 1}
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
	active, repaired, err := openActive(root, state.headerFor(state.ActiveSegment), state.Root)
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
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return 0, ErrClosed
	}
	if s.poisoned {
		return 0, ErrPoisoned
	}
	if s.nextSeq == math.MaxUint64 {
		return 0, ErrFull
	}
	build := NodeBuild{Level: level, NodeSeq: s.nextSeq, Prefix: prefix, CoveredCommitSeq: covered, Slots: slots}
	encoded, err := EncodeNode(build)
	if err != nil {
		return 0, err
	}
	if uint64(s.active.summary.ValidEnd)+uint64(len(encoded))+uint64(SegmentFooterSize) > uint64(s.state.SegmentSize) {
		if s.active.summary.NodeCount == 0 {
			return 0, ErrFull
		}
		if err := s.rotateLocked(); err != nil {
			s.poisoned = true
			return 0, errors.Join(ErrPoisoned, err)
		}
		build.NodeSeq = s.nextSeq
		encoded, err = EncodeNode(build)
		if err != nil || uint64(s.active.summary.ValidEnd)+uint64(len(encoded))+uint64(SegmentFooterSize) > uint64(s.state.SegmentSize) {
			return 0, errors.Join(ErrFull, err)
		}
	}
	offset := s.active.summary.ValidEnd
	if err := writeFullAt(s.active.file, encoded, int64(offset)); err != nil {
		s.poisoned = true
		return 0, errors.Join(ErrPoisoned, err)
	}
	addr, err := model.NewMapAddr(s.state.ActiveSegment, offset)
	if err != nil {
		s.poisoned = true
		return 0, errors.Join(ErrPoisoned, err)
	}
	if s.active.summary.NodeCount == 0 {
		s.active.summary.FirstSeq = s.nextSeq
	}
	s.active.summary.LastSeq = s.nextSeq
	s.active.summary.NodeCount++
	s.active.summary.ValidEnd += uint32(len(encoded))
	s.nextSeq++
	return addr, nil
}

func (s *Store) rotateLocked() error {
	fresh := s.catalog.SnapshotMapStore()
	if err := fresh.validate(); err != nil {
		return err
	}
	if fresh.StoreID != s.state.StoreID || fresh.SegmentSize != s.state.SegmentSize || fresh.ActiveSegment != s.state.ActiveSegment || fresh.NextSegment != s.state.NextSegment {
		return ErrCorrupt
	}
	journal := rotationJournal{
		BaseGeneration: fresh.Generation, StoreID: fresh.StoreID, SegmentSize: fresh.SegmentSize,
		Old: s.active.summary, NewActive: fresh.NextSegment, NextSegment: fresh.NextSegment + 1,
	}
	if err := installRotationJournal(s.root, journal); err != nil {
		return err
	}
	sealed, err := sealActive(s.root, s.active, journal.Old)
	if err != nil {
		return err
	}
	newActive, err := createActive(s.root, fresh.headerFor(journal.NewActive))
	if err != nil {
		_ = sealed.file.Close()
		return err
	}
	installed, err := installCatalogRotation(s.catalog, fresh, journal)
	if err != nil {
		_ = sealed.file.Close()
		_ = newActive.file.Close()
		return err
	}
	s.sealed[journal.Old.SegmentID] = sealed
	s.active = newActive
	s.state = installed
	return removeRotationJournal(s.root)
}

func (s *Store) Sync() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosed
	}
	if s.poisoned {
		return ErrPoisoned
	}
	if err := s.active.file.Sync(); err != nil {
		s.poisoned = true
		return errors.Join(ErrPoisoned, err)
	}
	return nil
}

func (s *Store) Read(addr model.MapAddr) (Node, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return Node{}, ErrClosed
	}
	return s.readLocked(addr)
}

func (s *Store) readLocked(addr model.MapAddr) (Node, error) {
	if !addr.Valid() {
		return Node{}, ErrInvalid
	}
	segment := s.sealed[addr.SegmentID()]
	if addr.SegmentID() == s.state.ActiveSegment {
		segment = s.active
	}
	if segment == nil || addr.Offset() > segment.summary.ValidEnd || segment.summary.ValidEnd-addr.Offset() < NodeHeaderSize {
		return Node{}, ErrInvalid
	}
	header := make([]byte, NodeHeaderSize)
	if _, err := segment.file.ReadAt(header, int64(addr.Offset())); err != nil {
		return Node{}, err
	}
	_, size, err := decodeNodeHeader(header, segment.summary.ValidEnd-addr.Offset())
	if err != nil {
		return Node{}, err
	}
	encoded := make([]byte, size)
	if _, err := segment.file.ReadAt(encoded, int64(addr.Offset())); err != nil {
		return Node{}, err
	}
	node, _, err := DecodeNode(encoded, segment.summary.ValidEnd-addr.Offset())
	return node, err
}

func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	return s.closeFiles()
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

func openActive(root string, expected SegmentHeader, rootAddr model.MapAddr) (*segmentFile, bool, error) {
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
		if err := file.Truncate(int64(summary.ValidEnd)); err != nil {
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

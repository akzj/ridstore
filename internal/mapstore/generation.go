package mapstore

import (
	"errors"
	"math"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"github.com/akzj/ridstore/internal/model"
	"github.com/akzj/ridstore/internal/recordlog"
)

// Generation describes one independently built, fully synced Mapping file
// set. It is not visible until a higher layer publishes it in the Catalog.
type Generation struct {
	SealedSegments []SegmentRef
	ActiveSegment  model.MapSegmentID
	NextSegment    model.MapSegmentID
	Root           model.MapAddr
	Covered        model.CommitSeq
}

// GenerationWriter builds Mapping files in an isolated staging root. It
// shares the v2 node and segment formats but never reads or mutates Catalog.
type GenerationWriter struct {
	mu          sync.RWMutex
	root        string
	storeID     StoreID
	segmentSize uint32
	active      *segmentFile
	sealed      map[model.MapSegmentID]*segmentFile
	refs        []SegmentRef
	nextSegment model.MapSegmentID
	nextSeq     uint64
	hook        FaultHook
	finished    bool
	closed      bool
	poisoned    bool
}

func CreateGenerationWriter(root string, storeID StoreID, segmentSize uint32, firstSegment model.MapSegmentID, hook FaultHook) (*GenerationWriter, error) {
	if root == "" || firstSegment == 0 || firstSegment == model.MapSegmentID(math.MaxUint32) || !validSegmentHeader(SegmentHeader{
		StoreID: storeID, SegmentID: firstSegment, PreviousSegment: firstSegment - 1, SegmentSize: segmentSize,
	}) {
		return nil, ErrInvalid
	}
	if _, err := os.Lstat(filepath.Join(root, mappingDirectory)); err == nil {
		return nil, ErrInvalid
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	active, err := createActive(root, SegmentHeader{
		StoreID: storeID, SegmentID: firstSegment, PreviousSegment: firstSegment - 1, SegmentSize: segmentSize,
	}, hook)
	if err != nil {
		return nil, err
	}
	return &GenerationWriter{
		root: root, storeID: storeID, segmentSize: segmentSize, active: active,
		sealed: make(map[model.MapSegmentID]*segmentFile), nextSegment: firstSegment + 1, nextSeq: 1, hook: hook,
	}, nil
}

func (w *GenerationWriter) Append(level uint8, prefix uint64, covered model.CommitSeq, slots [NodeSlots]uint64) (model.MapAddr, error) {
	return w.appendBuild(NodeBuild{Level: level, Prefix: prefix, CoveredCommitSeq: covered, Slots: slots})
}

func (w *GenerationWriter) AppendLeaf(prefix uint64, covered model.CommitSeq, refs [NodeSlots]recordlog.RecordRef) (model.MapAddr, error) {
	var build NodeBuild
	build.Level, build.Prefix, build.CoveredCommitSeq = 0, prefix, covered
	for index, ref := range refs {
		build.Slots[index] = uint64(ref.Addr)
		build.Sizes[index] = ref.PhysicalSize
	}
	return w.appendBuild(build)
}

func (w *GenerationWriter) appendBuild(build NodeBuild) (model.MapAddr, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed || w.finished {
		return 0, ErrClosed
	}
	if w.poisoned {
		return 0, ErrPoisoned
	}
	if w.nextSeq == math.MaxUint64 {
		return 0, ErrFull
	}
	build.NodeSeq = w.nextSeq
	encoded, err := EncodeNode(build)
	if err != nil {
		return 0, err
	}
	if uint64(w.active.summary.ValidEnd)+uint64(len(encoded))+uint64(SegmentFooterSize) > uint64(w.segmentSize) {
		if w.active.summary.NodeCount == 0 {
			return 0, ErrFull
		}
		if err := w.rotateLocked(); err != nil {
			w.poisoned = true
			return 0, errors.Join(ErrPoisoned, err)
		}
	}
	offset := w.active.summary.ValidEnd
	if err := hitFault(w.hook, FaultBeforeAppendWrite); err != nil {
		w.poisoned = true
		return 0, errors.Join(ErrPoisoned, err)
	}
	if err := writeFullAt(w.active.file, encoded, int64(offset)); err != nil {
		w.poisoned = true
		return 0, errors.Join(ErrPoisoned, err)
	}
	addr, err := model.NewMapAddr(w.active.header.SegmentID, offset)
	if err != nil {
		w.poisoned = true
		return 0, errors.Join(ErrPoisoned, err)
	}
	if w.active.summary.NodeCount == 0 {
		w.active.summary.FirstSeq = w.nextSeq
	}
	w.active.summary.LastSeq = w.nextSeq
	w.active.summary.NodeCount++
	w.active.summary.ValidEnd += uint32(len(encoded))
	w.nextSeq++
	return addr, nil
}

func (w *GenerationWriter) Read(addr model.MapAddr) (Node, error) {
	w.mu.RLock()
	defer w.mu.RUnlock()
	if w.closed {
		return Node{}, ErrClosed
	}
	segment := w.sealed[addr.SegmentID()]
	if w.active != nil && addr.SegmentID() == w.active.header.SegmentID {
		segment = w.active
	}
	if segment == nil || addr.Offset() > segment.summary.ValidEnd || segment.summary.ValidEnd-addr.Offset() < NodeHeaderSize {
		return Node{}, ErrInvalid
	}
	return readNode(segment, addr)
}

func (w *GenerationWriter) Finish(root model.MapAddr, covered model.CommitSeq) (Generation, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed || w.finished || w.poisoned || (root != 0 && !root.Valid()) {
		return Generation{}, ErrInvalid
	}
	if root != 0 {
		segment := w.sealed[root.SegmentID()]
		if root.SegmentID() == w.active.header.SegmentID {
			segment = w.active
		}
		if segment == nil {
			return Generation{}, ErrInvalid
		}
		node, err := readNode(segment, root)
		if err != nil || node.Level != MaxLevel || node.Prefix != 0 || node.CoveredCommitSeq != covered {
			return Generation{}, errors.Join(ErrCorrupt, err)
		}
	}
	if err := hitFault(w.hook, FaultBeforeSync); err != nil {
		w.poisoned = true
		return Generation{}, errors.Join(ErrPoisoned, err)
	}
	if err := w.active.file.Sync(); err != nil {
		w.poisoned = true
		return Generation{}, errors.Join(ErrPoisoned, err)
	}
	w.finished = true
	return Generation{
		SealedSegments: append([]SegmentRef(nil), w.refs...), ActiveSegment: w.active.header.SegmentID,
		NextSegment: w.nextSegment, Root: root, Covered: covered,
	}, nil
}

func (w *GenerationWriter) Close() error {
	if w == nil {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return nil
	}
	w.closed = true
	var result error
	if w.active != nil {
		result = errors.Join(result, w.active.file.Close())
	}
	ids := make([]int, 0, len(w.sealed))
	for id := range w.sealed {
		ids = append(ids, int(id))
	}
	sort.Ints(ids)
	for _, raw := range ids {
		result = errors.Join(result, w.sealed[model.MapSegmentID(raw)].file.Close())
	}
	return result
}

func (w *GenerationWriter) rotateLocked() error {
	if w.nextSegment == 0 || w.nextSegment == model.MapSegmentID(math.MaxUint32) {
		return ErrFull
	}
	sealed, err := sealActive(w.root, w.active, w.active.summary, w.hook)
	if err != nil {
		return err
	}
	ref := SegmentRef{SegmentID: sealed.header.SegmentID, ValidEnd: sealed.summary.ValidEnd}
	active, err := createActive(w.root, SegmentHeader{
		StoreID: w.storeID, SegmentID: w.nextSegment, PreviousSegment: w.nextSegment - 1, SegmentSize: w.segmentSize,
	}, w.hook)
	if err != nil {
		_ = sealed.file.Close()
		return err
	}
	w.sealed[ref.SegmentID] = sealed
	w.refs = append(w.refs, ref)
	w.active = active
	w.nextSegment++
	return nil
}

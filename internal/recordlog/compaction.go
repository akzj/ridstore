package recordlog

import (
	"context"
	"errors"
	"os"
	"path/filepath"
)

// CompactionWriter writes immutable output segments outside the user append
// queue. Its IDs must come from the high compaction namespace reserved by the
// Catalog, so user rotations and compaction never contend for an active file.
type CompactionWriter struct {
	log       *Log
	ids       []SegmentID
	used      int
	active    *activeSegment
	summaries []SegmentSummary
	closed    bool
}

func (l *Log) NewCompactionWriter(ids []SegmentID) (*CompactionWriter, error) {
	if l == nil || l.root == "" || len(ids) == 0 {
		return nil, ErrInvalidConfig
	}
	for index, id := range ids {
		if !IsCompactionSegment(id) || index != 0 && id != ids[index-1]+1 {
			return nil, ErrInvalidConfig
		}
	}
	return &CompactionWriter{log: l, ids: append([]SegmentID(nil), ids...)}, nil
}

func (w *CompactionWriter) Append(ctx context.Context, payload []byte) (AppendResult, error) {
	if w == nil || w.closed || w.log == nil {
		return AppendResult{}, ErrClosed
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return AppendResult{}, err
	}
	if uint64(len(payload)) > uint64(w.log.maxPayloadBytes) {
		return AppendResult{}, ErrPayloadTooBig
	}
	physical, err := PhysicalRecordSize(uint64(len(payload)))
	if err != nil {
		return AppendResult{}, err
	}
	if w.active == nil || w.active.remaining() < physical {
		if err := w.rotate(); err != nil {
			return AppendResult{}, err
		}
	}
	start := w.active.summary().ValidEnd
	addr, err := NewVAddr(w.active.header.SegmentID, start, physical)
	if err != nil {
		return AppendResult{}, err
	}
	result, err := NewAppendResult(addr, physical)
	if err != nil {
		return AppendResult{}, err
	}
	encoded, err := EncodeRecord(addr, payload)
	if err != nil {
		return AppendResult{}, err
	}
	if _, err := w.active.appendEncoded(encoded, []recordExtent{{Result: result, Size: physical}}); err != nil {
		return AppendResult{}, err
	}
	return result, nil
}

func (w *CompactionWriter) rotate() error {
	if w.active != nil {
		sealed, summary, err := w.active.seal()
		if err != nil {
			return err
		}
		w.summaries = append(w.summaries, summary)
		if err := sealed.close(); err != nil {
			return err
		}
		w.active = nil
	}
	if w.used == len(w.ids) {
		return ErrInvalidConfig
	}
	id := w.ids[w.used]
	w.used++
	snapshot := w.log.catalog.SnapshotRecordLog()
	header := SegmentHeader{LogID: snapshot.LogID, SegmentID: id, PreviousSegment: id - 1, SegmentSize: snapshot.SegmentSize}
	active, err := createActiveSegment(w.log.root, header, w.log.files, w.log.hook)
	if err != nil {
		return err
	}
	w.active = active
	return nil
}

func (w *CompactionWriter) Finish() ([]SegmentSummary, error) {
	if w == nil || w.closed {
		return nil, ErrClosed
	}
	if w.active != nil {
		sealed, summary, err := w.active.seal()
		if err != nil {
			return nil, err
		}
		w.summaries = append(w.summaries, summary)
		if err := sealed.close(); err != nil {
			return nil, err
		}
		w.active = nil
	}
	w.closed = true
	return append([]SegmentSummary(nil), w.summaries...), nil
}

// Abort releases the writer's unpublished active descriptor. Sealed outputs
// have already transferred ownership to closed descriptors and are removed by
// RemoveUnpublishedCompactionFiles after Abort returns.
func (w *CompactionWriter) Abort() error {
	if w == nil || w.closed {
		return nil
	}
	w.closed = true
	if w.active == nil {
		return nil
	}
	err := w.active.close()
	w.active = nil
	return err
}

func (l *Log) RegisterCompactionOutputs(summaries []SegmentSummary) error {
	if l == nil || len(summaries) == 0 {
		return ErrInvalidConfig
	}
	snapshot := l.catalog.SnapshotRecordLog()
	opened := make([]*sealedSegment, 0, len(summaries))
	for _, summary := range summaries {
		if !IsCompactionSegment(summary.SegmentID) {
			return ErrInvalidConfig
		}
		segment, err := openSealedSegment(l.root, snapshot.headerFor(summary.SegmentID), summary, l.files)
		if err != nil {
			for _, item := range opened {
				_ = item.close()
			}
			return err
		}
		opened = append(opened, segment)
	}
	if err := l.registry.publishSealed(opened); err != nil {
		for _, item := range opened {
			_ = item.close()
		}
		return err
	}
	return nil
}

func (l *Log) RemoveUnpublishedCompactionFiles(ids []SegmentID) error {
	if l == nil || l.root == "" {
		return ErrInvalidConfig
	}
	dir := recordsPath(l.root)
	var result error
	for _, id := range ids {
		if !IsCompactionSegment(id) {
			return ErrInvalidConfig
		}
		for _, name := range []string{creatingSegmentName(id), activeSegmentName(id), sealedSegmentName(id)} {
			if err := l.files.remove(filepath.Join(dir, name)); err != nil && !errors.Is(err, os.ErrNotExist) {
				result = errors.Join(result, err)
			}
		}
	}
	return errors.Join(result, l.files.syncDir(dir))
}

// CleanupUnpublishedCompactionFiles is the pre-Open recovery form. It is safe
// only for IDs proven absent from the authoritative Catalog.
func CleanupUnpublishedCompactionFiles(root string, ids []SegmentID) error {
	if root == "" {
		return ErrInvalidConfig
	}
	files := osFileBackend{}
	dir := recordsPath(root)
	var result error
	for _, id := range ids {
		if !IsCompactionSegment(id) {
			return ErrInvalidConfig
		}
		for _, name := range []string{creatingSegmentName(id), activeSegmentName(id), sealedSegmentName(id)} {
			if err := files.remove(filepath.Join(dir, name)); err != nil && !errors.Is(err, os.ErrNotExist) {
				result = errors.Join(result, err)
			}
		}
	}
	return errors.Join(result, files.syncDir(dir))
}

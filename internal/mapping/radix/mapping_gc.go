package radix

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/akzj/ridstore/internal/base"
	"github.com/akzj/ridstore/internal/failpoint"
	storeformat "github.com/akzj/ridstore/internal/format"
	"github.com/akzj/ridstore/internal/segment"
)

const (
	PointMappingGCPrepared          failpoint.Point = "mapping-gc.prepared"
	PointMappingGCCopying           failpoint.Point = "mapping-gc.copying"
	PointMappingGCCopied            failpoint.Point = "mapping-gc.copied"
	PointMappingGCFilesDurable      failpoint.Point = "mapping-gc.files-durable"
	PointMappingGCManifestInstalled failpoint.Point = "mapping-gc.manifest-installed"
	PointMappingGCRuntimeInstalled  failpoint.Point = "mapping-gc.runtime-installed"
	PointMappingGCTrashed           failpoint.Point = "mapping-gc.trashed"
)

type mapGCFile struct {
	id       base.MapSegmentID
	tempPath string
	final    string
	sealed   bool
	validEnd uint64
	first    base.NodeSeq
	last     base.NodeSeq
	count    uint64
}

type mapGCWriter struct {
	root        string
	uuid        base.StoreUUID
	segmentSize uint64
	operationID [16]byte
	currentID   base.MapSegmentID
	nextID      base.MapSegmentID
	nextNodeSeq base.NodeSeq
	file        *os.File
	end         uint64
	first       base.NodeSeq
	last        base.NodeSeq
	count       uint64
	files       []mapGCFile
}

func newMapGCWriter(root string, uuid base.StoreUUID, segmentSize uint64, firstID base.MapSegmentID, firstSeq base.NodeSeq, operationID [16]byte) (*mapGCWriter, error) {
	if root == "" || uuid == (base.StoreUUID{}) || firstID == 0 || firstSeq == 0 || operationID == ([16]byte{}) {
		return nil, base.ErrInvalidConfig
	}
	w := &mapGCWriter{root: root, uuid: uuid, segmentSize: segmentSize, currentID: firstID, nextID: firstID + 1, nextNodeSeq: firstSeq, operationID: operationID}
	if w.nextID == 0 {
		return nil, base.ErrGenerationExhausted
	}
	if err := w.createCurrent(); err != nil {
		return nil, err
	}
	return w, nil
}

func (w *mapGCWriter) createCurrent() error {
	header, err := storeformat.EncodeSegmentHeader(storeformat.SegmentHeader{
		Kind: storeformat.SegmentKindMapping, StoreUUID: w.uuid, FileID: uint32(w.currentID), FirstSeq: uint64(w.nextNodeSeq), CreatedUnixNano: uint64(time.Now().UnixNano()),
	})
	if err != nil {
		return err
	}
	path := mappingGCTempPath(w.root, w.operationID, w.currentID)
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	if _, err := writeFullAt(file, header[:], 0); err != nil {
		return errors.Join(err, file.Close(), os.Remove(path))
	}
	w.file, w.end, w.first, w.last, w.count = file, storeformat.SegmentHeaderSize, w.nextNodeSeq, 0, 0
	return nil
}

func (w *mapGCWriter) append(level uint8, prefix uint64, covered base.CommitSeq, slots [storeformat.MappingNodeSlots]uint64) (base.MapAddr, error) {
	build := storeformat.MappingNodeBuild{Level: level, Encoding: storeformat.NodeEncodingAuto, NodeSeq: w.nextNodeSeq, Prefix: prefix, CoveredCommitSeq: covered, Slots: slots}
	encoded, err := storeformat.EncodeMappingNode(build)
	if err != nil {
		return 0, err
	}
	if uint64(len(encoded)) > w.segmentSize-storeformat.SegmentFooterSize-w.end {
		if w.count == 0 {
			return 0, segment.ErrFull
		}
		if err := w.sealCurrent(); err != nil {
			return 0, err
		}
		if w.nextID == base.MapSegmentID(^uint32(0)) {
			return 0, base.ErrGenerationExhausted
		}
		w.currentID, w.nextID = w.nextID, w.nextID+1
		if err := w.createCurrent(); err != nil {
			return 0, err
		}
		build.NodeSeq = w.nextNodeSeq
		encoded, err = storeformat.EncodeMappingNode(build)
		if err != nil || uint64(len(encoded)) > w.segmentSize-storeformat.SegmentFooterSize-w.end {
			return 0, errors.Join(err, segment.ErrFull)
		}
	}
	offset := w.end
	if _, err := writeFullAt(w.file, encoded, int64(offset)); err != nil {
		return 0, err
	}
	w.end += uint64(len(encoded))
	w.last, w.count = w.nextNodeSeq, w.count+1
	if w.nextNodeSeq == base.NodeSeq(^uint64(0)) {
		return 0, base.ErrGenerationExhausted
	}
	w.nextNodeSeq++
	return base.NewMapAddr(w.currentID, uint32(offset))
}

func (w *mapGCWriter) sealCurrent() error {
	footer, err := storeformat.EncodeMappingSegmentFooter(storeformat.MappingSegmentFooter{
		SegmentID: w.currentID, ValidNodeEnd: w.end, FirstNodeSeq: w.first, LastNodeSeq: w.last, NodeCount: w.count,
	})
	if err != nil {
		return err
	}
	if _, err := writeFullAt(w.file, footer[:], int64(w.end)); err != nil {
		return err
	}
	if err := w.file.Sync(); err != nil {
		return err
	}
	path := w.file.Name()
	if err := w.file.Close(); err != nil {
		return err
	}
	w.files = append(w.files, mapGCFile{id: w.currentID, tempPath: path, final: filepath.Join(w.root, "mapping", sealedMapFileName(w.currentID)), sealed: true, validEnd: w.end, first: w.first, last: w.last, count: w.count})
	w.file = nil
	return nil
}

func (w *mapGCWriter) finish() error {
	if w.file == nil {
		return base.ErrCorrupt
	}
	if err := w.file.Sync(); err != nil {
		return err
	}
	path := w.file.Name()
	if err := w.file.Close(); err != nil {
		return err
	}
	last := w.last
	if last == 0 {
		last = w.first
	}
	w.files = append(w.files, mapGCFile{id: w.currentID, tempPath: path, final: filepath.Join(w.root, "mapping", activeMapFileName(w.currentID)), validEnd: w.end, first: w.first, last: last, count: w.count})
	w.file = nil
	return syncDirectory(filepath.Join(w.root, "mapping"))
}

func (w *mapGCWriter) publishFiles() error {
	for _, file := range w.files {
		if err := os.Rename(file.tempPath, file.final); err != nil {
			return err
		}
	}
	return syncDirectory(filepath.Join(w.root, "mapping"))
}

func (w *mapGCWriter) temporaryRefs() []storeformat.JournalFileRef {
	refs := make([]storeformat.JournalFileRef, 0, len(w.files))
	for _, file := range w.files {
		refs = append(refs, storeformat.JournalFileRef{Kind: storeformat.FileKindMapping, State: storeformat.FileStateTemporary, FileID: uint32(file.id), ValidEnd: file.validEnd, FirstSeq: uint64(file.first), LastSeq: uint64(file.last)})
	}
	return refs
}

func (w *mapGCWriter) finalRefs() []storeformat.JournalFileRef {
	refs := make([]storeformat.JournalFileRef, 0, len(w.files))
	for _, file := range w.files {
		state := storeformat.FileStateActive
		if file.sealed {
			state = storeformat.FileStateSealed
		}
		refs = append(refs, storeformat.JournalFileRef{Kind: storeformat.FileKindMapping, State: state, FileID: uint32(file.id), ValidEnd: file.validEnd, FirstSeq: uint64(file.first), LastSeq: uint64(file.last)})
	}
	return refs
}

func (w *mapGCWriter) sealedSummaries() []storeformat.FileSummary {
	result := make([]storeformat.FileSummary, 0, len(w.files))
	for _, file := range w.files {
		if file.sealed {
			result = append(result, storeformat.FileSummary{FileID: uint32(file.id), ValidEnd: file.validEnd, FirstSeq: uint64(file.first), LastSeq: uint64(file.last)})
		}
	}
	return result
}

func (w *mapGCWriter) cleanup() error {
	var result error
	if w.file != nil {
		result = errors.Join(result, w.file.Close())
	}
	for _, file := range w.files {
		if err := os.Remove(file.tempPath); err != nil && !errors.Is(err, os.ErrNotExist) {
			result = errors.Join(result, err)
		}
		if err := os.Remove(file.final); err != nil && !errors.Is(err, os.ErrNotExist) {
			result = errors.Join(result, err)
		}
	}
	if err := os.Remove(mappingGCTempPath(w.root, w.operationID, w.currentID)); err != nil && !errors.Is(err, os.ErrNotExist) {
		result = errors.Join(result, err)
	}
	result = errors.Join(result, syncDirectory(filepath.Join(w.root, "mapping")))
	return result
}

func (m *Mapping) Compact(ctx context.Context) (storeformat.Manifest, error) {
	if err := ctx.Err(); err != nil {
		return storeformat.Manifest{}, err
	}
	if m.store.catalog == nil {
		return storeformat.Manifest{}, base.ErrInvalidConfig
	}
	m.mu.Lock()
	if m.checkpoint {
		m.mu.Unlock()
		return storeformat.Manifest{}, base.ErrConflict
	}
	oldRoot, covered := m.root, m.rootCovered
	if oldRoot != 0 {
		m.readerMu.Lock()
		m.readers[oldRoot]++
		m.readerMu.Unlock()
	}
	m.mu.Unlock()
	pinned := oldRoot != 0
	releasePin := func() {
		if pinned {
			m.releaseRoot(oldRoot)
			pinned = false
		}
	}
	defer releasePin()

	current := m.store.catalog.Snapshot()
	sourceRefs, firstNodeSeq, err := m.store.mappingGCSourceRefs(current)
	if err != nil {
		return storeformat.Manifest{}, err
	}
	if current.NextMapSegmentID == 0 || current.NextMapSegmentID == base.MapSegmentID(^uint32(0)) || current.MaintenanceGeneration == ^uint64(0) {
		return storeformat.Manifest{}, base.ErrGenerationExhausted
	}
	var operationID [16]byte
	if _, err := rand.Read(operationID[:]); err != nil {
		return storeformat.Manifest{}, err
	}
	journal := storeformat.MaintenanceJournal{
		Generation: current.MaintenanceGeneration + 1, StoreUUID: current.StoreUUID, OperationID: operationID,
		OperationType: storeformat.MaintenanceMappingGC, Phase: 1, SourceFiles: sourceRefs, OldManifestGeneration: current.Generation,
	}
	if err := installMaintenanceJournal(m.store.root, journal); err != nil {
		return storeformat.Manifest{}, err
	}
	if err := failpoint.Hit(m.store.hook, PointMappingGCPrepared); err != nil {
		return storeformat.Manifest{}, err
	}
	writer, err := newMapGCWriter(m.store.root, m.store.uuid, m.store.segmentSize, current.NextMapSegmentID, firstNodeSeq, operationID)
	if err != nil {
		_ = removeMaintenanceJournal(m.store.root)
		return storeformat.Manifest{}, err
	}
	if err := failpoint.Hit(m.store.hook, PointMappingGCCopying); err != nil {
		if cleanupErr := writer.cleanup(); cleanupErr == nil {
			_ = removeMaintenanceJournal(m.store.root)
		}
		return storeformat.Manifest{}, err
	}
	installedManifest := false
	defer func() {
		if !installedManifest {
			if cleanupErr := writer.cleanup(); cleanupErr == nil {
				_ = removeMaintenanceJournal(m.store.root)
			}
		}
	}()
	newRoot := base.MapAddr(0)
	if oldRoot != 0 {
		newRoot, err = m.copyMappingTree(ctx, writer, oldRoot, 7, 0, covered)
		if err != nil {
			return storeformat.Manifest{}, err
		}
	}
	if err := writer.finish(); err != nil {
		return storeformat.Manifest{}, err
	}
	journal.Phase, journal.DestinationFiles = 2, writer.temporaryRefs()
	if err := installMaintenanceJournal(m.store.root, journal); err != nil {
		return storeformat.Manifest{}, err
	}
	if err := failpoint.Hit(m.store.hook, PointMappingGCCopied); err != nil {
		return storeformat.Manifest{}, err
	}
	if err := writer.publishFiles(); err != nil {
		return storeformat.Manifest{}, err
	}
	journal.Phase, journal.DestinationFiles = 3, writer.finalRefs()
	if err := installMaintenanceJournal(m.store.root, journal); err != nil {
		return storeformat.Manifest{}, err
	}
	if err := failpoint.Hit(m.store.hook, PointMappingGCFilesDurable); err != nil {
		return storeformat.Manifest{}, err
	}
	installed, err := m.store.catalog.Install(0, func(next *storeformat.Manifest) error {
		if next.ActiveMapSegmentID != current.ActiveMapSegmentID || !sameFileSummaries(next.SealedMappingSegments, current.SealedMappingSegments) || next.MappingRoot != oldRoot || next.CoveredCommitSeq != covered {
			return base.ErrConflict
		}
		next.MappingRoot = newRoot
		next.SealedMappingSegments = writer.sealedSummaries()
		next.ActiveMapSegmentID = writer.currentID
		next.NextMapSegmentID = writer.nextID
		next.MaintenanceGeneration = journal.Generation
		return nil
	})
	if err != nil {
		return storeformat.Manifest{}, err
	}
	installedManifest = true
	journal.Phase, journal.NewManifestGeneration = 4, installed.Generation
	if err := installMaintenanceJournal(m.store.root, journal); err != nil {
		return storeformat.Manifest{}, err
	}
	if err := failpoint.Hit(m.store.hook, PointMappingGCManifestInstalled); err != nil {
		return storeformat.Manifest{}, err
	}
	if err := m.installCompactedRoot(oldRoot, covered, newRoot, installed); err != nil {
		return storeformat.Manifest{}, err
	}
	if err := failpoint.Hit(m.store.hook, PointMappingGCRuntimeInstalled); err != nil {
		return storeformat.Manifest{}, err
	}
	releasePin()
	m.waitRootReaders(oldRoot)
	trash, err := m.store.trashRetired(sourceRefs, operationID)
	if err != nil {
		return storeformat.Manifest{}, err
	}
	journal.Phase = 5
	if err := installMaintenanceJournal(m.store.root, journal); err != nil {
		return storeformat.Manifest{}, err
	}
	if err := failpoint.Hit(m.store.hook, PointMappingGCTrashed); err != nil {
		return storeformat.Manifest{}, err
	}
	if err := deleteMappingTrash(m.store.root, trash); err != nil {
		return storeformat.Manifest{}, err
	}
	journal.Phase = 6
	if err := installMaintenanceJournal(m.store.root, journal); err != nil {
		return storeformat.Manifest{}, err
	}
	if err := removeMaintenanceJournal(m.store.root); err != nil {
		return storeformat.Manifest{}, err
	}
	return installed, nil
}

func (m *Mapping) copyMappingTree(ctx context.Context, writer *mapGCWriter, addr base.MapAddr, level uint8, prefix uint64, covered base.CommitSeq) (base.MapAddr, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	node, err := m.loadNode(addr, level, prefix, covered)
	if err != nil {
		return 0, err
	}
	var slots [storeformat.MappingNodeSlots]uint64
	for slot := uint16(0); slot < storeformat.MappingNodeSlots; slot++ {
		value, ok := node.Lookup(slot)
		if !ok {
			continue
		}
		if level == 0 {
			slots[slot] = value
			continue
		}
		childPrefix := uint64(slot)
		if level != 7 {
			childPrefix = (prefix << 9) | uint64(slot)
		}
		child, err := m.copyMappingTree(ctx, writer, base.MapAddr(value), level-1, childPrefix, covered)
		if err != nil {
			return 0, err
		}
		slots[slot] = uint64(child)
	}
	return writer.append(level, prefix, covered, slots)
}

func mappingGCTempPath(root string, operationID [16]byte, id base.MapSegmentID) string {
	return filepath.Join(root, "mapping", fmt.Sprintf(".MAP-GC-%x-%08d.tmp", operationID, id))
}

func mappingGCTrashPath(root string, operationID [16]byte, id base.MapSegmentID) string {
	return filepath.Join(root, "trash", fmt.Sprintf("MAP-%08d.%x.trash", id, operationID))
}

func sameFileSummaries(a, b []storeformat.FileSummary) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func sortJournalRefs(refs []storeformat.JournalFileRef) {
	sort.Slice(refs, func(i, j int) bool {
		if refs[i].Kind != refs[j].Kind {
			return refs[i].Kind < refs[j].Kind
		}
		if refs[i].FileID != refs[j].FileID {
			return refs[i].FileID < refs[j].FileID
		}
		return refs[i].State < refs[j].State
	})
}

func deleteMappingTrash(root string, paths []string) error {
	for _, path := range paths {
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	return syncDirectory(filepath.Join(root, "trash"))
}

func recoverMappingGC(root string, current storeformat.Manifest, journal storeformat.MaintenanceJournal) (storeformat.Manifest, error) {
	if journal.StoreUUID != current.StoreUUID || journal.OperationType != storeformat.MaintenanceMappingGC {
		return storeformat.Manifest{}, base.ErrCorrupt
	}
	if current.MaintenanceGeneration < journal.Generation {
		pattern := filepath.Join(root, "mapping", fmt.Sprintf(".MAP-GC-%x-*.tmp", journal.OperationID))
		temps, err := filepath.Glob(pattern)
		if err != nil {
			return storeformat.Manifest{}, err
		}
		for _, path := range temps {
			if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
				return storeformat.Manifest{}, err
			}
		}
		for _, ref := range journal.DestinationFiles {
			id := base.MapSegmentID(ref.FileID)
			paths := []string{
				mappingGCTempPath(root, journal.OperationID, id),
				filepath.Join(root, "mapping", activeMapFileName(id)),
				filepath.Join(root, "mapping", sealedMapFileName(id)),
			}
			for _, path := range paths {
				if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
					return storeformat.Manifest{}, err
				}
			}
		}
		if err := syncDirectory(filepath.Join(root, "mapping")); err != nil {
			return storeformat.Manifest{}, err
		}
		if err := removeMaintenanceJournal(root); err != nil {
			return storeformat.Manifest{}, err
		}
		return current, nil
	}
	if current.MaintenanceGeneration != journal.Generation || current.Generation <= journal.OldManifestGeneration {
		return storeformat.Manifest{}, base.ErrCorrupt
	}
	validated, err := Open(root, current, 64<<10)
	if err != nil {
		return storeformat.Manifest{}, err
	}
	walkErr := validated.WalkRoot(current.MappingRoot, current.CoveredCommitSeq, func(base.ID, base.VAddr) error { return nil })
	if err := errors.Join(walkErr, validated.Close()); err != nil {
		return storeformat.Manifest{}, err
	}
	currentIDs := map[base.MapSegmentID]struct{}{current.ActiveMapSegmentID: {}}
	for _, summary := range current.SealedMappingSegments {
		currentIDs[base.MapSegmentID(summary.FileID)] = struct{}{}
	}
	trash := make([]string, 0, len(journal.SourceFiles))
	for _, ref := range journal.SourceFiles {
		id := base.MapSegmentID(ref.FileID)
		if _, retained := currentIDs[id]; retained {
			return storeformat.Manifest{}, base.ErrCorrupt
		}
		target := mappingGCTrashPath(root, journal.OperationID, id)
		if _, err := os.Stat(target); errors.Is(err, os.ErrNotExist) {
			source := filepath.Join(root, "mapping", activeMapFileName(id))
			if ref.State == storeformat.FileStateSealed {
				source = filepath.Join(root, "mapping", sealedMapFileName(id))
			}
			if err := os.Rename(source, target); err != nil && !errors.Is(err, os.ErrNotExist) {
				return storeformat.Manifest{}, err
			}
		} else if err != nil {
			return storeformat.Manifest{}, err
		}
		trash = append(trash, target)
	}
	if err := syncDirectory(filepath.Join(root, "mapping")); err != nil {
		return storeformat.Manifest{}, err
	}
	if err := syncDirectory(filepath.Join(root, "trash")); err != nil {
		return storeformat.Manifest{}, err
	}
	if err := deleteMappingTrash(root, trash); err != nil {
		return storeformat.Manifest{}, err
	}
	if err := removeMaintenanceJournal(root); err != nil {
		return storeformat.Manifest{}, err
	}
	return current, nil
}

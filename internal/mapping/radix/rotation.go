package radix

import (
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
	"github.com/akzj/ridstore/internal/maintenance"
	"github.com/akzj/ridstore/internal/manifest"
)

const (
	PointRotationPrepared          failpoint.Point = "mapping-rotation.prepared"
	PointRotationOldSealed         failpoint.Point = "mapping-rotation.old-sealed"
	PointRotationNewCreated        failpoint.Point = "mapping-rotation.new-created"
	PointRotationManifestInstalled failpoint.Point = "mapping-rotation.manifest-installed"
	PointBeforeRotationActiveSync  failpoint.Point = "mapping-rotation.before-active-sync"
	PointBeforeRotationFooterWrite failpoint.Point = "mapping-rotation.before-footer-write"
	PointBeforeRotationFooterSync  failpoint.Point = "mapping-rotation.before-footer-sync"
	PointBeforeRotationRename      failpoint.Point = "mapping-rotation.before-rename"
	PointBeforeRotationDirSync     failpoint.Point = "mapping-rotation.before-dir-sync"
	PointBeforeRotationHeaderWrite failpoint.Point = "mapping-rotation.before-header-write"
	PointBeforeRotationHeaderSync  failpoint.Point = "mapping-rotation.before-header-sync"
	PointBeforeRotationCreateSync  failpoint.Point = "mapping-rotation.before-create-dir-sync"
	PointBeforeRotationTruncate    failpoint.Point = "mapping-rotation.before-recovery-truncate"
	PointBeforeRotationRemove      failpoint.Point = "mapping-rotation.before-recovery-remove"
)

func (s *nodeStore) rotateLocked() error {
	current := s.catalog.Snapshot()
	if current.ActiveMapSegmentID != s.activeID || current.NextMapSegmentID <= s.activeID || s.activeCount == 0 {
		return fmt.Errorf("mapping rotation catalog mismatch: %w", base.ErrConflict)
	}
	if parent, found, err := loadMaintenanceJournal(s.root); err != nil {
		return err
	} else if found {
		if parent.OperationType != storeformat.MaintenanceDataGC || parent.Phase != 3 || parent.StoreUUID != current.StoreUUID ||
			(parent.Generation != current.MaintenanceGeneration && (current.MaintenanceGeneration == ^uint64(0) || parent.Generation != current.MaintenanceGeneration+1)) {
			return fmt.Errorf("mapping rotation conflicts with maintenance operation: %w", base.ErrConflict)
		}
		return s.rotateNestedDataGCLocked(current, parent)
	}
	newID := current.NextMapSegmentID
	if newID == base.MapSegmentID(^uint32(0)) {
		return base.ErrGenerationExhausted
	}
	var operationID [16]byte
	if _, err := rand.Read(operationID[:]); err != nil {
		return err
	}
	generation := current.MaintenanceGeneration + 1
	if generation == 0 {
		return base.ErrGenerationExhausted
	}
	summary := storeformat.FileSummary{FileID: uint32(s.activeID), ValidEnd: s.activeEnd, FirstSeq: uint64(s.activeFirst), LastSeq: uint64(s.activeLast)}
	journal := storeformat.MaintenanceJournal{
		Generation: generation, StoreUUID: s.uuid, OperationID: operationID,
		OperationType: storeformat.MaintenanceMappingCheckpoint, Phase: 1,
		SourceFiles:           []storeformat.JournalFileRef{{Kind: storeformat.FileKindMapping, State: storeformat.FileStateActive, FileID: uint32(s.activeID), ValidEnd: s.activeEnd, FirstSeq: uint64(s.activeFirst), LastSeq: uint64(s.activeLast)}},
		DestinationFiles:      []storeformat.JournalFileRef{{Kind: storeformat.FileKindMapping, State: storeformat.FileStateTemporary, FileID: uint32(newID), ValidEnd: storeformat.SegmentHeaderSize, FirstSeq: uint64(s.nextNodeSeq), LastSeq: uint64(s.nextNodeSeq)}},
		OldManifestGeneration: current.Generation,
	}
	if err := failpoint.Hit(s.hook, PointBeforeRotationActiveSync); err != nil {
		return err
	}
	if err := s.active.Sync(); err != nil {
		return err
	}
	if err := installMaintenanceJournalWithHook(s.root, journal, s.hook); err != nil {
		return err
	}
	if err := failpoint.Hit(s.hook, PointRotationPrepared); err != nil {
		return err
	}
	footer, err := storeformat.EncodeMappingSegmentFooter(storeformat.MappingSegmentFooter{
		SegmentID: s.activeID, ValidNodeEnd: s.activeEnd, FirstNodeSeq: s.activeFirst, LastNodeSeq: s.activeLast, NodeCount: s.activeCount,
	})
	if err != nil {
		return err
	}
	if err := failpoint.Hit(s.hook, PointBeforeRotationFooterWrite); err != nil {
		return err
	}
	if _, err := writeFullAt(s.active, footer[:], int64(s.activeEnd)); err != nil {
		return err
	}
	if err := failpoint.Hit(s.hook, PointBeforeRotationFooterSync); err != nil {
		return err
	}
	if err := s.active.Sync(); err != nil {
		return err
	}
	activePath := s.active.Name()
	if err := s.active.Close(); err != nil {
		return err
	}
	sealedPath := filepath.Join(s.root, "mapping", sealedMapFileName(s.activeID))
	if err := failpoint.Hit(s.hook, PointBeforeRotationRename); err != nil {
		return err
	}
	if err := os.Rename(activePath, sealedPath); err != nil {
		return err
	}
	if err := failpoint.Hit(s.hook, PointBeforeRotationDirSync); err != nil {
		return err
	}
	if err := syncDirectory(filepath.Join(s.root, "mapping")); err != nil {
		return err
	}
	if err := failpoint.Hit(s.hook, PointRotationOldSealed); err != nil {
		return err
	}
	journal.Phase = 2
	journal.SourceFiles[0].State = storeformat.FileStateSealed
	if err := installMaintenanceJournalWithHook(s.root, journal, s.hook); err != nil {
		return err
	}
	newActive, err := createActiveMapWithHook(s.root, s.uuid, newID, s.nextNodeSeq, s.hook)
	if err != nil {
		return err
	}
	if err := failpoint.Hit(s.hook, PointRotationNewCreated); err != nil {
		return errors.Join(err, newActive.Close())
	}
	journal.Phase = 3
	journal.DestinationFiles[0].State = storeformat.FileStateActive
	if err := installMaintenanceJournalWithHook(s.root, journal, s.hook); err != nil {
		return errors.Join(err, newActive.Close())
	}
	installed, err := s.catalog.Install(0, func(next *storeformat.Manifest) error {
		if next.ActiveMapSegmentID != s.activeID || next.NextMapSegmentID != newID {
			return base.ErrConflict
		}
		next.SealedMappingSegments = append(next.SealedMappingSegments, summary)
		sort.Slice(next.SealedMappingSegments, func(i, j int) bool {
			return next.SealedMappingSegments[i].FileID < next.SealedMappingSegments[j].FileID
		})
		next.ActiveMapSegmentID = newID
		next.NextMapSegmentID = newID + 1
		next.MaintenanceGeneration = generation
		return nil
	})
	if err != nil {
		return errors.Join(err, newActive.Close())
	}
	if err := failpoint.Hit(s.hook, PointRotationManifestInstalled); err != nil {
		return errors.Join(err, newActive.Close())
	}
	journal.Phase = 4
	journal.NewManifestGeneration = installed.Generation
	if err := installMaintenanceJournalWithHook(s.root, journal, s.hook); err != nil {
		return errors.Join(err, newActive.Close())
	}
	sealedFile, err := openMappingFile(s.root, s.uuid, summary, true, s.segmentSize)
	if err != nil {
		return errors.Join(err, newActive.Close())
	}
	s.sealed[s.activeID] = sealedMapFile{file: sealedFile, end: summary.ValidEnd, summary: summary}
	s.activeID, s.active = newID, newActive
	s.activeEnd = storeformat.SegmentHeaderSize
	s.activeFirst, s.activeLast, s.activeCount = s.nextNodeSeq, 0, 0
	journal.Phase = 5
	if err := installMaintenanceJournalWithHook(s.root, journal, s.hook); err != nil {
		return err
	}
	return removeMaintenanceJournalWithHook(s.root, s.hook)
}

// rotateNestedDataGCLocked records Mapping file-set changes in the active
// DataGC journal. The parent remains at RelocationsDurable (phase 3); its
// checkpoint advances the same journal only after the new Root is durable.
func (s *nodeStore) rotateNestedDataGCLocked(current storeformat.Manifest, journal storeformat.MaintenanceJournal) error {
	newID := current.NextMapSegmentID
	if newID == base.MapSegmentID(^uint32(0)) {
		return base.ErrGenerationExhausted
	}
	summary := storeformat.FileSummary{FileID: uint32(s.activeID), ValidEnd: s.activeEnd, FirstSeq: uint64(s.activeFirst), LastSeq: uint64(s.activeLast)}
	oldRef := storeformat.JournalFileRef{Kind: storeformat.FileKindMapping, State: storeformat.FileStateActive, FileID: uint32(s.activeID), ValidEnd: s.activeEnd, FirstSeq: uint64(s.activeFirst), LastSeq: uint64(s.activeLast)}
	newRef := storeformat.JournalFileRef{Kind: storeformat.FileKindMapping, State: storeformat.FileStateTemporary, FileID: uint32(newID), ValidEnd: storeformat.SegmentHeaderSize, FirstSeq: uint64(s.nextNodeSeq), LastSeq: uint64(s.nextNodeSeq)}
	journal.SourceFiles = upsertJournalRef(journal.SourceFiles, oldRef)
	journal.DestinationFiles = upsertJournalRef(journal.DestinationFiles, newRef)
	if err := failpoint.Hit(s.hook, PointBeforeRotationActiveSync); err != nil {
		return err
	}
	if err := s.active.Sync(); err != nil {
		return err
	}
	if err := installMaintenanceJournalWithHook(s.root, journal, s.hook); err != nil {
		return err
	}
	if err := failpoint.Hit(s.hook, PointRotationPrepared); err != nil {
		return err
	}
	footer, err := storeformat.EncodeMappingSegmentFooter(storeformat.MappingSegmentFooter{
		SegmentID: s.activeID, ValidNodeEnd: s.activeEnd, FirstNodeSeq: s.activeFirst, LastNodeSeq: s.activeLast, NodeCount: s.activeCount,
	})
	if err != nil {
		return err
	}
	if err := failpoint.Hit(s.hook, PointBeforeRotationFooterWrite); err != nil {
		return err
	}
	if _, err := writeFullAt(s.active, footer[:], int64(s.activeEnd)); err != nil {
		return err
	}
	if err := failpoint.Hit(s.hook, PointBeforeRotationFooterSync); err != nil {
		return err
	}
	if err := s.active.Sync(); err != nil {
		return err
	}
	activePath := s.active.Name()
	if err := s.active.Close(); err != nil {
		return err
	}
	sealedPath := filepath.Join(s.root, "mapping", sealedMapFileName(s.activeID))
	if err := failpoint.Hit(s.hook, PointBeforeRotationRename); err != nil {
		return err
	}
	if err := os.Rename(activePath, sealedPath); err != nil {
		return err
	}
	if err := failpoint.Hit(s.hook, PointBeforeRotationDirSync); err != nil {
		return err
	}
	if err := syncDirectory(filepath.Join(s.root, "mapping")); err != nil {
		return err
	}
	if err := failpoint.Hit(s.hook, PointRotationOldSealed); err != nil {
		return err
	}
	oldRef.State = storeformat.FileStateSealed
	journal.SourceFiles = upsertJournalRef(journal.SourceFiles, oldRef)
	if err := installMaintenanceJournalWithHook(s.root, journal, s.hook); err != nil {
		return err
	}
	newActive, err := createActiveMapWithHook(s.root, s.uuid, newID, s.nextNodeSeq, s.hook)
	if err != nil {
		return err
	}
	if err := failpoint.Hit(s.hook, PointRotationNewCreated); err != nil {
		return errors.Join(err, newActive.Close())
	}
	newRef.State = storeformat.FileStateActive
	journal.DestinationFiles = upsertJournalRef(journal.DestinationFiles, newRef)
	if err := installMaintenanceJournalWithHook(s.root, journal, s.hook); err != nil {
		return errors.Join(err, newActive.Close())
	}
	_, err = s.catalog.Install(0, func(next *storeformat.Manifest) error {
		if next.ActiveMapSegmentID != s.activeID || next.NextMapSegmentID != newID ||
			(next.MaintenanceGeneration != current.MaintenanceGeneration) {
			return base.ErrConflict
		}
		next.SealedMappingSegments = append(next.SealedMappingSegments, summary)
		sort.Slice(next.SealedMappingSegments, func(i, j int) bool {
			return next.SealedMappingSegments[i].FileID < next.SealedMappingSegments[j].FileID
		})
		next.ActiveMapSegmentID = newID
		next.NextMapSegmentID = newID + 1
		next.MaintenanceGeneration = journal.Generation
		return nil
	})
	if err != nil {
		return errors.Join(err, newActive.Close())
	}
	if err := failpoint.Hit(s.hook, PointRotationManifestInstalled); err != nil {
		return errors.Join(err, newActive.Close())
	}
	sealedFile, err := openMappingFile(s.root, s.uuid, summary, true, s.segmentSize)
	if err != nil {
		return errors.Join(err, newActive.Close())
	}
	s.sealed[s.activeID] = sealedMapFile{file: sealedFile, end: summary.ValidEnd, summary: summary}
	s.activeID, s.active = newID, newActive
	s.activeEnd = storeformat.SegmentHeaderSize
	s.activeFirst, s.activeLast, s.activeCount = s.nextNodeSeq, 0, 0
	return nil
}

func upsertJournalRef(refs []storeformat.JournalFileRef, ref storeformat.JournalFileRef) []storeformat.JournalFileRef {
	result := append([]storeformat.JournalFileRef(nil), refs...)
	for i := range result {
		if result[i].Kind == ref.Kind && result[i].FileID == ref.FileID {
			result[i] = ref
			sort.Slice(result, func(i, j int) bool {
				if result[i].Kind != result[j].Kind {
					return result[i].Kind < result[j].Kind
				}
				return result[i].FileID < result[j].FileID
			})
			return result
		}
	}
	result = append(result, ref)
	sort.Slice(result, func(i, j int) bool {
		if result[i].Kind != result[j].Kind {
			return result[i].Kind < result[j].Kind
		}
		return result[i].FileID < result[j].FileID
	})
	return result
}

func RecoverMappingRotation(root string, current storeformat.Manifest) (storeformat.Manifest, error) {
	return RecoverMappingRotationWithHook(root, current, nil)
}

func RecoverMappingRotationWithHook(root string, current storeformat.Manifest, hook failpoint.Hook) (storeformat.Manifest, error) {
	journal, found, err := loadMaintenanceJournal(root)
	if err != nil || !found {
		return current, err
	}
	if journal.OperationType == storeformat.MaintenanceMappingGC {
		return recoverMappingGCWithHook(root, current, journal, hook)
	}
	if journal.OperationType == storeformat.MaintenanceDataGC {
		return recoverNestedDataGCRotations(root, current, journal, hook)
	}
	if journal.OperationType != storeformat.MaintenanceMappingCheckpoint {
		return current, nil
	}
	if journal.StoreUUID != current.StoreUUID || len(journal.SourceFiles) != 1 || len(journal.DestinationFiles) != 1 {
		return storeformat.Manifest{}, base.ErrCorrupt
	}
	oldRef, newRef := journal.SourceFiles[0], journal.DestinationFiles[0]
	oldID, newID := base.MapSegmentID(oldRef.FileID), base.MapSegmentID(newRef.FileID)
	if newID == base.MapSegmentID(^uint32(0)) {
		return storeformat.Manifest{}, base.ErrGenerationExhausted
	}
	if current.ActiveMapSegmentID == newID && hasMapSummary(current.SealedMappingSegments, oldID) {
		if err := removeMaintenanceJournalWithHook(root, hook); err != nil {
			return storeformat.Manifest{}, err
		}
		return current, nil
	}
	if current.ActiveMapSegmentID != oldID || current.Generation < journal.OldManifestGeneration {
		return storeformat.Manifest{}, base.ErrCorrupt
	}
	summary := storeformat.FileSummary{FileID: uint32(oldID), ValidEnd: oldRef.ValidEnd, FirstSeq: oldRef.FirstSeq, LastSeq: oldRef.LastSeq}
	if err := ensureSealedMapWithHook(root, current, summary, hook); err != nil {
		return storeformat.Manifest{}, err
	}
	if err := ensureEmptyActiveMapWithHook(root, current.StoreUUID, newID, base.NodeSeq(newRef.FirstSeq), hook); err != nil {
		return storeformat.Manifest{}, err
	}
	if current.Generation == ^uint64(0) {
		return storeformat.Manifest{}, base.ErrGenerationExhausted
	}
	next := cloneManifest(current)
	next.Generation++
	next.SealedMappingSegments = append(next.SealedMappingSegments, summary)
	sort.Slice(next.SealedMappingSegments, func(i, j int) bool {
		return next.SealedMappingSegments[i].FileID < next.SealedMappingSegments[j].FileID
	})
	next.ActiveMapSegmentID = newID
	next.NextMapSegmentID = newID + 1
	next.MaintenanceGeneration = journal.Generation
	if err := (manifest.Installer{Dir: root, FailpointHook: hook}).Install(next); err != nil {
		return storeformat.Manifest{}, err
	}
	if err := removeMaintenanceJournalWithHook(root, hook); err != nil {
		return storeformat.Manifest{}, err
	}
	return next, nil
}

func recoverNestedDataGCRotations(root string, current storeformat.Manifest, journal storeformat.MaintenanceJournal, hook failpoint.Hook) (storeformat.Manifest, error) {
	if journal.StoreUUID != current.StoreUUID || journal.Generation < current.MaintenanceGeneration {
		return storeformat.Manifest{}, base.ErrCorrupt
	}
	if journal.Phase < 3 {
		return current, nil
	}
	for _, oldRef := range journal.SourceFiles {
		if oldRef.Kind != storeformat.FileKindMapping {
			continue
		}
		oldID := base.MapSegmentID(oldRef.FileID)
		if oldID == base.MapSegmentID(^uint32(0)) {
			return storeformat.Manifest{}, base.ErrCorrupt
		}
		newID := oldID + 1
		if newID == base.MapSegmentID(^uint32(0)) {
			return storeformat.Manifest{}, base.ErrGenerationExhausted
		}
		newRef, ok := journalMappingRef(journal.DestinationFiles, newID)
		if !ok || newRef.FirstSeq <= oldRef.LastSeq {
			return storeformat.Manifest{}, base.ErrCorrupt
		}
		if hasMapSummary(current.SealedMappingSegments, oldID) {
			if current.ActiveMapSegmentID < newID {
				return storeformat.Manifest{}, base.ErrCorrupt
			}
			continue
		}
		if current.ActiveMapSegmentID != oldID || current.NextMapSegmentID != newID {
			return storeformat.Manifest{}, base.ErrCorrupt
		}
		summary := storeformat.FileSummary{FileID: uint32(oldID), ValidEnd: oldRef.ValidEnd, FirstSeq: oldRef.FirstSeq, LastSeq: oldRef.LastSeq}
		if err := ensureSealedMapWithHook(root, current, summary, hook); err != nil {
			return storeformat.Manifest{}, err
		}
		if err := ensureEmptyActiveMapWithHook(root, current.StoreUUID, newID, base.NodeSeq(newRef.FirstSeq), hook); err != nil {
			return storeformat.Manifest{}, err
		}
		if current.Generation == ^uint64(0) {
			return storeformat.Manifest{}, base.ErrGenerationExhausted
		}
		next := cloneManifest(current)
		next.Generation++
		next.SealedMappingSegments = append(next.SealedMappingSegments, summary)
		sort.Slice(next.SealedMappingSegments, func(i, j int) bool {
			return next.SealedMappingSegments[i].FileID < next.SealedMappingSegments[j].FileID
		})
		next.ActiveMapSegmentID = newID
		next.NextMapSegmentID = newID + 1
		next.MaintenanceGeneration = journal.Generation
		if err := (manifest.Installer{Dir: root, FailpointHook: hook}).Install(next); err != nil {
			return storeformat.Manifest{}, err
		}
		current = next
	}
	return current, nil
}

func journalMappingRef(refs []storeformat.JournalFileRef, id base.MapSegmentID) (storeformat.JournalFileRef, bool) {
	for _, ref := range refs {
		if ref.Kind == storeformat.FileKindMapping && ref.FileID == uint32(id) {
			return ref, true
		}
	}
	return storeformat.JournalFileRef{}, false
}

func ensureEmptyActiveMap(root string, uuid base.StoreUUID, id base.MapSegmentID, first base.NodeSeq) error {
	return ensureEmptyActiveMapWithHook(root, uuid, id, first, nil)
}

func ensureEmptyActiveMapWithHook(root string, uuid base.StoreUUID, id base.MapSegmentID, first base.NodeSeq, hook failpoint.Hook) error {
	path := filepath.Join(root, "mapping", activeMapFileName(id))
	info, err := os.Lstat(path)
	if err == nil && (!info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0) {
		return fmt.Errorf("nested mapping rotation active path: %w", base.ErrCorrupt)
	}
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if errors.Is(err, os.ErrNotExist) {
		created, createErr := createActiveMapWithHook(root, uuid, id, first, hook)
		if createErr != nil {
			return createErr
		}
		return created.Close()
	}
	if err != nil {
		return err
	}
	info, statErr := file.Stat()
	header, headerErr := readMapHeader(file)
	if statErr == nil && headerErr == nil && info.Size() == storeformat.SegmentHeaderSize && header.Kind == storeformat.SegmentKindMapping && header.StoreUUID == uuid && header.FileID == uint32(id) && header.FirstSeq == uint64(first) {
		if err := failpoint.Hit(hook, PointBeforeRotationHeaderSync); err != nil {
			return errors.Join(err, file.Close())
		}
		if err := file.Sync(); err != nil {
			return errors.Join(err, file.Close())
		}
		if err := file.Close(); err != nil {
			return err
		}
		if err := failpoint.Hit(hook, PointBeforeRotationCreateSync); err != nil {
			return err
		}
		return syncDirectory(filepath.Join(root, "mapping"))
	}
	if closeErr := file.Close(); closeErr != nil {
		return closeErr
	}
	// The destination is not referenced by the current Manifest yet. A crash
	// during header creation may therefore leave a partial regular file; remove
	// only that journal-named file, sync the directory, and recreate it.
	if err := failpoint.Hit(hook, PointBeforeRotationRemove); err != nil {
		return errors.Join(statErr, headerErr, err)
	}
	if err := os.Remove(path); err != nil {
		return errors.Join(statErr, headerErr, err)
	}
	if err := failpoint.Hit(hook, PointBeforeRotationDirSync); err != nil {
		return err
	}
	if err := syncDirectory(filepath.Join(root, "mapping")); err != nil {
		return err
	}
	created, err := createActiveMapWithHook(root, uuid, id, first, hook)
	if err != nil {
		return err
	}
	return created.Close()
}

func ensureSealedMap(root string, current storeformat.Manifest, summary storeformat.FileSummary) error {
	return ensureSealedMapWithHook(root, current, summary, nil)
}

func ensureSealedMapWithHook(root string, current storeformat.Manifest, summary storeformat.FileSummary, hook failpoint.Hook) error {
	sealedPath := filepath.Join(root, "mapping", sealedMapFileName(base.MapSegmentID(summary.FileID)))
	if _, err := os.Stat(sealedPath); err == nil {
		file, err := openMappingFile(root, current.StoreUUID, summary, true, current.HardLimits.SegmentSize)
		if err != nil {
			return err
		}
		if err := failpoint.Hit(hook, PointBeforeRotationFooterSync); err != nil {
			return errors.Join(err, file.Close())
		}
		if err := file.Sync(); err != nil {
			return errors.Join(err, file.Close())
		}
		if err := file.Close(); err != nil {
			return err
		}
		if err := failpoint.Hit(hook, PointBeforeRotationDirSync); err != nil {
			return err
		}
		return syncDirectory(filepath.Join(root, "mapping"))
	}
	activePath := filepath.Join(root, "mapping", activeMapFileName(base.MapSegmentID(summary.FileID)))
	file, err := os.OpenFile(activePath, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	_, _, count, err := scanNodes(file, summary.ValidEnd, summary.ValidEnd, base.NodeSeq(summary.FirstSeq), true)
	if err != nil {
		return errors.Join(err, file.Close())
	}
	if err := failpoint.Hit(hook, PointBeforeRotationTruncate); err != nil {
		return errors.Join(err, file.Close())
	}
	if err := file.Truncate(int64(summary.ValidEnd)); err != nil {
		return errors.Join(err, file.Close())
	}
	footer, err := storeformat.EncodeMappingSegmentFooter(storeformat.MappingSegmentFooter{
		SegmentID: base.MapSegmentID(summary.FileID), ValidNodeEnd: summary.ValidEnd,
		FirstNodeSeq: base.NodeSeq(summary.FirstSeq), LastNodeSeq: base.NodeSeq(summary.LastSeq), NodeCount: count,
	})
	if err != nil {
		return errors.Join(err, file.Close())
	}
	if err := failpoint.Hit(hook, PointBeforeRotationFooterWrite); err != nil {
		return errors.Join(err, file.Close())
	}
	if _, err := writeFullAt(file, footer[:], int64(summary.ValidEnd)); err != nil {
		return errors.Join(err, file.Close())
	}
	if err := failpoint.Hit(hook, PointBeforeRotationFooterSync); err != nil {
		return errors.Join(err, file.Close())
	}
	if err := file.Sync(); err != nil {
		return errors.Join(err, file.Close())
	}
	if err := file.Close(); err != nil {
		return err
	}
	if err := failpoint.Hit(hook, PointBeforeRotationRename); err != nil {
		return err
	}
	if err := os.Rename(activePath, sealedPath); err != nil {
		return err
	}
	if err := failpoint.Hit(hook, PointBeforeRotationDirSync); err != nil {
		return err
	}
	return syncDirectory(filepath.Join(root, "mapping"))
}

func createActiveMap(root string, uuid base.StoreUUID, id base.MapSegmentID, first base.NodeSeq) (*os.File, error) {
	return createActiveMapWithHook(root, uuid, id, first, nil)
}

func createActiveMapWithHook(root string, uuid base.StoreUUID, id base.MapSegmentID, first base.NodeSeq, hook failpoint.Hook) (*os.File, error) {
	header, err := storeformat.EncodeSegmentHeader(storeformat.SegmentHeader{
		Kind: storeformat.SegmentKindMapping, StoreUUID: uuid, FileID: uint32(id), FirstSeq: uint64(first), CreatedUnixNano: uint64(time.Now().UnixNano()),
	})
	if err != nil {
		return nil, err
	}
	path := filepath.Join(root, "mapping", activeMapFileName(id))
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return nil, err
	}
	if err := failpoint.Hit(hook, PointBeforeRotationHeaderWrite); err != nil {
		return nil, errors.Join(err, file.Close())
	}
	if _, err := writeFullAt(file, header[:], 0); err != nil {
		return nil, errors.Join(err, file.Close())
	}
	if err := failpoint.Hit(hook, PointBeforeRotationHeaderSync); err != nil {
		return nil, errors.Join(err, file.Close())
	}
	if err := file.Sync(); err != nil {
		return nil, errors.Join(err, file.Close())
	}
	if err := failpoint.Hit(hook, PointBeforeRotationCreateSync); err != nil {
		return nil, errors.Join(err, file.Close())
	}
	if err := syncDirectory(filepath.Join(root, "mapping")); err != nil {
		return nil, errors.Join(err, file.Close())
	}
	return file, nil
}

func installMaintenanceJournal(root string, journal storeformat.MaintenanceJournal) error {
	return maintenance.Install(root, journal)
}

func installMaintenanceJournalWithHook(root string, journal storeformat.MaintenanceJournal, hook failpoint.Hook) error {
	return maintenance.InstallWithHook(root, journal, hook)
}

func loadMaintenanceJournal(root string) (storeformat.MaintenanceJournal, bool, error) {
	return maintenance.Load(root)
}

func removeMaintenanceJournal(root string) error {
	return maintenance.Remove(root)
}

func removeMaintenanceJournalWithHook(root string, hook failpoint.Hook) error {
	return maintenance.RemoveWithHook(root, hook)
}

func syncDirectory(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	return errors.Join(dir.Sync(), dir.Close())
}

func hasMapSummary(items []storeformat.FileSummary, id base.MapSegmentID) bool {
	for _, item := range items {
		if item.FileID == uint32(id) {
			return true
		}
	}
	return false
}

func cloneManifest(value storeformat.Manifest) storeformat.Manifest {
	value.SealedDataSegments = append([]storeformat.FileSummary(nil), value.SealedDataSegments...)
	value.SealedMappingSegments = append([]storeformat.FileSummary(nil), value.SealedMappingSegments...)
	value.OpenBatchIDsAtCut = append([]base.BatchID(nil), value.OpenBatchIDsAtCut...)
	value.SegmentStats = append([]storeformat.SegmentStatsEntry(nil), value.SegmentStats...)
	return value
}

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
	"github.com/akzj/ridstore/internal/manifest"
)

const maintenanceJournalName = "MAINTENANCE"

const (
	PointRotationPrepared          failpoint.Point = "mapping-rotation.prepared"
	PointRotationOldSealed         failpoint.Point = "mapping-rotation.old-sealed"
	PointRotationNewCreated        failpoint.Point = "mapping-rotation.new-created"
	PointRotationManifestInstalled failpoint.Point = "mapping-rotation.manifest-installed"
)

func (s *nodeStore) rotateLocked() error {
	current := s.catalog.Snapshot()
	if current.ActiveMapSegmentID != s.activeID || current.NextMapSegmentID <= s.activeID || s.activeCount == 0 {
		return fmt.Errorf("mapping rotation catalog mismatch: %w", base.ErrConflict)
	}
	newID := current.NextMapSegmentID
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
	if err := s.active.Sync(); err != nil {
		return err
	}
	if err := installMaintenanceJournal(s.root, journal); err != nil {
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
	if _, err := writeFullAt(s.active, footer[:], int64(s.activeEnd)); err != nil {
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
	if err := os.Rename(activePath, sealedPath); err != nil {
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
	if err := installMaintenanceJournal(s.root, journal); err != nil {
		return err
	}
	newActive, err := createActiveMap(s.root, s.uuid, newID, s.nextNodeSeq)
	if err != nil {
		return err
	}
	if err := failpoint.Hit(s.hook, PointRotationNewCreated); err != nil {
		return errors.Join(err, newActive.Close())
	}
	journal.Phase = 3
	journal.DestinationFiles[0].State = storeformat.FileStateActive
	if err := installMaintenanceJournal(s.root, journal); err != nil {
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
	if err := installMaintenanceJournal(s.root, journal); err != nil {
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
	if err := installMaintenanceJournal(s.root, journal); err != nil {
		return err
	}
	return removeMaintenanceJournal(s.root)
}

func RecoverMappingRotation(root string, current storeformat.Manifest) (storeformat.Manifest, error) {
	journal, found, err := loadMaintenanceJournal(root)
	if err != nil || !found || journal.OperationType != storeformat.MaintenanceMappingCheckpoint {
		return current, err
	}
	if journal.StoreUUID != current.StoreUUID || len(journal.SourceFiles) != 1 || len(journal.DestinationFiles) != 1 {
		return storeformat.Manifest{}, base.ErrCorrupt
	}
	oldRef, newRef := journal.SourceFiles[0], journal.DestinationFiles[0]
	oldID, newID := base.MapSegmentID(oldRef.FileID), base.MapSegmentID(newRef.FileID)
	if current.ActiveMapSegmentID == newID && hasMapSummary(current.SealedMappingSegments, oldID) {
		if err := removeMaintenanceJournal(root); err != nil {
			return storeformat.Manifest{}, err
		}
		return current, nil
	}
	if current.ActiveMapSegmentID != oldID || current.Generation < journal.OldManifestGeneration {
		return storeformat.Manifest{}, base.ErrCorrupt
	}
	summary := storeformat.FileSummary{FileID: uint32(oldID), ValidEnd: oldRef.ValidEnd, FirstSeq: oldRef.FirstSeq, LastSeq: oldRef.LastSeq}
	if err := ensureSealedMap(root, current, summary); err != nil {
		return storeformat.Manifest{}, err
	}
	newPath := filepath.Join(root, "mapping", activeMapFileName(newID))
	if _, err := os.Stat(newPath); errors.Is(err, os.ErrNotExist) {
		file, err := createActiveMap(root, current.StoreUUID, newID, base.NodeSeq(newRef.FirstSeq))
		if err != nil {
			return storeformat.Manifest{}, err
		}
		if err := file.Close(); err != nil {
			return storeformat.Manifest{}, err
		}
	} else if err != nil {
		return storeformat.Manifest{}, err
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
	if err := (manifest.Installer{Dir: root}).Install(next); err != nil {
		return storeformat.Manifest{}, err
	}
	if err := removeMaintenanceJournal(root); err != nil {
		return storeformat.Manifest{}, err
	}
	return next, nil
}

func ensureSealedMap(root string, current storeformat.Manifest, summary storeformat.FileSummary) error {
	sealedPath := filepath.Join(root, "mapping", sealedMapFileName(base.MapSegmentID(summary.FileID)))
	if _, err := os.Stat(sealedPath); err == nil {
		file, err := openMappingFile(root, current.StoreUUID, summary, true, current.HardLimits.SegmentSize)
		if err != nil {
			return err
		}
		return file.Close()
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
	if _, err := writeFullAt(file, footer[:], int64(summary.ValidEnd)); err != nil {
		return errors.Join(err, file.Close())
	}
	if err := file.Sync(); err != nil {
		return errors.Join(err, file.Close())
	}
	if err := file.Close(); err != nil {
		return err
	}
	if err := os.Rename(activePath, sealedPath); err != nil {
		return err
	}
	return syncDirectory(filepath.Join(root, "mapping"))
}

func createActiveMap(root string, uuid base.StoreUUID, id base.MapSegmentID, first base.NodeSeq) (*os.File, error) {
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
	if _, err := writeFullAt(file, header[:], 0); err != nil {
		return nil, errors.Join(err, file.Close())
	}
	if err := file.Sync(); err != nil {
		return nil, errors.Join(err, file.Close())
	}
	if err := syncDirectory(filepath.Join(root, "mapping")); err != nil {
		return nil, errors.Join(err, file.Close())
	}
	return file, nil
}

func installMaintenanceJournal(root string, journal storeformat.MaintenanceJournal) error {
	encoded, err := storeformat.EncodeMaintenanceJournal(journal)
	if err != nil {
		return err
	}
	dir := filepath.Join(root, "journal")
	temp, final := filepath.Join(dir, ".MAINTENANCE.tmp"), filepath.Join(dir, maintenanceJournalName)
	if err := os.Remove(temp); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	file, err := os.OpenFile(temp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	if _, err := file.Write(encoded); err != nil {
		return errors.Join(err, file.Close())
	}
	if err := file.Sync(); err != nil {
		return errors.Join(err, file.Close())
	}
	if err := file.Close(); err != nil {
		return err
	}
	if err := os.Rename(temp, final); err != nil {
		return err
	}
	return syncDirectory(dir)
}

func loadMaintenanceJournal(root string) (storeformat.MaintenanceJournal, bool, error) {
	data, err := os.ReadFile(filepath.Join(root, "journal", maintenanceJournalName))
	if errors.Is(err, os.ErrNotExist) {
		return storeformat.MaintenanceJournal{}, false, nil
	}
	if err != nil {
		return storeformat.MaintenanceJournal{}, false, err
	}
	journal, err := storeformat.DecodeMaintenanceJournal(data)
	return journal, err == nil, err
}

func removeMaintenanceJournal(root string) error {
	dir := filepath.Join(root, "journal")
	if err := os.Remove(filepath.Join(dir, maintenanceJournalName)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := os.Remove(filepath.Join(dir, ".MAINTENANCE.tmp")); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return syncDirectory(dir)
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

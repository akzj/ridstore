package rotation

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"github.com/akzj/ridstore/internal/base"
	"github.com/akzj/ridstore/internal/catalog"
	"github.com/akzj/ridstore/internal/failpoint"
	storeformat "github.com/akzj/ridstore/internal/format"
	"github.com/akzj/ridstore/internal/manifest"
	"github.com/akzj/ridstore/internal/segment"
)

const journalName = "ROTATION"

type Manager struct {
	mu         sync.Mutex
	root       string
	catalog    *catalog.Manager
	registry   *segment.Registry
	maxPayload uint64
	hook       failpoint.Hook
}

const (
	PointPrepared                   failpoint.Point = "rotation.prepared"
	PointOldSealed                  failpoint.Point = "rotation.old-sealed"
	PointNewCreated                 failpoint.Point = "rotation.new-created"
	PointManifestInstalled          failpoint.Point = "rotation.manifest-installed"
	PointBeforeJournalWrite         failpoint.Point = "rotation.before-journal-write"
	PointBeforeJournalSync          failpoint.Point = "rotation.before-journal-sync"
	PointBeforeJournalRename        failpoint.Point = "rotation.before-journal-rename"
	PointBeforeJournalDirSync       failpoint.Point = "rotation.before-journal-dir-sync"
	PointBeforeJournalTempRemove    failpoint.Point = "rotation.before-journal-temp-remove"
	PointBeforeJournalRemove        failpoint.Point = "rotation.before-journal-remove"
	PointBeforeJournalRemoveTemp    failpoint.Point = "rotation.before-journal-remove-temp"
	PointBeforeJournalRemoveDirSync failpoint.Point = "rotation.before-journal-remove-dir-sync"
	PointBeforeNewActiveRemove      failpoint.Point = "rotation.before-new-active-remove"
	PointBeforeNewActiveRemoveSync  failpoint.Point = "rotation.before-new-active-remove-dir-sync"
)

func NewManager(root string, catalog *catalog.Manager, registry *segment.Registry, maxPayload uint64, hook failpoint.Hook) (*Manager, error) {
	if root == "" || catalog == nil || registry == nil || maxPayload == 0 {
		return nil, base.ErrInvalidConfig
	}
	return &Manager{root: root, catalog: catalog, registry: registry, maxPayload: maxPayload, hook: hook}, nil
}

// Rotate runs under the append sequencer. No frame can be allocated between
// sealing old and publishing the new Active Segment.
func (m *Manager) Rotate(active *segment.ActiveData, nextFrameSeq base.FrameSeq) (*segment.ActiveData, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	current := m.catalog.Snapshot()
	if active == nil || active.SegmentID() != current.ActiveDataSegmentID || nextFrameSeq == 0 {
		return nil, base.ErrInvalidConfig
	}
	oldID, newID := active.SegmentID(), current.NextDataSegmentID
	if newID <= oldID || newID == base.DataSegmentID(^uint32(0)) {
		return nil, base.ErrGenerationExhausted
	}
	journal := storeformat.RotationJournal{
		StoreUUID: current.StoreUUID, OldSegmentID: oldID, NewSegmentID: newID,
		BaseManifestGeneration: current.Generation, Phase: 1,
	}
	if err := installJournal(m.root, journal, m.hook); err != nil {
		return nil, err
	}
	if err := failpoint.Hit(m.hook, PointPrepared); err != nil {
		return nil, err
	}
	summary, err := active.Seal(nextFrameSeq)
	if err != nil {
		return nil, err
	}
	if err := failpoint.Hit(m.hook, PointOldSealed); err != nil {
		return nil, err
	}
	journal.Phase = 2
	if err := installJournal(m.root, journal, m.hook); err != nil {
		return nil, err
	}
	journal.Phase = 3
	if err := installJournal(m.root, journal, m.hook); err != nil {
		return nil, err
	}
	newActive, err := segment.CreateActiveDataWithHook(m.root, current.StoreUUID, newID, nextFrameSeq+1, current.HardLimits.SegmentSize, m.maxPayload, m.hook)
	if err != nil {
		return nil, err
	}
	newActive.SetHook(m.hook)
	if err := failpoint.Hit(m.hook, PointNewCreated); err != nil {
		return nil, errors.Join(err, newActive.Close())
	}
	journal.Phase = 4
	if err := installJournal(m.root, journal, m.hook); err != nil {
		return nil, errors.Join(err, newActive.Close())
	}
	sealed, err := segment.OpenSealedData(m.root, current.StoreUUID, summary, current.HardLimits.SegmentSize, m.maxPayload)
	if err != nil {
		return nil, errors.Join(err, newActive.Close())
	}
	nextManifest, err := m.catalog.Install(0, func(next *storeformat.Manifest) error {
		if next.ActiveDataSegmentID != oldID || next.NextDataSegmentID != newID {
			return fmt.Errorf("data file set changed during rotation: %w", base.ErrConflict)
		}
		next.ActiveDataSegmentID = newID
		next.NextDataSegmentID = newID + 1
		next.NextFrameSeq = nextFrameSeq + 1
		next.SealedDataSegments = append(next.SealedDataSegments, summary)
		sort.Slice(next.SealedDataSegments, func(i, j int) bool { return next.SealedDataSegments[i].FileID < next.SealedDataSegments[j].FileID })
		return nil
	})
	if err != nil {
		return nil, errors.Join(err, sealed.Close(), newActive.Close())
	}
	if err := failpoint.Hit(m.hook, PointManifestInstalled); err != nil {
		return nil, errors.Join(err, sealed.Close(), newActive.Close())
	}
	journal.Phase = 5
	journal.InstalledManifestGeneration = nextManifest.Generation
	if err := installJournal(m.root, journal, m.hook); err != nil {
		return nil, errors.Join(err, sealed.Close(), newActive.Close())
	}
	if err := m.registry.ReplaceActive(oldID, sealed, newActive); err != nil {
		return nil, errors.Join(err, sealed.Close(), newActive.Close())
	}
	if err := removeJournal(m.root, m.hook); err != nil {
		return nil, err
	}
	return newActive, nil
}

func (m *Manager) Current() storeformat.Manifest {
	return m.catalog.Snapshot()
}

// Recover completes an interrupted rotation before normal segment opening.
func Recover(root string, current storeformat.Manifest, maxPayload uint64) (storeformat.Manifest, error) {
	return RecoverWithHook(root, current, maxPayload, nil)
}

func RecoverWithHook(root string, current storeformat.Manifest, maxPayload uint64, hook failpoint.Hook) (storeformat.Manifest, error) {
	journal, found, err := loadJournal(root)
	if err != nil {
		return current, err
	}
	if !found {
		return current, cleanupOrphanJournalTemp(root)
	}
	if journal.StoreUUID != current.StoreUUID {
		return storeformat.Manifest{}, fmt.Errorf("rotation StoreUUID mismatch: %w", base.ErrCorrupt)
	}
	if current.ActiveDataSegmentID == journal.NewSegmentID && containsSummary(current.SealedDataSegments, journal.OldSegmentID) {
		if err := removeJournal(root, hook); err != nil {
			return storeformat.Manifest{}, err
		}
		return current, nil
	}
	if current.Generation < journal.BaseManifestGeneration || current.ActiveDataSegmentID != journal.OldSegmentID {
		return storeformat.Manifest{}, fmt.Errorf("rotation base manifest mismatch: %w", base.ErrCorrupt)
	}
	oldSummary, err := ensureOldSealed(root, current, journal, maxPayload, hook)
	if err != nil {
		return storeformat.Manifest{}, err
	}
	nextFrameSeq := base.FrameSeq(oldSummary.LastSeq + 1)
	if err := ensureNewActive(root, current, journal.NewSegmentID, nextFrameSeq, maxPayload, hook); err != nil {
		return storeformat.Manifest{}, err
	}
	nextManifest, err := rotatedManifest(current, oldSummary, journal.NewSegmentID, nextFrameSeq)
	if err != nil {
		return storeformat.Manifest{}, err
	}
	if err := (manifest.Installer{Dir: root, FailpointHook: hook}).Install(nextManifest); err != nil {
		return storeformat.Manifest{}, err
	}
	if err := removeJournal(root, hook); err != nil {
		return storeformat.Manifest{}, err
	}
	return nextManifest, nil
}

func ensureOldSealed(root string, current storeformat.Manifest, journal storeformat.RotationJournal, maxPayload uint64, hook failpoint.Hook) (storeformat.FileSummary, error) {
	sealedPath := filepath.Join(root, "data", segment.SealedDataFileName(journal.OldSegmentID))
	if _, err := os.Lstat(sealedPath); err == nil {
		summary, err := segment.LoadSealedDataSummary(root, journal.OldSegmentID)
		if err != nil {
			return storeformat.FileSummary{}, err
		}
		if err := segment.ValidateSealedData(root, current.StoreUUID, summary, current.HardLimits.SegmentSize, maxPayload); err != nil {
			return storeformat.FileSummary{}, err
		}
		return summary, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return storeformat.FileSummary{}, err
	}
	// ResumeSeal discovers an already-written terminal seal itself. When no
	// seal exists, OpenActiveData truncates only an incomplete tail and uses the
	// manifest watermark as the conservative fallback sequence.
	return segment.ResumeSealWithHook(root, current.StoreUUID, journal.OldSegmentID, current.HardLimits.SegmentSize, maxPayload, current.NextFrameSeq, hook)
}

func ensureNewActive(root string, current storeformat.Manifest, newID base.DataSegmentID, firstSeq base.FrameSeq, maxPayload uint64, hook failpoint.Hook) error {
	path := filepath.Join(root, "data", segment.ActiveDataFileName(newID))
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		active, createErr := segment.CreateActiveDataWithHook(root, current.StoreUUID, newID, firstSeq, current.HardLimits.SegmentSize, maxPayload, hook)
		if createErr != nil {
			return createErr
		}
		return active.Close()
	}
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("unpublished new active is not a regular file: %w", base.ErrCorrupt)
	}
	if info.Size() != storeformat.SegmentHeaderSize {
		if info.Size() > storeformat.SegmentHeaderSize {
			return fmt.Errorf("unpublished new active is not empty: %w", base.ErrCorrupt)
		}
		return recreateNewActive(root, current, newID, firstSeq, maxPayload, hook)
	}
	active, openErr := segment.OpenUnpublishedActiveDataWithHook(root, current.StoreUUID, newID, current.HardLimits.SegmentSize, maxPayload, hook)
	if openErr != nil {
		if errors.Is(openErr, base.ErrCorrupt) {
			return recreateNewActive(root, current, newID, firstSeq, maxPayload, hook)
		}
		return openErr
	}
	if err := active.EnsureCreationDurable(); err != nil {
		return errors.Join(err, active.Close())
	}
	return active.Close()
}

func recreateNewActive(root string, current storeformat.Manifest, newID base.DataSegmentID, firstSeq base.FrameSeq, maxPayload uint64, hook failpoint.Hook) error {
	path := filepath.Join(root, "data", segment.ActiveDataFileName(newID))
	if err := failpoint.Hit(hook, PointBeforeNewActiveRemove); err != nil {
		return err
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := failpoint.Hit(hook, PointBeforeNewActiveRemoveSync); err != nil {
		return err
	}
	if err := syncDir(filepath.Join(root, "data")); err != nil {
		return err
	}
	active, err := segment.CreateActiveDataWithHook(root, current.StoreUUID, newID, firstSeq, current.HardLimits.SegmentSize, maxPayload, hook)
	if err != nil {
		return err
	}
	return active.Close()
}

func rotatedManifest(current storeformat.Manifest, summary storeformat.FileSummary, newID base.DataSegmentID, nextFrameSeq base.FrameSeq) (storeformat.Manifest, error) {
	next := cloneManifest(current)
	next.Generation++
	next.ActiveDataSegmentID = newID
	next.NextDataSegmentID = newID + 1
	next.NextFrameSeq = nextFrameSeq
	next.SealedDataSegments = append(next.SealedDataSegments, summary)
	sort.Slice(next.SealedDataSegments, func(i, j int) bool { return next.SealedDataSegments[i].FileID < next.SealedDataSegments[j].FileID })
	if _, err := storeformat.EncodeManifest(next); err != nil {
		return storeformat.Manifest{}, err
	}
	return next, nil
}

func installJournal(root string, journal storeformat.RotationJournal, hook failpoint.Hook) error {
	encoded, err := storeformat.EncodeRotationJournal(journal)
	if err != nil {
		return err
	}
	dirPath := filepath.Join(root, "journal")
	temp := filepath.Join(dirPath, ".ROTATION.tmp")
	final := filepath.Join(dirPath, journalName)
	if err := failpoint.Hit(hook, PointBeforeJournalTempRemove); err != nil {
		return err
	}
	if err := os.Remove(temp); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	file, err := os.OpenFile(temp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	if err := failpoint.Hit(hook, PointBeforeJournalWrite); err != nil {
		return errors.Join(err, file.Close())
	}
	if n, err := file.Write(encoded); err != nil || n != len(encoded) {
		if err == nil {
			err = io.ErrShortWrite
		}
		return errors.Join(err, file.Close())
	}
	if err := failpoint.Hit(hook, PointBeforeJournalSync); err != nil {
		return errors.Join(err, file.Close())
	}
	if err := file.Sync(); err != nil {
		return errors.Join(err, file.Close())
	}
	if err := file.Close(); err != nil {
		return err
	}
	if err := failpoint.Hit(hook, PointBeforeJournalRename); err != nil {
		return err
	}
	if err := os.Rename(temp, final); err != nil {
		return err
	}
	if err := failpoint.Hit(hook, PointBeforeJournalDirSync); err != nil {
		return err
	}
	return syncDir(dirPath)
}

func cleanupOrphanJournalTemp(root string) error {
	dir := filepath.Join(root, "journal")
	temp := filepath.Join(dir, ".ROTATION.tmp")
	info, err := os.Lstat(temp)
	if errors.Is(err, os.ErrNotExist) {
		return syncDir(dir)
	}
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("rotation temp is not a regular file: %w", base.ErrCorrupt)
	}
	if err := os.Remove(temp); err != nil {
		return err
	}
	return syncDir(dir)
}

func loadJournal(root string) (storeformat.RotationJournal, bool, error) {
	data, err := os.ReadFile(filepath.Join(root, "journal", journalName))
	if errors.Is(err, os.ErrNotExist) {
		return storeformat.RotationJournal{}, false, nil
	}
	if err != nil {
		return storeformat.RotationJournal{}, false, err
	}
	journal, err := storeformat.DecodeRotationJournal(data)
	return journal, err == nil, err
}

func removeJournal(root string, hook failpoint.Hook) error {
	dir := filepath.Join(root, "journal")
	if err := failpoint.Hit(hook, PointBeforeJournalRemove); err != nil {
		return err
	}
	if err := os.Remove(filepath.Join(dir, journalName)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := failpoint.Hit(hook, PointBeforeJournalRemoveTemp); err != nil {
		return err
	}
	if err := os.Remove(filepath.Join(dir, ".ROTATION.tmp")); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := failpoint.Hit(hook, PointBeforeJournalRemoveDirSync); err != nil {
		return err
	}
	return syncDir(dir)
}

func syncDir(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	return errors.Join(dir.Sync(), dir.Close())
}

func containsSummary(items []storeformat.FileSummary, id base.DataSegmentID) bool {
	for _, item := range items {
		if item.FileID == uint32(id) {
			return true
		}
	}
	return false
}

func cloneManifest(manifest storeformat.Manifest) storeformat.Manifest {
	manifest.SealedDataSegments = append([]storeformat.FileSummary(nil), manifest.SealedDataSegments...)
	manifest.SealedMappingSegments = append([]storeformat.FileSummary(nil), manifest.SealedMappingSegments...)
	manifest.OpenBatchIDsAtCut = append([]base.BatchID(nil), manifest.OpenBatchIDsAtCut...)
	manifest.SegmentStats = append([]storeformat.SegmentStatsEntry(nil), manifest.SegmentStats...)
	return manifest
}

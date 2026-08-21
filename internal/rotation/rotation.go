package rotation

import (
	"errors"
	"fmt"
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
	PointPrepared          failpoint.Point = "rotation.prepared"
	PointOldSealed         failpoint.Point = "rotation.old-sealed"
	PointNewCreated        failpoint.Point = "rotation.new-created"
	PointManifestInstalled failpoint.Point = "rotation.manifest-installed"
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
	if err := installJournal(m.root, journal); err != nil {
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
	if err := installJournal(m.root, journal); err != nil {
		return nil, err
	}
	journal.Phase = 3
	if err := installJournal(m.root, journal); err != nil {
		return nil, err
	}
	newActive, err := segment.CreateActiveData(m.root, current.StoreUUID, newID, nextFrameSeq+1, current.HardLimits.SegmentSize, m.maxPayload)
	if err != nil {
		return nil, err
	}
	newActive.SetHook(m.hook)
	if err := failpoint.Hit(m.hook, PointNewCreated); err != nil {
		return nil, errors.Join(err, newActive.Close())
	}
	journal.Phase = 4
	if err := installJournal(m.root, journal); err != nil {
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
	if err := installJournal(m.root, journal); err != nil {
		return nil, errors.Join(err, sealed.Close(), newActive.Close())
	}
	if err := m.registry.ReplaceActive(oldID, sealed, newActive); err != nil {
		return nil, errors.Join(err, sealed.Close(), newActive.Close())
	}
	if err := removeJournal(m.root); err != nil {
		return nil, err
	}
	return newActive, nil
}

func (m *Manager) Current() storeformat.Manifest {
	return m.catalog.Snapshot()
}

// Recover completes an interrupted rotation before normal segment opening.
func Recover(root string, current storeformat.Manifest, maxPayload uint64) (storeformat.Manifest, error) {
	journal, found, err := loadJournal(root)
	if err != nil || !found {
		return current, err
	}
	if journal.StoreUUID != current.StoreUUID {
		return storeformat.Manifest{}, fmt.Errorf("rotation StoreUUID mismatch: %w", base.ErrCorrupt)
	}
	if current.ActiveDataSegmentID == journal.NewSegmentID && containsSummary(current.SealedDataSegments, journal.OldSegmentID) {
		if err := removeJournal(root); err != nil {
			return storeformat.Manifest{}, err
		}
		return current, nil
	}
	if current.Generation < journal.BaseManifestGeneration || current.ActiveDataSegmentID != journal.OldSegmentID {
		return storeformat.Manifest{}, fmt.Errorf("rotation base manifest mismatch: %w", base.ErrCorrupt)
	}
	oldSummary, err := ensureOldSealed(root, current, journal, maxPayload)
	if err != nil {
		return storeformat.Manifest{}, err
	}
	nextFrameSeq := base.FrameSeq(oldSummary.LastSeq + 1)
	newPath := filepath.Join(root, "data", segment.ActiveDataFileName(journal.NewSegmentID))
	if _, err := os.Lstat(newPath); errors.Is(err, os.ErrNotExist) {
		active, createErr := segment.CreateActiveData(root, current.StoreUUID, journal.NewSegmentID, nextFrameSeq, current.HardLimits.SegmentSize, maxPayload)
		if createErr != nil {
			return storeformat.Manifest{}, createErr
		}
		if err := active.Close(); err != nil {
			return storeformat.Manifest{}, err
		}
	} else if err != nil {
		return storeformat.Manifest{}, err
	} else {
		active, openErr := segment.OpenActiveData(root, current.StoreUUID, journal.NewSegmentID, current.HardLimits.SegmentSize, maxPayload)
		if openErr != nil {
			return storeformat.Manifest{}, openErr
		}
		if active.End() != storeformat.SegmentHeaderSize {
			_ = active.Close()
			return storeformat.Manifest{}, fmt.Errorf("unpublished new active is not empty: %w", base.ErrCorrupt)
		}
		if err := active.Close(); err != nil {
			return storeformat.Manifest{}, err
		}
	}
	nextManifest, err := rotatedManifest(current, oldSummary, journal.NewSegmentID, nextFrameSeq)
	if err != nil {
		return storeformat.Manifest{}, err
	}
	if err := (manifest.Installer{Dir: root}).Install(nextManifest); err != nil {
		return storeformat.Manifest{}, err
	}
	if err := removeJournal(root); err != nil {
		return storeformat.Manifest{}, err
	}
	return nextManifest, nil
}

func ensureOldSealed(root string, current storeformat.Manifest, journal storeformat.RotationJournal, maxPayload uint64) (storeformat.FileSummary, error) {
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
	return segment.ResumeSeal(root, current.StoreUUID, journal.OldSegmentID, current.HardLimits.SegmentSize, maxPayload, current.NextFrameSeq)
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

func installJournal(root string, journal storeformat.RotationJournal) error {
	encoded, err := storeformat.EncodeRotationJournal(journal)
	if err != nil {
		return err
	}
	dirPath := filepath.Join(root, "journal")
	temp := filepath.Join(dirPath, ".ROTATION.tmp")
	final := filepath.Join(dirPath, journalName)
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
	return syncDir(dirPath)
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

func removeJournal(root string) error {
	dir := filepath.Join(root, "journal")
	if err := os.Remove(filepath.Join(dir, journalName)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := os.Remove(filepath.Join(dir, ".ROTATION.tmp")); err != nil && !errors.Is(err, os.ErrNotExist) {
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

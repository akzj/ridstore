package recordlog

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

func Open(root string, cfg Config, catalog CatalogPort) (*Log, error) {
	return OpenWithFaultHook(root, cfg, catalog, nil)
}

func OpenWithFaultHook(root string, cfg Config, catalog CatalogPort, hook FaultHook) (*Log, error) {
	if root == "" || catalog == nil {
		return nil, ErrInvalidConfig
	}
	files := fileBackend(osFileBackend{})
	snapshot := catalog.SnapshotRecordLog()
	if err := snapshot.validate(); err != nil {
		return nil, err
	}
	if err := cfg.validate(snapshot.SegmentSize, snapshot.MaxPayloadBytes); err != nil {
		return nil, err
	}
	var err error
	snapshot, err = recoverRotation(root, catalog, snapshot, files, hook)
	if err != nil {
		return nil, err
	}
	sealed := make([]*sealedSegment, 0, len(snapshot.SealedSegments))
	fail := func(cause error) (*Log, error) {
		for _, segment := range sealed {
			cause = errors.Join(cause, segment.close())
		}
		return nil, cause
	}
	for _, summary := range snapshot.SealedSegments {
		segment, err := openSealedSegment(root, snapshot.headerFor(summary.SegmentID), summary, files)
		if err != nil {
			return fail(err)
		}
		sealed = append(sealed, segment)
	}
	active, _, err := openActiveSegment(root, snapshot.headerFor(snapshot.ActiveSegmentID), files, hook)
	if err != nil {
		return fail(err)
	}
	registry, err := newSegmentRegistry(active, sealed)
	if err != nil {
		_ = active.close()
		return fail(err)
	}
	manager := &rotationManager{root: root, catalog: catalog, files: files, registry: registry, hook: hook}
	log, err := newLog(cfg, snapshot.MaxPayloadBytes, active, registry, manager.rotate)
	if err != nil {
		_ = registry.close()
		return nil, err
	}
	log.root = root
	log.catalog = catalog
	log.files = files
	log.hook = hook
	return log, nil
}

// CreateInitialSegment writes and syncs the first RecordLog file. The caller
// installs the initial global Manifest only after this function succeeds.
func CreateInitialSegment(root string, logID LogID, segmentSize uint32) error {
	header := SegmentHeader{LogID: logID, SegmentID: 1, SegmentSize: segmentSize}
	segment, err := createActiveSegment(root, header, nil, nil)
	if err != nil {
		return err
	}
	return segment.close()
}

// EnsureInitialSegment idempotently completes creation of the first empty
// segment. It is only valid before the initial Catalog generation is
// published; callers must serialize it with the store directory lock.
func EnsureInitialSegment(root string, logID LogID, segmentSize uint32) error {
	header := SegmentHeader{LogID: logID, SegmentID: 1, SegmentSize: segmentSize}
	if _, err := EncodeSegmentHeader(header); err != nil {
		return err
	}
	files := osFileBackend{}
	dir, err := ensureRecordsDirectory(root, files)
	if err != nil {
		return err
	}
	activePath := filepath.Join(dir, activeSegmentName(1))
	if _, err := os.Lstat(activePath); err == nil {
		segment, repaired, err := openActiveSegment(root, header, nil, nil)
		if err != nil {
			return err
		}
		invalid := repaired || segment.end != SegmentHeaderSize || segment.records != 0
		closeErr := segment.close()
		if invalid {
			return errors.Join(ErrCorrupt, closeErr)
		}
		creatingPath := filepath.Join(dir, creatingSegmentName(1))
		if err := os.Remove(creatingPath); err == nil {
			return errors.Join(files.syncDir(dir), closeErr)
		} else if !errors.Is(err, os.ErrNotExist) {
			return errors.Join(err, closeErr)
		}
		return closeErr
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	creatingPath := filepath.Join(dir, creatingSegmentName(1))
	if err := os.Remove(creatingPath); err == nil {
		if err := files.syncDir(dir); err != nil {
			return err
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	segment, err := createActiveSegment(root, header, nil, nil)
	if err != nil {
		return err
	}
	return segment.close()
}

type rotationManager struct {
	root     string
	catalog  CatalogPort
	files    fileBackend
	registry *segmentRegistry
	hook     FaultHook
}

func (m *rotationManager) rotate(old *activeSegment) (*activeSegment, error) {
	snapshot := m.catalog.SnapshotRecordLog()
	if err := snapshot.validate(); err != nil {
		return nil, err
	}
	if snapshot.ActiveSegmentID != old.header.SegmentID || snapshot.LogID != old.header.LogID || snapshot.SegmentSize != old.header.SegmentSize {
		return nil, fmt.Errorf("catalog active segment changed: %w", ErrCorrupt)
	}
	summary := old.summary()
	journal := rotationJournal{
		BaseGeneration: snapshot.Generation, LogID: snapshot.LogID, SegmentSize: snapshot.SegmentSize,
		Old: summary, NewActive: snapshot.NextSegmentID, NextSegmentID: snapshot.NextSegmentID + 1,
	}
	// The durable journal names Old.ValidEnd as recovery truth. Make the
	// corresponding record bytes durable before publishing that promise.
	if err := old.sync(); err != nil {
		return nil, err
	}
	if err := installRotationJournal(m.root, journal, m.files, m.hook); err != nil {
		return nil, err
	}
	sealed, sealedSummary, newActive, err := m.prepareRotationFiles(old, snapshot.headerFor(journal.NewActive))
	if err != nil {
		return nil, err
	}
	if sealedSummary != summary {
		return nil, errors.Join(fmt.Errorf("sealed summary changed: %w", ErrCorrupt), newActive.close())
	}
	installed, err := installCatalogRotation(m.catalog, snapshot, journal)
	if err != nil {
		return nil, errors.Join(err, newActive.close())
	}
	if err := validateRotationResult(snapshot, installed, summary, journal.NewActive, journal.NextSegmentID); err != nil {
		return nil, errors.Join(err, newActive.close())
	}
	if err := m.registry.publishRotation(old.header.SegmentID, sealed, newActive); err != nil {
		return nil, errors.Join(err, newActive.close())
	}
	if err := removeRotationJournal(m.root, m.files, m.hook); err != nil {
		return nil, err
	}
	return newActive, nil
}

type rotationSealResult struct {
	sealed  *sealedSegment
	summary SegmentSummary
	err     error
}

type rotationActiveResult struct {
	active *activeSegment
	err    error
}

// prepareRotationFiles runs only after the rotation journal is durable, so
// recovery can complete either half independently after a crash. In normal
// operation footer sealing and next-header creation use different files and
// may overlap their fsync latency. Fault-injected paths remain serial so the
// crash matrix retains deterministic boundaries and ordering.
func (m *rotationManager) prepareRotationFiles(old *activeSegment, header SegmentHeader) (*sealedSegment, SegmentSummary, *activeSegment, error) {
	if m.hook != nil {
		sealed, summary, err := old.seal()
		if err != nil {
			return nil, SegmentSummary{}, nil, err
		}
		active, err := createActiveSegment(m.root, header, m.files, m.hook)
		if err != nil {
			return nil, SegmentSummary{}, nil, err
		}
		return sealed, summary, active, nil
	}
	sealedResult := make(chan rotationSealResult, 1)
	activeResult := make(chan rotationActiveResult, 1)
	go func() {
		sealed, summary, err := old.seal()
		sealedResult <- rotationSealResult{sealed: sealed, summary: summary, err: err}
	}()
	go func() {
		active, err := createActiveSegment(m.root, header, m.files, nil)
		activeResult <- rotationActiveResult{active: active, err: err}
	}()
	sealed := <-sealedResult
	active := <-activeResult
	if sealed.err != nil || active.err != nil {
		var closeErr error
		if active.active != nil {
			closeErr = active.active.close()
		}
		return nil, SegmentSummary{}, nil, errors.Join(sealed.err, active.err, closeErr)
	}
	return sealed.sealed, sealed.summary, active.active, nil
}

func installCatalogRotation(catalog CatalogPort, before CatalogSnapshot, journal rotationJournal) (CatalogSnapshot, error) {
	current := before
	for attempts := 0; attempts < 16; attempts++ {
		installed, err := catalog.InstallRecordLogRotation(current.Generation, journal.Old, journal.NewActive, journal.NextSegmentID)
		if err == nil {
			return installed, nil
		}
		fresh := catalog.SnapshotRecordLog()
		if committedRotation(fresh, journal) {
			return fresh, nil
		}
		if fresh.Generation == current.Generation {
			return CatalogSnapshot{}, err
		}
		if fresh.ActiveSegmentID != journal.Old.SegmentID || fresh.LogID != journal.LogID || fresh.SegmentSize != journal.SegmentSize {
			return CatalogSnapshot{}, errors.Join(err, ErrCorrupt)
		}
		current = fresh
	}
	return CatalogSnapshot{}, fmt.Errorf("catalog rotation did not converge: %w", ErrPoisoned)
}

func committedRotation(snapshot CatalogSnapshot, journal rotationJournal) bool {
	if snapshot.ActiveSegmentID != journal.NewActive || snapshot.NextSegmentID != journal.NextSegmentID {
		return false
	}
	summary, ok := snapshot.sealedSummary(journal.Old.SegmentID)
	return ok && summary == journal.Old
}

func recoverRotation(root string, catalog CatalogPort, snapshot CatalogSnapshot, files fileBackend, hook FaultHook) (CatalogSnapshot, error) {
	journal, found, err := loadRotationJournal(root, files, hook)
	if err != nil {
		return CatalogSnapshot{}, err
	}
	if !found {
		return snapshot, nil
	}
	if journal.LogID != snapshot.LogID || journal.SegmentSize != snapshot.SegmentSize || snapshot.Generation < journal.BaseGeneration {
		return CatalogSnapshot{}, fmt.Errorf("rotation journal identity: %w", ErrCorrupt)
	}
	if committedRotation(snapshot, journal) {
		if err := ensureCommittedRotationFiles(root, snapshot, journal, files, hook); err != nil {
			return CatalogSnapshot{}, err
		}
		if err := removeRotationJournal(root, files, hook); err != nil {
			return CatalogSnapshot{}, err
		}
		return snapshot, nil
	}
	if snapshot.ActiveSegmentID != journal.Old.SegmentID {
		return CatalogSnapshot{}, fmt.Errorf("rotation journal/catalog state: %w", ErrCorrupt)
	}
	sealed, err := resumeJournalSeal(root, snapshot.headerFor(journal.Old.SegmentID), journal.Old, files, hook)
	if err != nil {
		return CatalogSnapshot{}, err
	}
	if err := sealed.close(); err != nil {
		return CatalogSnapshot{}, err
	}
	newActive, err := ensureJournalActive(root, snapshot.headerFor(journal.NewActive), files, hook)
	if err != nil {
		return CatalogSnapshot{}, err
	}
	if err := newActive.close(); err != nil {
		return CatalogSnapshot{}, err
	}
	installed, err := installCatalogRotation(catalog, snapshot, journal)
	if err != nil {
		return CatalogSnapshot{}, err
	}
	if err := validateRotationResult(snapshot, installed, journal.Old, journal.NewActive, journal.NextSegmentID); err != nil {
		return CatalogSnapshot{}, err
	}
	if err := removeRotationJournal(root, files, hook); err != nil {
		return CatalogSnapshot{}, err
	}
	return installed, nil
}

func ensureCommittedRotationFiles(root string, snapshot CatalogSnapshot, journal rotationJournal, files fileBackend, hook FaultHook) error {
	sealed, err := openSealedSegment(root, snapshot.headerFor(journal.Old.SegmentID), journal.Old, files)
	if err != nil {
		return err
	}
	if err := sealed.close(); err != nil {
		return err
	}
	active, _, err := openActiveSegment(root, snapshot.headerFor(journal.NewActive), files, hook)
	if err != nil {
		return err
	}
	return active.close()
}

func resumeJournalSeal(root string, header SegmentHeader, summary SegmentSummary, files fileBackend, hook FaultHook) (*sealedSegment, error) {
	dir := recordsPath(root)
	activePath := filepath.Join(dir, activeSegmentName(header.SegmentID))
	sealedPath := filepath.Join(dir, sealedSegmentName(header.SegmentID))
	_, activeErr := files.stat(activePath)
	_, sealedErr := files.stat(sealedPath)
	activeExists := activeErr == nil
	sealedExists := sealedErr == nil
	if activeExists && sealedExists {
		return nil, fmt.Errorf("active and sealed files both exist: %w", ErrCorrupt)
	}
	if sealedExists {
		return openSealedSegment(root, header, summary, files)
	}
	if !activeExists {
		if activeErr != nil && !errors.Is(activeErr, os.ErrNotExist) {
			return nil, activeErr
		}
		return nil, fmt.Errorf("rotation source missing: %w", ErrCorrupt)
	}
	file, err := files.openFile(activePath, os.O_RDWR, 0)
	if err != nil {
		return nil, err
	}
	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	if info.Size() < int64(summary.ValidEnd) || info.Size() > int64(summary.ValidEnd+SegmentFooterSize) {
		_ = file.Close()
		return nil, fmt.Errorf("rotation source size: %w", ErrCorrupt)
	}
	if info.Size() > int64(summary.ValidEnd) {
		if err := hitSegmentFault(hook, faultBeforeTailTruncate); err != nil {
			_ = file.Close()
			return nil, err
		}
		if err := file.Truncate(int64(summary.ValidEnd)); err != nil {
			_ = file.Close()
			return nil, err
		}
		if err := hitSegmentFault(hook, faultBeforeTailSync); err != nil {
			_ = file.Close()
			return nil, err
		}
		if err := file.Sync(); err != nil {
			_ = file.Close()
			return nil, err
		}
	}
	if err := file.Close(); err != nil {
		return nil, err
	}
	active, repaired, err := openActiveSegment(root, header, files, hook)
	if err != nil {
		return nil, err
	}
	if repaired || active.summary() != summary {
		_ = active.close()
		return nil, fmt.Errorf("rotation source summary: %w", ErrCorrupt)
	}
	sealed, got, err := active.seal()
	if err != nil {
		_ = active.close()
		return nil, err
	}
	if got != summary {
		_ = sealed.close()
		return nil, fmt.Errorf("rotation sealed summary: %w", ErrCorrupt)
	}
	return sealed, nil
}

func ensureJournalActive(root string, header SegmentHeader, files fileBackend, hook FaultHook) (*activeSegment, error) {
	path := filepath.Join(recordsPath(root), activeSegmentName(header.SegmentID))
	if _, err := files.stat(path); err == nil {
		active, repaired, err := openActiveSegment(root, header, files, hook)
		if err != nil {
			return nil, err
		}
		if repaired || active.summary().ValidEnd != SegmentHeaderSize {
			_ = active.close()
			return nil, fmt.Errorf("rotation destination not empty: %w", ErrCorrupt)
		}
		return active, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	creating := filepath.Join(recordsPath(root), creatingSegmentName(header.SegmentID))
	if err := files.remove(creating); err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	return createActiveSegment(root, header, files, hook)
}

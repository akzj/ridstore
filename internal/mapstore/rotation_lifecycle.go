package mapstore

import (
	"errors"
	"os"
	"path/filepath"

	"github.com/akzj/ridstore/internal/model"
)

func recoverRotation(root string, catalog CatalogPort) (CatalogSnapshot, error) {
	state := catalog.SnapshotMapStore()
	journal, found, err := loadRotationJournal(root)
	if err != nil || !found {
		return state, err
	}
	if state.Generation < journal.BaseGeneration || state.StoreID != journal.StoreID || state.SegmentSize != journal.SegmentSize {
		return CatalogSnapshot{}, ErrCorrupt
	}
	if committedRotation(state, journal) {
		if err := verifyCommittedRotationFiles(root, state, journal); err != nil {
			return CatalogSnapshot{}, err
		}
		if err := removeRotationJournal(root); err != nil {
			return CatalogSnapshot{}, err
		}
		return state, nil
	}
	if state.ActiveSegment != journal.Old.SegmentID || state.NextSegment != journal.NewActive {
		return CatalogSnapshot{}, ErrCorrupt
	}
	sealed, err := resumeSeal(root, state.headerFor(journal.Old.SegmentID), state.Root, journal.Old)
	if err != nil {
		return CatalogSnapshot{}, err
	}
	if err := sealed.file.Close(); err != nil {
		return CatalogSnapshot{}, err
	}
	active, err := ensureNewActive(root, state.headerFor(journal.NewActive))
	if err != nil {
		return CatalogSnapshot{}, err
	}
	if err := active.file.Close(); err != nil {
		return CatalogSnapshot{}, err
	}
	installed, err := installCatalogRotation(catalog, state, journal)
	if err != nil {
		return CatalogSnapshot{}, err
	}
	if err := removeRotationJournal(root); err != nil {
		return CatalogSnapshot{}, err
	}
	return installed, nil
}

func installCatalogRotation(catalog CatalogPort, state CatalogSnapshot, journal rotationJournal) (CatalogSnapshot, error) {
	current := state
	ref := SegmentRef{SegmentID: journal.Old.SegmentID, ValidEnd: journal.Old.ValidEnd}
	for attempts := 0; attempts < 16; attempts++ {
		installed, err := catalog.InstallMapStoreRotation(current.Generation, ref, journal.NewActive, journal.NextSegment)
		if err == nil {
			if !committedRotation(installed, journal) {
				return CatalogSnapshot{}, ErrCorrupt
			}
			return installed, nil
		}
		fresh := catalog.SnapshotMapStore()
		if committedRotation(fresh, journal) {
			return fresh, nil
		}
		if fresh.Generation == current.Generation {
			return CatalogSnapshot{}, err
		}
		if fresh.StoreID != journal.StoreID || fresh.SegmentSize != journal.SegmentSize || fresh.ActiveSegment != journal.Old.SegmentID || fresh.NextSegment != journal.NewActive {
			return CatalogSnapshot{}, errors.Join(err, ErrCorrupt)
		}
		current = fresh
	}
	return CatalogSnapshot{}, ErrPoisoned
}

func verifyCommittedRotationFiles(root string, state CatalogSnapshot, journal rotationJournal) error {
	sealed, err := openSealed(root, state.headerFor(journal.Old.SegmentID), SegmentRef{SegmentID: journal.Old.SegmentID, ValidEnd: journal.Old.ValidEnd})
	if err != nil {
		return err
	}
	if err := sealed.file.Close(); err != nil {
		return err
	}
	active, _, err := openActive(root, state.headerFor(journal.NewActive), state.Root, nil)
	if err != nil {
		return err
	}
	return active.file.Close()
}

func resumeSeal(root string, header SegmentHeader, rootAddr model.MapAddr, summary SegmentSummary) (*segmentFile, error) {
	dir := filepath.Join(root, mappingDirectory)
	activePath := filepath.Join(dir, activeName(header.SegmentID))
	sealedPath := filepath.Join(dir, sealedName(header.SegmentID))
	_, activeErr := os.Stat(activePath)
	_, sealedErr := os.Stat(sealedPath)
	activeExists := activeErr == nil
	sealedExists := sealedErr == nil
	if activeExists && sealedExists {
		return nil, ErrCorrupt
	}
	if sealedExists {
		return openSealed(root, header, SegmentRef{SegmentID: summary.SegmentID, ValidEnd: summary.ValidEnd})
	}
	if !activeExists {
		return nil, errors.Join(ErrCorrupt, activeErr, sealedErr)
	}
	file, err := os.OpenFile(activePath, os.O_RDWR, 0)
	if err != nil {
		return nil, err
	}
	fail := func(cause error) (*segmentFile, error) { return nil, errors.Join(cause, file.Close()) }
	if err := verifyHeader(file, header); err != nil {
		return fail(err)
	}
	info, err := file.Stat()
	if err != nil || info.Size() < int64(summary.ValidEnd) || info.Size() > int64(summary.ValidEnd+SegmentFooterSize) {
		return fail(errors.Join(ErrCorrupt, err))
	}
	actual, repaired, err := scanNodes(file, header, summary.ValidEnd, rootAddr)
	if err != nil || repaired || actual != summary {
		return fail(errors.Join(ErrCorrupt, err))
	}
	if info.Size() != int64(summary.ValidEnd+SegmentFooterSize) {
		if err := file.Truncate(int64(summary.ValidEnd)); err != nil {
			return fail(err)
		}
		footer, _ := EncodeSegmentFooter(summary.footer())
		if err := writeFullAt(file, footer[:], int64(summary.ValidEnd)); err != nil {
			return fail(err)
		}
	} else {
		footerBytes := make([]byte, SegmentFooterSize)
		if _, err := file.ReadAt(footerBytes, int64(summary.ValidEnd)); err != nil {
			return fail(err)
		}
		footer, err := DecodeSegmentFooter(footerBytes)
		if err != nil || footer != summary.footer() {
			return fail(errors.Join(ErrCorrupt, err))
		}
	}
	if err := file.Sync(); err != nil {
		return fail(err)
	}
	if err := file.Close(); err != nil {
		return nil, err
	}
	if err := os.Rename(activePath, sealedPath); err != nil {
		return nil, err
	}
	if err := syncDirectory(dir); err != nil {
		return nil, err
	}
	return openSealed(root, header, SegmentRef{SegmentID: summary.SegmentID, ValidEnd: summary.ValidEnd})
}

func ensureNewActive(root string, header SegmentHeader) (*segmentFile, error) {
	dir := filepath.Join(root, mappingDirectory)
	activePath := filepath.Join(dir, activeName(header.SegmentID))
	if _, err := os.Stat(activePath); err == nil {
		active, repaired, err := openActive(root, header, 0, nil)
		if err != nil {
			return nil, err
		}
		if repaired || active.summary.NodeCount != 0 {
			_ = active.file.Close()
			return nil, ErrCorrupt
		}
		return active, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	if err := os.Remove(filepath.Join(dir, creatingName(header.SegmentID))); err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	return createActive(root, header)
}

func createActive(root string, header SegmentHeader) (*segmentFile, error) {
	encoded, err := EncodeSegmentHeader(header)
	if err != nil {
		return nil, err
	}
	dir, err := ensureDirectory(root)
	if err != nil {
		return nil, err
	}
	creatingPath := filepath.Join(dir, creatingName(header.SegmentID))
	activePath := filepath.Join(dir, activeName(header.SegmentID))
	file, err := os.OpenFile(creatingPath, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	fail := func(cause error) (*segmentFile, error) { return nil, errors.Join(cause, file.Close()) }
	if err := writeFullAt(file, encoded[:], 0); err != nil {
		return fail(err)
	}
	if err := file.Sync(); err != nil {
		return fail(err)
	}
	if err := os.Rename(creatingPath, activePath); err != nil {
		return fail(err)
	}
	if err := syncDirectory(dir); err != nil {
		return fail(err)
	}
	return &segmentFile{file: file, header: header, summary: SegmentSummary{SegmentID: header.SegmentID, ValidEnd: SegmentHeaderSize}}, nil
}

func sealActive(root string, active *segmentFile, expected SegmentSummary) (*segmentFile, error) {
	if active == nil || active.summary != expected || expected.NodeCount == 0 {
		return nil, ErrInvalid
	}
	footer, err := EncodeSegmentFooter(expected.footer())
	if err != nil {
		return nil, err
	}
	if err := writeFullAt(active.file, footer[:], int64(expected.ValidEnd)); err != nil {
		return nil, err
	}
	if err := active.file.Sync(); err != nil {
		return nil, err
	}
	if err := active.file.Close(); err != nil {
		return nil, err
	}
	dir := filepath.Join(root, mappingDirectory)
	if err := os.Rename(filepath.Join(dir, activeName(expected.SegmentID)), filepath.Join(dir, sealedName(expected.SegmentID))); err != nil {
		return nil, err
	}
	if err := syncDirectory(dir); err != nil {
		return nil, err
	}
	return openSealed(root, active.header, SegmentRef{SegmentID: expected.SegmentID, ValidEnd: expected.ValidEnd})
}

package mapstore

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"os"
	"path/filepath"

	"github.com/akzj/ridstore/internal/model"
)

const rotationJournalSize = 128

var rotationMagic = [8]byte{'R', 'I', 'D', 'M', 'R', 'O', 'T', '2'}

type rotationJournal struct {
	BaseGeneration uint64
	StoreID        StoreID
	SegmentSize    uint32
	Old            SegmentSummary
	NewActive      model.MapSegmentID
	NextSegment    model.MapSegmentID
}

func (j rotationJournal) valid() bool {
	return j.BaseGeneration != 0 && j.StoreID != (StoreID{}) && j.Old.NodeCount != 0 && j.Old.valid(j.SegmentSize) &&
		j.NewActive == j.Old.SegmentID+1 && j.NextSegment == j.NewActive+1 && j.NewActive != 0 && j.NextSegment != 0
}

func encodeRotationJournal(j rotationJournal) ([rotationJournalSize]byte, error) {
	var dst [rotationJournalSize]byte
	if !j.valid() {
		return dst, ErrInvalid
	}
	copy(dst[0:8], rotationMagic[:])
	binary.LittleEndian.PutUint16(dst[8:10], FormatVersion)
	binary.LittleEndian.PutUint16(dst[10:12], rotationJournalSize)
	binary.LittleEndian.PutUint64(dst[16:24], j.BaseGeneration)
	copy(dst[24:40], j.StoreID[:])
	binary.LittleEndian.PutUint32(dst[40:44], j.SegmentSize)
	binary.LittleEndian.PutUint32(dst[44:48], uint32(j.Old.SegmentID))
	binary.LittleEndian.PutUint32(dst[48:52], j.Old.ValidEnd)
	binary.LittleEndian.PutUint32(dst[52:56], uint32(j.NewActive))
	binary.LittleEndian.PutUint32(dst[56:60], uint32(j.NextSegment))
	binary.LittleEndian.PutUint64(dst[64:72], j.Old.FirstSeq)
	binary.LittleEndian.PutUint64(dst[72:80], j.Old.LastSeq)
	binary.LittleEndian.PutUint64(dst[80:88], j.Old.NodeCount)
	binary.LittleEndian.PutUint32(dst[124:128], crc32.Checksum(dst[:124], crcTable))
	return dst, nil
}

func decodeRotationJournal(src []byte) (rotationJournal, error) {
	if len(src) != rotationJournalSize || string(src[:8]) != string(rotationMagic[:]) {
		return rotationJournal{}, ErrCorrupt
	}
	if binary.LittleEndian.Uint16(src[8:10]) != FormatVersion {
		return rotationJournal{}, ErrUnsupported
	}
	if binary.LittleEndian.Uint16(src[10:12]) != rotationJournalSize || !zeroBytes(src[12:16]) || !zeroBytes(src[60:64]) || !zeroBytes(src[88:124]) || binary.LittleEndian.Uint32(src[124:128]) != crc32.Checksum(src[:124], crcTable) {
		return rotationJournal{}, ErrCorrupt
	}
	j := rotationJournal{
		BaseGeneration: binary.LittleEndian.Uint64(src[16:24]),
		SegmentSize:    binary.LittleEndian.Uint32(src[40:44]),
		Old: SegmentSummary{
			SegmentID: model.MapSegmentID(binary.LittleEndian.Uint32(src[44:48])),
			ValidEnd:  binary.LittleEndian.Uint32(src[48:52]),
			FirstSeq:  binary.LittleEndian.Uint64(src[64:72]),
			LastSeq:   binary.LittleEndian.Uint64(src[72:80]),
			NodeCount: binary.LittleEndian.Uint64(src[80:88]),
		},
		NewActive:   model.MapSegmentID(binary.LittleEndian.Uint32(src[52:56])),
		NextSegment: model.MapSegmentID(binary.LittleEndian.Uint32(src[56:60])),
	}
	copy(j.StoreID[:], src[24:40])
	if !j.valid() {
		return rotationJournal{}, ErrCorrupt
	}
	return j, nil
}

func rotationPath(root string) string { return filepath.Join(root, mappingDirectory, "ROTATION") }
func rotationTempPath(root string) string {
	return filepath.Join(root, mappingDirectory, "ROTATION.tmp")
}

func installRotationJournal(root string, journal rotationJournal) error {
	encoded, err := encodeRotationJournal(journal)
	if err != nil {
		return err
	}
	file, err := os.OpenFile(rotationTempPath(root), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	if err := writeFullAt(file, encoded[:], 0); err != nil {
		return errors.Join(err, file.Close())
	}
	if err := file.Sync(); err != nil {
		return errors.Join(err, file.Close())
	}
	if err := file.Close(); err != nil {
		return err
	}
	if err := os.Rename(rotationTempPath(root), rotationPath(root)); err != nil {
		return err
	}
	return syncDirectory(filepath.Join(root, mappingDirectory))
}

func loadRotationJournal(root string) (rotationJournal, bool, error) {
	value, err := os.ReadFile(rotationPath(root))
	if errors.Is(err, os.ErrNotExist) {
		if err := removeRotationTemp(root); err != nil {
			return rotationJournal{}, false, err
		}
		return rotationJournal{}, false, nil
	}
	if err != nil {
		return rotationJournal{}, false, err
	}
	j, err := decodeRotationJournal(value)
	return j, err == nil, err
}

func removeRotationTemp(root string) error {
	err := os.Remove(rotationTempPath(root))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	return syncDirectory(filepath.Join(root, mappingDirectory))
}

func removeRotationJournal(root string) error {
	if err := os.Remove(rotationPath(root)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := os.Remove(rotationTempPath(root)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return syncDirectory(filepath.Join(root, mappingDirectory))
}

func committedRotation(state CatalogSnapshot, journal rotationJournal) bool {
	if state.ActiveSegment != journal.NewActive || state.NextSegment != journal.NextSegment {
		return false
	}
	for _, segment := range state.SealedSegments {
		if segment.SegmentID == journal.Old.SegmentID {
			return segment.ValidEnd == journal.Old.ValidEnd
		}
	}
	return false
}

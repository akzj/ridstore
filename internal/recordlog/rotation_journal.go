package recordlog

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
)

const rotationJournalSize = 96

var rotationJournalMagic = [8]byte{'R', 'I', 'D', 'R', 'J', 'V', '2', 0}

type rotationJournal struct {
	BaseGeneration uint64
	LogID          LogID
	SegmentSize    uint32
	Old            SegmentSummary
	NewActive      SegmentID
	NextSegmentID  SegmentID
}

func (j rotationJournal) validate() error {
	if j.BaseGeneration == 0 || j.LogID == (LogID{}) || j.Old.SegmentID == ^SegmentID(0) || j.NewActive == ^SegmentID(0) || j.NewActive != j.Old.SegmentID+1 || j.NextSegmentID != j.NewActive+1 || j.Old.validate(j.SegmentSize) != nil {
		return ErrInvalidConfig
	}
	return nil
}

func encodeRotationJournal(j rotationJournal) ([rotationJournalSize]byte, error) {
	var encoded [rotationJournalSize]byte
	if err := j.validate(); err != nil {
		return encoded, err
	}
	copy(encoded[0:8], rotationJournalMagic[:])
	binary.LittleEndian.PutUint16(encoded[8:10], FormatVersion)
	binary.LittleEndian.PutUint16(encoded[10:12], rotationJournalSize)
	binary.LittleEndian.PutUint64(encoded[16:24], j.BaseGeneration)
	copy(encoded[24:40], j.LogID[:])
	binary.LittleEndian.PutUint32(encoded[40:44], j.SegmentSize)
	binary.LittleEndian.PutUint32(encoded[44:48], uint32(j.Old.SegmentID))
	binary.LittleEndian.PutUint32(encoded[48:52], j.Old.ValidEnd)
	binary.LittleEndian.PutUint64(encoded[56:64], j.Old.RecordCount)
	binary.LittleEndian.PutUint64(encoded[64:72], uint64(j.Old.FirstAddr))
	binary.LittleEndian.PutUint64(encoded[72:80], uint64(j.Old.LastAddr))
	binary.LittleEndian.PutUint32(encoded[80:84], uint32(j.NewActive))
	binary.LittleEndian.PutUint32(encoded[84:88], uint32(j.NextSegmentID))
	binary.LittleEndian.PutUint32(encoded[92:96], crc32.Checksum(encoded[:92], crcTable))
	return encoded, nil
}

func decodeRotationJournal(encoded []byte) (rotationJournal, error) {
	if len(encoded) != rotationJournalSize || string(encoded[:8]) != string(rotationJournalMagic[:]) || binary.LittleEndian.Uint16(encoded[8:10]) != FormatVersion {
		return rotationJournal{}, fmt.Errorf("rotation journal header: %w", ErrCorrupt)
	}
	if binary.LittleEndian.Uint16(encoded[10:12]) != rotationJournalSize || !allZero(encoded[12:16]) || !allZero(encoded[52:56]) || !allZero(encoded[88:92]) || binary.LittleEndian.Uint32(encoded[92:96]) != crc32.Checksum(encoded[:92], crcTable) {
		return rotationJournal{}, fmt.Errorf("rotation journal fields: %w", ErrCorrupt)
	}
	j := rotationJournal{
		BaseGeneration: binary.LittleEndian.Uint64(encoded[16:24]),
		SegmentSize:    binary.LittleEndian.Uint32(encoded[40:44]),
		Old: SegmentSummary{
			SegmentID: SegmentID(binary.LittleEndian.Uint32(encoded[44:48])), ValidEnd: binary.LittleEndian.Uint32(encoded[48:52]),
			RecordCount: binary.LittleEndian.Uint64(encoded[56:64]), FirstAddr: VAddr(binary.LittleEndian.Uint64(encoded[64:72])), LastAddr: VAddr(binary.LittleEndian.Uint64(encoded[72:80])),
		},
		NewActive: SegmentID(binary.LittleEndian.Uint32(encoded[80:84])), NextSegmentID: SegmentID(binary.LittleEndian.Uint32(encoded[84:88])),
	}
	copy(j.LogID[:], encoded[24:40])
	if err := j.validate(); err != nil {
		return rotationJournal{}, fmt.Errorf("rotation journal values: %w", ErrCorrupt)
	}
	return j, nil
}

func journalDirectory(root string) string { return filepath.Join(root, "journal") }
func rotationJournalPath(root string) string {
	return filepath.Join(journalDirectory(root), "RECORDLOG-ROTATION-v2")
}
func rotationJournalTempPath(root string) string { return rotationJournalPath(root) + ".tmp" }

func ensureJournalDirectory(root string, files fileBackend) (string, error) {
	dir := journalDirectory(root)
	err := files.mkdir(dir, 0o700)
	if err == nil {
		if err := files.syncDir(root); err != nil {
			return "", err
		}
		return dir, nil
	}
	if !errors.Is(err, os.ErrExist) {
		return "", err
	}
	info, statErr := files.stat(dir)
	if statErr != nil {
		return "", statErr
	}
	if !info.IsDir() {
		return "", fmt.Errorf("journal path is not a directory: %w", ErrCorrupt)
	}
	return dir, nil
}

func installRotationJournal(root string, journal rotationJournal, files fileBackend, hook FaultHook) error {
	encoded, err := encodeRotationJournal(journal)
	if err != nil {
		return err
	}
	dir, err := ensureJournalDirectory(root, files)
	if err != nil {
		return err
	}
	if _, err := files.stat(rotationJournalPath(root)); err == nil {
		return fmt.Errorf("rotation journal already exists: %w", ErrCorrupt)
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	temp := rotationJournalTempPath(root)
	file, err := files.openFile(temp, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	fail := func(cause error) error { return errors.Join(cause, file.Close()) }
	if err := hitSegmentFault(hook, faultBeforeJournalWrite); err != nil {
		return fail(err)
	}
	if _, err := writeFullAt(file, encoded[:], 0); err != nil {
		return fail(err)
	}
	if err := hitSegmentFault(hook, faultBeforeJournalSync); err != nil {
		return fail(err)
	}
	if err := file.Sync(); err != nil {
		return fail(err)
	}
	if err := file.Close(); err != nil {
		return err
	}
	if err := hitSegmentFault(hook, faultBeforeJournalRename); err != nil {
		return err
	}
	if err := files.rename(temp, rotationJournalPath(root)); err != nil {
		return err
	}
	if err := hitSegmentFault(hook, faultBeforeJournalDirSync); err != nil {
		return err
	}
	return files.syncDir(dir)
}

func loadRotationJournal(root string, files fileBackend, hook FaultHook) (rotationJournal, bool, error) {
	encoded, err := os.ReadFile(rotationJournalPath(root))
	if errors.Is(err, os.ErrNotExist) {
		if err := removeRotationJournalTemp(root, files, hook); err != nil {
			return rotationJournal{}, false, err
		}
		if _, err := files.stat(journalDirectory(root)); errors.Is(err, os.ErrNotExist) {
			return rotationJournal{}, false, nil
		} else if err != nil {
			return rotationJournal{}, false, err
		}
		// A previous cleanup may have removed the journal but failed its
		// directory sync. A successful Open closes that durable ambiguity.
		return rotationJournal{}, false, files.syncDir(journalDirectory(root))
	}
	if err != nil {
		return rotationJournal{}, false, err
	}
	journal, err := decodeRotationJournal(encoded)
	return journal, err == nil, err
}

func removeRotationJournalTemp(root string, files fileBackend, hook FaultHook) error {
	if _, err := files.stat(rotationJournalTempPath(root)); errors.Is(err, os.ErrNotExist) {
		return nil
	} else if err != nil {
		return err
	}
	if err := hitSegmentFault(hook, faultBeforeJournalRemove); err != nil {
		return err
	}
	if err := files.remove(rotationJournalTempPath(root)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := hitSegmentFault(hook, faultBeforeCleanupDirSync); err != nil {
		return err
	}
	return files.syncDir(journalDirectory(root))
}

func removeRotationJournal(root string, files fileBackend, hook FaultHook) error {
	exists := false
	for _, path := range []string{rotationJournalPath(root), rotationJournalTempPath(root)} {
		if _, err := files.stat(path); err == nil {
			exists = true
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	if exists {
		if err := hitSegmentFault(hook, faultBeforeJournalRemove); err != nil {
			return err
		}
	}
	if err := files.remove(rotationJournalPath(root)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := files.remove(rotationJournalTempPath(root)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if exists {
		if err := hitSegmentFault(hook, faultBeforeCleanupDirSync); err != nil {
			return err
		}
	}
	return files.syncDir(journalDirectory(root))
}

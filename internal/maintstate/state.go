package maintstate

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"

	"github.com/akzj/ridstore/internal/model"
	"github.com/akzj/ridstore/internal/recordlog"
	"github.com/akzj/ridstore/internal/storecatalog"
)

const (
	encodedSize = 128
	version     = uint16(2)
	journalName = "MAINTENANCE.v2"
	tempName    = ".MAINTENANCE.v2.tmp"
)

var (
	magic      = [8]byte{'R', 'I', 'D', 'M', 'N', 'T', '2', 0}
	crcTable   = crc32.MakeTable(crc32.Castagnoli)
	ErrInvalid = errors.New("maintstate: invalid state")
	ErrCorrupt = errors.New("maintstate: corrupt state")
	ErrActive  = errors.New("maintstate: operation already active")
)

type Operation uint8

const DataRetire Operation = 1

// State is the single durable marker around one irreversible maintenance
// transition. Catalog membership, not Phase, determines recovery direction.
type State struct {
	Operation        Operation
	StoreUUID        storecatalog.StoreUUID
	LogID            recordlog.LogID
	BaseGeneration   uint64
	CoveredCommitSeq model.CommitSeq
	ReplayStart      recordlog.LogPos
	Source           recordlog.SegmentSummary
}

func Encode(state State) ([encodedSize]byte, error) {
	var dst [encodedSize]byte
	if err := validate(state); err != nil {
		return dst, err
	}
	copy(dst[0:8], magic[:])
	binary.LittleEndian.PutUint16(dst[8:10], version)
	binary.LittleEndian.PutUint16(dst[10:12], encodedSize)
	dst[12] = byte(state.Operation)
	copy(dst[16:32], state.StoreUUID[:])
	copy(dst[32:48], state.LogID[:])
	binary.LittleEndian.PutUint64(dst[48:56], state.BaseGeneration)
	binary.LittleEndian.PutUint64(dst[56:64], uint64(state.CoveredCommitSeq))
	binary.LittleEndian.PutUint64(dst[64:72], state.ReplayStart.Uint64())
	binary.LittleEndian.PutUint32(dst[72:76], uint32(state.Source.SegmentID))
	binary.LittleEndian.PutUint32(dst[76:80], state.Source.ValidEnd)
	binary.LittleEndian.PutUint64(dst[80:88], state.Source.RecordCount)
	binary.LittleEndian.PutUint64(dst[88:96], uint64(state.Source.FirstAddr))
	binary.LittleEndian.PutUint64(dst[96:104], uint64(state.Source.LastAddr))
	binary.LittleEndian.PutUint32(dst[124:128], crc32.Checksum(dst[:124], crcTable))
	return dst, nil
}

func Decode(src []byte) (State, error) {
	if len(src) != encodedSize || string(src[:8]) != string(magic[:]) ||
		binary.LittleEndian.Uint16(src[8:10]) != version || binary.LittleEndian.Uint16(src[10:12]) != encodedSize ||
		!allZero(src[13:16]) || !allZero(src[104:124]) ||
		binary.LittleEndian.Uint32(src[124:128]) != crc32.Checksum(src[:124], crcTable) {
		return State{}, ErrCorrupt
	}
	replay, err := recordlog.ParseLogPos(binary.LittleEndian.Uint64(src[64:72]))
	if err != nil {
		return State{}, ErrCorrupt
	}
	state := State{
		Operation: Operation(src[12]), BaseGeneration: binary.LittleEndian.Uint64(src[48:56]),
		CoveredCommitSeq: model.CommitSeq(binary.LittleEndian.Uint64(src[56:64])), ReplayStart: replay,
		Source: recordlog.SegmentSummary{
			SegmentID: recordlog.SegmentID(binary.LittleEndian.Uint32(src[72:76])),
			ValidEnd:  binary.LittleEndian.Uint32(src[76:80]), RecordCount: binary.LittleEndian.Uint64(src[80:88]),
			FirstAddr: recordlog.VAddr(binary.LittleEndian.Uint64(src[88:96])), LastAddr: recordlog.VAddr(binary.LittleEndian.Uint64(src[96:104])),
		},
	}
	copy(state.StoreUUID[:], src[16:32])
	copy(state.LogID[:], src[32:48])
	if err := validate(state); err != nil {
		return State{}, fmt.Errorf("maintstate fields: %w", errors.Join(ErrCorrupt, err))
	}
	return state, nil
}

func Install(root string, state State) error {
	if root == "" {
		return ErrInvalid
	}
	encoded, err := Encode(state)
	if err != nil {
		return err
	}
	dir := filepath.Join(root, "journal")
	if err := ensureJournalDir(root, dir); err != nil {
		return err
	}
	final, temp := filepath.Join(dir, journalName), filepath.Join(dir, tempName)
	if _, err := os.Lstat(final); err == nil {
		return ErrActive
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := os.Remove(temp); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	file, err := os.OpenFile(temp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	if n, writeErr := file.Write(encoded[:]); writeErr != nil || n != len(encoded) {
		if writeErr == nil {
			writeErr = io.ErrShortWrite
		}
		return errors.Join(writeErr, file.Close())
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
	return syncDir(dir)
}

func Load(root string) (State, bool, error) {
	if root == "" {
		return State{}, false, ErrInvalid
	}
	dir := filepath.Join(root, "journal")
	removed, err := removeIfRegular(filepath.Join(dir, tempName))
	if err != nil {
		return State{}, false, err
	}
	if removed {
		if err := syncDir(dir); err != nil {
			return State{}, false, err
		}
	}
	data, err := os.ReadFile(filepath.Join(dir, journalName))
	if errors.Is(err, os.ErrNotExist) {
		return State{}, false, nil
	}
	if err != nil {
		return State{}, false, err
	}
	state, err := Decode(data)
	return state, err == nil, err
}

func Remove(root string) error {
	dir := filepath.Join(root, "journal")
	_, err := removeIfRegular(filepath.Join(dir, journalName))
	if err != nil {
		return err
	}
	_, err = removeIfRegular(filepath.Join(dir, tempName))
	if err != nil {
		return err
	}
	return syncDir(dir)
}

func Path(root string) string { return filepath.Join(root, "journal", journalName) }

func validate(state State) error {
	if state.Operation != DataRetire || state.StoreUUID == (storecatalog.StoreUUID{}) || state.LogID == (recordlog.LogID{}) ||
		state.BaseGeneration == 0 || !state.ReplayStart.Valid() || state.Source.SegmentID == 0 || state.Source.ValidEnd < recordlog.SegmentHeaderSize {
		return ErrInvalid
	}
	if state.Source.RecordCount == 0 {
		if state.Source.ValidEnd != recordlog.SegmentHeaderSize || state.Source.FirstAddr != 0 || state.Source.LastAddr != 0 {
			return ErrInvalid
		}
	} else if !state.Source.FirstAddr.Valid() || !state.Source.LastAddr.Valid() ||
		state.Source.FirstAddr.SegmentID() != state.Source.SegmentID || state.Source.LastAddr.SegmentID() != state.Source.SegmentID ||
		state.Source.FirstAddr > state.Source.LastAddr || state.Source.LastAddr.Offset() >= state.Source.ValidEnd {
		return ErrInvalid
	}
	return nil
}

func removeIfRegular(path string) (bool, error) {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
		return false, ErrCorrupt
	}
	if err := os.Remove(path); err != nil {
		return false, err
	}
	return true, nil
}

func ensureJournalDir(root, dir string) error {
	err := os.Mkdir(dir, 0o700)
	if err == nil {
		return syncDir(root)
	}
	if !errors.Is(err, os.ErrExist) {
		return err
	}
	info, err := os.Lstat(dir)
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return ErrCorrupt
	}
	return nil
}

func syncDir(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	return errors.Join(dir.Sync(), dir.Close())
}

func allZero(src []byte) bool {
	for _, value := range src {
		if value != 0 {
			return false
		}
	}
	return true
}

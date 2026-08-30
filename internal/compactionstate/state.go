package compactionstate

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"

	"github.com/akzj/ridstore/internal/recordlog"
	"github.com/akzj/ridstore/internal/storecatalog"
)

const (
	journalName = "DATA-COMPACTION.v3"
	tempName    = ".DATA-COMPACTION.v3.tmp"
	maxSize     = 1 << 20
)

var (
	magic      = [8]byte{'R', 'I', 'D', 'C', 'M', 'P', '3', 0}
	crcTable   = crc32.MakeTable(crc32.Castagnoli)
	ErrInvalid = errors.New("compactionstate: invalid state")
	ErrCorrupt = errors.New("compactionstate: corrupt state")
)

type Phase uint8

const (
	PhaseReserved Phase = iota + 1
	PhaseOutputsPublished
	PhaseInputsRetired
)

type State struct {
	Phase          Phase
	StoreUUID      storecatalog.StoreUUID
	LogID          recordlog.LogID
	BaseGeneration uint64
	Inputs         []recordlog.SegmentSummary
	OutputIDs      []recordlog.SegmentID
	Outputs        []recordlog.SegmentSummary
}

func Install(root string, state State) error { return write(root, state, true) }
func Update(root string, state State) error  { return write(root, state, false) }
func Path(root string) string                { return filepath.Join(root, "journal", journalName) }

func write(root string, state State, exclusive bool) error {
	if root == "" || !valid(state) {
		return ErrInvalid
	}
	payload, err := json.Marshal(state)
	if err != nil || len(payload) > maxSize-16 {
		return ErrInvalid
	}
	encoded := make([]byte, 16+len(payload))
	copy(encoded[:8], magic[:])
	binary.LittleEndian.PutUint32(encoded[8:12], uint32(len(payload)))
	copy(encoded[16:], payload)
	binary.LittleEndian.PutUint32(encoded[12:16], crc32.Checksum(payload, crcTable))
	dir := filepath.Join(root, "journal")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	final, temp := filepath.Join(dir, journalName), filepath.Join(dir, tempName)
	if exclusive {
		if _, err := os.Lstat(final); err == nil {
			return os.ErrExist
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	_ = os.Remove(temp)
	file, err := os.OpenFile(temp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	if n, err := file.Write(encoded); err != nil || n != len(encoded) {
		if err == nil {
			err = io.ErrShortWrite
		}
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
	return syncDir(dir)
}

func Load(root string) (State, bool, error) {
	if root == "" {
		return State{}, false, ErrInvalid
	}
	dir := filepath.Join(root, "journal")
	_ = os.Remove(filepath.Join(dir, tempName))
	src, err := os.ReadFile(filepath.Join(dir, journalName))
	if errors.Is(err, os.ErrNotExist) {
		return State{}, false, nil
	}
	if err != nil {
		return State{}, false, err
	}
	if len(src) < 16 || len(src) > maxSize || string(src[:8]) != string(magic[:]) || int(binary.LittleEndian.Uint32(src[8:12])) != len(src)-16 || binary.LittleEndian.Uint32(src[12:16]) != crc32.Checksum(src[16:], crcTable) {
		return State{}, false, ErrCorrupt
	}
	var state State
	if err := json.Unmarshal(src[16:], &state); err != nil || !valid(state) {
		return State{}, false, ErrCorrupt
	}
	return state, true, nil
}

func Remove(root string) error {
	dir := filepath.Join(root, "journal")
	err := os.Remove(filepath.Join(dir, journalName))
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return syncDir(dir)
}

func RecoveryArtifacts(root string) (bool, error) {
	for _, name := range []string{journalName, tempName} {
		if _, err := os.Lstat(filepath.Join(root, "journal", name)); err == nil {
			return true, nil
		} else if !errors.Is(err, os.ErrNotExist) {
			return false, err
		}
	}
	return false, nil
}

func valid(state State) bool {
	if state.Phase < PhaseReserved || state.Phase > PhaseInputsRetired || state.StoreUUID == (storecatalog.StoreUUID{}) || state.LogID == (recordlog.LogID{}) || state.BaseGeneration == 0 || len(state.Inputs) == 0 {
		return false
	}
	for index, id := range state.OutputIDs {
		if !recordlog.IsCompactionSegment(id) || index != 0 && id != state.OutputIDs[index-1]+1 {
			return false
		}
	}
	if len(state.OutputIDs) == 0 && len(state.Outputs) != 0 {
		return false
	}
	return true
}

func syncDir(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	return errors.Join(dir.Sync(), dir.Close())
}

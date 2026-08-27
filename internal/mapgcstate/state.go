package mapgcstate

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"math"
	"os"
	"path/filepath"

	"github.com/akzj/ridstore/internal/mapstore"
	"github.com/akzj/ridstore/internal/model"
)

const (
	version        = uint16(1)
	headerSize     = 96
	refSize        = 8
	checksumSize   = 4
	maxEncodedSize = 16 << 20
	markerName     = "MAPPING-GC.v2"
	tempName       = "MAPPING-GC.v2.tmp"
	StagingDirName = "mapping-gc-stage-v2"
)

var (
	magic      = [8]byte{'R', 'I', 'D', 'M', 'G', 'C', '2', 0}
	crcTable   = crc32.MakeTable(crc32.Castagnoli)
	ErrInvalid = errors.New("mapgcstate: invalid state")
	ErrCorrupt = errors.New("mapgcstate: corrupt state")
	ErrActive  = errors.New("mapgcstate: operation already active")
)

type FaultPoint string
type FaultHook func(FaultPoint) error

const (
	FaultBeforeTempRemove     FaultPoint = "mapgcstate.before-temp-remove"
	FaultBeforeTempCreate     FaultPoint = "mapgcstate.before-temp-create"
	FaultBeforeWrite          FaultPoint = "mapgcstate.before-write"
	FaultBeforeFileSync       FaultPoint = "mapgcstate.before-file-sync"
	FaultBeforeFileClose      FaultPoint = "mapgcstate.before-file-close"
	FaultBeforePublishRename  FaultPoint = "mapgcstate.before-publish-rename"
	FaultBeforeJournalDirSync FaultPoint = "mapgcstate.before-journal-dir-sync"
	FaultBeforeMarkerRemove   FaultPoint = "mapgcstate.before-marker-remove"
	FaultBeforeCleanupDirSync FaultPoint = "mapgcstate.before-cleanup-dir-sync"
)

type FileSet struct {
	Sealed []mapstore.SegmentRef
	Active model.MapSegmentID
	Next   model.MapSegmentID
	Root   model.MapAddr
}

type State struct {
	StoreID        [16]byte
	BaseGeneration uint64
	SegmentSize    uint32
	Covered        model.CommitSeq
	Old            FileSet
	New            FileSet
}

func Encode(state State) ([]byte, error) {
	if err := validate(state); err != nil {
		return nil, err
	}
	maxRefs := (maxEncodedSize - headerSize - checksumSize) / refSize
	if len(state.Old.Sealed) > maxRefs || len(state.New.Sealed) > maxRefs-len(state.Old.Sealed) {
		return nil, ErrInvalid
	}
	count := len(state.Old.Sealed) + len(state.New.Sealed)
	total := headerSize + count*refSize + checksumSize
	dst := make([]byte, total)
	copy(dst[0:8], magic[:])
	binary.LittleEndian.PutUint16(dst[8:10], version)
	binary.LittleEndian.PutUint16(dst[10:12], headerSize)
	binary.LittleEndian.PutUint32(dst[12:16], uint32(total))
	copy(dst[16:32], state.StoreID[:])
	binary.LittleEndian.PutUint64(dst[32:40], state.BaseGeneration)
	binary.LittleEndian.PutUint32(dst[40:44], state.SegmentSize)
	binary.LittleEndian.PutUint64(dst[48:56], uint64(state.Covered))
	encodeSetHeader(dst[56:76], state.Old)
	encodeSetHeader(dst[76:96], state.New)
	offset := headerSize
	for _, set := range []FileSet{state.Old, state.New} {
		for _, ref := range set.Sealed {
			binary.LittleEndian.PutUint32(dst[offset:offset+4], uint32(ref.SegmentID))
			binary.LittleEndian.PutUint32(dst[offset+4:offset+8], ref.ValidEnd)
			offset += refSize
		}
	}
	binary.LittleEndian.PutUint32(dst[total-checksumSize:], crc32.Checksum(dst[:total-checksumSize], crcTable))
	return dst, nil
}

func Decode(src []byte) (State, error) {
	if len(src) < headerSize+checksumSize || len(src) > maxEncodedSize || string(src[:8]) != string(magic[:]) ||
		binary.LittleEndian.Uint16(src[8:10]) != version || binary.LittleEndian.Uint16(src[10:12]) != headerSize ||
		binary.LittleEndian.Uint32(src[12:16]) != uint32(len(src)) || !zero(src[44:48]) ||
		binary.LittleEndian.Uint32(src[len(src)-checksumSize:]) != crc32.Checksum(src[:len(src)-checksumSize], crcTable) {
		return State{}, ErrCorrupt
	}
	state := State{BaseGeneration: binary.LittleEndian.Uint64(src[32:40]), SegmentSize: binary.LittleEndian.Uint32(src[40:44]), Covered: model.CommitSeq(binary.LittleEndian.Uint64(src[48:56]))}
	copy(state.StoreID[:], src[16:32])
	oldCount := binary.LittleEndian.Uint32(src[72:76])
	newCount := binary.LittleEndian.Uint32(src[92:96])
	want := uint64(headerSize) + uint64(oldCount)*refSize + uint64(newCount)*refSize + checksumSize
	if want != uint64(len(src)) {
		return State{}, ErrCorrupt
	}
	state.Old = decodeSetHeader(src[56:76], oldCount)
	state.New = decodeSetHeader(src[76:96], newCount)
	offset := headerSize
	readRefs := func(count uint32) []mapstore.SegmentRef {
		refs := make([]mapstore.SegmentRef, count)
		for i := range refs {
			refs[i] = mapstore.SegmentRef{SegmentID: model.MapSegmentID(binary.LittleEndian.Uint32(src[offset : offset+4])), ValidEnd: binary.LittleEndian.Uint32(src[offset+4 : offset+8])}
			offset += refSize
		}
		return refs
	}
	state.Old.Sealed = readRefs(oldCount)
	state.New.Sealed = readRefs(newCount)
	if err := validate(state); err != nil {
		return State{}, errors.Join(ErrCorrupt, err)
	}
	return state, nil
}

func encodeSetHeader(dst []byte, set FileSet) {
	binary.LittleEndian.PutUint32(dst[0:4], uint32(set.Active))
	binary.LittleEndian.PutUint32(dst[4:8], uint32(set.Next))
	binary.LittleEndian.PutUint64(dst[8:16], uint64(set.Root))
	binary.LittleEndian.PutUint32(dst[16:20], uint32(len(set.Sealed)))
}

func decodeSetHeader(src []byte, count uint32) FileSet {
	return FileSet{
		Active: model.MapSegmentID(binary.LittleEndian.Uint32(src[0:4])),
		Next:   model.MapSegmentID(binary.LittleEndian.Uint32(src[4:8])),
		Root:   model.MapAddr(binary.LittleEndian.Uint64(src[8:16])),
		Sealed: make([]mapstore.SegmentRef, 0, count),
	}
}

func Install(root string, state State, hook FaultHook) error {
	if root == "" {
		return ErrInvalid
	}
	encoded, err := Encode(state)
	if err != nil {
		return err
	}
	dir := filepath.Join(root, "journal")
	if err := ensureDir(root, dir); err != nil {
		return err
	}
	final, temp := filepath.Join(dir, markerName), filepath.Join(dir, tempName)
	if _, err := os.Lstat(final); err == nil {
		return ErrActive
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	removed, err := removeRegular(temp, hook, FaultBeforeTempRemove)
	if err != nil {
		return err
	}
	if removed {
		if err := hit(hook, FaultBeforeCleanupDirSync); err != nil {
			return err
		}
		if err := syncDir(dir); err != nil {
			return err
		}
	}
	if err := hit(hook, FaultBeforeTempCreate); err != nil {
		return err
	}
	file, err := os.OpenFile(temp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	fail := func(cause error) error { return errors.Join(cause, file.Close()) }
	if err := hit(hook, FaultBeforeWrite); err != nil {
		return fail(err)
	}
	if n, err := file.Write(encoded); err != nil || n != len(encoded) {
		if err == nil {
			err = io.ErrShortWrite
		}
		return fail(err)
	}
	if err := hit(hook, FaultBeforeFileSync); err != nil {
		return fail(err)
	}
	if err := file.Sync(); err != nil {
		return fail(err)
	}
	if err := hit(hook, FaultBeforeFileClose); err != nil {
		return fail(err)
	}
	if err := file.Close(); err != nil {
		return err
	}
	if err := hit(hook, FaultBeforePublishRename); err != nil {
		return err
	}
	if err := os.Rename(temp, final); err != nil {
		return err
	}
	if err := hit(hook, FaultBeforeJournalDirSync); err != nil {
		return err
	}
	return syncDir(dir)
}

func Load(root string) (State, bool, error) {
	return LoadWithFaultHook(root, nil)
}

func LoadWithFaultHook(root string, hook FaultHook) (State, bool, error) {
	if root == "" {
		return State{}, false, ErrInvalid
	}
	dir := filepath.Join(root, "journal")
	info, err := os.Lstat(dir)
	if errors.Is(err, os.ErrNotExist) {
		return State{}, false, nil
	}
	if err != nil {
		return State{}, false, err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return State{}, false, ErrCorrupt
	}
	_, err = removeRegular(filepath.Join(dir, tempName), hook, FaultBeforeTempRemove)
	if err != nil {
		return State{}, false, err
	}
	// Always sync: a previous process may have removed temp or final marker and
	// then observed an uncertain directory-sync result.
	if err := hit(hook, FaultBeforeCleanupDirSync); err != nil {
		return State{}, false, err
	}
	if err := syncDir(dir); err != nil {
		return State{}, false, err
	}
	path := filepath.Join(dir, markerName)
	info, err = os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return State{}, false, nil
	}
	if err != nil {
		return State{}, false, err
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Size() > maxEncodedSize {
		return State{}, false, ErrCorrupt
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return State{}, false, err
	}
	state, err := Decode(data)
	return state, err == nil, err
}

func Remove(root string, hook FaultHook) error {
	if root == "" {
		return ErrInvalid
	}
	dir := filepath.Join(root, "journal")
	info, err := os.Lstat(dir)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return ErrCorrupt
	}
	_, err = removeRegular(filepath.Join(dir, markerName), hook, FaultBeforeMarkerRemove)
	if err != nil {
		return err
	}
	_, err = removeRegular(filepath.Join(dir, tempName), hook, FaultBeforeTempRemove)
	if err != nil {
		return err
	}
	if err := hit(hook, FaultBeforeCleanupDirSync); err != nil {
		return err
	}
	return syncDir(dir)
}

func RecoveryArtifacts(root string) (bool, error) {
	for _, name := range []string{markerName, tempName} {
		if _, err := os.Lstat(filepath.Join(root, "journal", name)); err == nil {
			return true, nil
		} else if !errors.Is(err, os.ErrNotExist) {
			return false, err
		}
	}
	if _, err := os.Lstat(StagingRoot(root)); err == nil {
		return true, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return false, err
	}
	return false, nil
}

func StagingRoot(root string) string { return filepath.Join(root, StagingDirName) }

func validate(state State) error {
	if state.StoreID == ([16]byte{}) || state.BaseGeneration == 0 || state.SegmentSize < mapstore.SegmentHeaderSize+mapstore.DenseNodeSize+mapstore.SegmentFooterSize || state.SegmentSize%mapstore.Alignment != 0 ||
		!validFileSet(state.Old, state.SegmentSize) || !validFileSet(state.New, state.SegmentSize) {
		return ErrInvalid
	}
	first := state.New.Active
	if len(state.New.Sealed) != 0 {
		first = state.New.Sealed[0].SegmentID
	}
	if first != state.Old.Next || state.New.Root == 0 && len(state.New.Sealed) != 0 {
		return ErrInvalid
	}
	want := first
	for _, ref := range state.New.Sealed {
		if ref.SegmentID != want {
			return ErrInvalid
		}
		want++
	}
	if want != state.New.Active {
		return ErrInvalid
	}
	return nil
}

func validFileSet(set FileSet, segmentSize uint32) bool {
	if set.Active == 0 || set.Active == model.MapSegmentID(math.MaxUint32) || set.Next != set.Active+1 {
		return false
	}
	var previous model.MapSegmentID
	foundRoot := set.Root == 0
	for _, ref := range set.Sealed {
		if ref.SegmentID == 0 || ref.SegmentID <= previous || ref.SegmentID >= set.Active || ref.ValidEnd < mapstore.SegmentHeaderSize || ref.ValidEnd > segmentSize-mapstore.SegmentFooterSize || ref.ValidEnd%mapstore.Alignment != 0 {
			return false
		}
		if set.Root != 0 && set.Root.SegmentID() == ref.SegmentID && set.Root.Offset() < ref.ValidEnd {
			foundRoot = true
		}
		previous = ref.SegmentID
	}
	if set.Root != 0 && set.Root.Valid() && set.Root.SegmentID() == set.Active && set.Root.Offset() < segmentSize-mapstore.SegmentFooterSize {
		foundRoot = true
	}
	return foundRoot && (set.Root == 0 || set.Root.Valid())
}

func ensureDir(root, dir string) error {
	if err := os.Mkdir(dir, 0o700); err == nil {
		return syncDir(root)
	} else if !errors.Is(err, os.ErrExist) {
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

func removeRegular(path string, hook FaultHook, point FaultPoint) (bool, error) {
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
	if err := hit(hook, point); err != nil {
		return false, err
	}
	return true, os.Remove(path)
}

func hit(hook FaultHook, point FaultPoint) error {
	if hook == nil {
		return nil
	}
	return hook(point)
}

func syncDir(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	return errors.Join(dir.Sync(), dir.Close())
}

func zero(src []byte) bool {
	for _, value := range src {
		if value != 0 {
			return false
		}
	}
	return true
}

func Path(root string) string { return filepath.Join(root, "journal", markerName) }

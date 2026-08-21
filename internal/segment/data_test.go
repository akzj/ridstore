package segment

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/akzj/ridstore/internal/base"
	storeformat "github.com/akzj/ridstore/internal/format"
)

func createActiveDataFile(t *testing.T, root string, uuid base.StoreUUID, id base.DataSegmentID) {
	t.Helper()
	if err := os.Mkdir(filepath.Join(root, "data"), 0o700); err != nil {
		t.Fatal(err)
	}
	header, err := storeformat.EncodeSegmentHeader(storeformat.SegmentHeader{Kind: storeformat.SegmentKindData, StoreUUID: uuid, FileID: uint32(id), FirstSeq: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "data", ActiveDataFileName(id)), header[:], 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestActiveDataAppendReadAndReopen(t *testing.T) {
	root := t.TempDir()
	uuid := base.StoreUUID{1, 2, 3}
	createActiveDataFile(t, root, uuid, 1)
	segment, err := OpenActiveData(root, uuid, 1, 1<<20, 64<<10)
	if err != nil {
		t.Fatal(err)
	}
	want := storeformat.Frame{Type: storeformat.FrameTypePutRecord, FrameSeq: 1, BatchID: 2, RecordID: 3, Payload: []byte("value")}
	addr, size, err := segment.Append(want)
	if err != nil {
		t.Fatal(err)
	}
	if addr.Offset() != storeformat.SegmentHeaderSize || size != 72 || segment.End() != storeformat.SegmentHeaderSize+72 {
		t.Fatalf("addr=%x size=%d end=%d", addr, size, segment.End())
	}
	got, err := segment.ReadFrame(addr)
	if err != nil {
		t.Fatal(err)
	}
	if got.Type != want.Type || got.FrameSeq != want.FrameSeq || got.BatchID != want.BatchID || got.RecordID != want.RecordID || string(got.Payload) != "value" {
		t.Fatalf("frame=%+v", got)
	}
	if err := segment.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := segment.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenActiveData(root, uuid, 1, 1<<20, 64<<10)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	got, err = reopened.ReadFrame(addr)
	if err != nil || string(got.Payload) != "value" {
		t.Fatalf("reopened frame=%+v error=%v", got, err)
	}
}

func TestActiveDataCapacityAndAddressChecks(t *testing.T) {
	root := t.TempDir()
	uuid := base.StoreUUID{1}
	createActiveDataFile(t, root, uuid, 1)
	segmentSize := uint64(storeformat.SegmentHeaderSize + storeformat.SegmentFooterSize + storeformat.FrameHeaderSize)
	segment, err := OpenActiveData(root, uuid, 1, segmentSize, 1024)
	if err != nil {
		t.Fatal(err)
	}
	defer segment.Close()
	if _, _, err := segment.Append(storeformat.Frame{Type: storeformat.FrameTypePutRecord, FrameSeq: 1, BatchID: 1, RecordID: 1}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := segment.Append(storeformat.Frame{Type: storeformat.FrameTypePutRecord, FrameSeq: 2, BatchID: 1, RecordID: 2}); !errors.Is(err, ErrFull) {
		t.Fatalf("full error=%v", err)
	}
	other, _ := base.NewVAddr(2, storeformat.SegmentHeaderSize)
	if _, err := segment.ReadFrame(other); !errors.Is(err, base.ErrInvalidAddress) {
		t.Fatalf("address error=%v", err)
	}
}

func TestOpenActiveDataRejectsIdentityAndTruncatesIncompleteTail(t *testing.T) {
	root := t.TempDir()
	uuid := base.StoreUUID{1}
	createActiveDataFile(t, root, uuid, 1)
	if _, err := OpenActiveData(root, base.StoreUUID{2}, 1, 1<<20, 1024); !errors.Is(err, base.ErrCorrupt) {
		t.Fatalf("UUID error=%v", err)
	}
	path := filepath.Join(root, "data", ActiveDataFileName(1))
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.Write([]byte{1}); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	segment, err := OpenActiveData(root, uuid, 1, 1<<20, 1024)
	if err != nil {
		t.Fatalf("tail recovery error=%v", err)
	}
	defer segment.Close()
	if segment.End() != storeformat.SegmentHeaderSize {
		t.Fatalf("recovered end=%d", segment.End())
	}
}

func TestOpenActiveDataTruncatesIncompletePayloadButRejectsCompleteCorruption(t *testing.T) {
	root := t.TempDir()
	uuid := base.StoreUUID{1}
	createActiveDataFile(t, root, uuid, 1)
	path := filepath.Join(root, "data", ActiveDataFileName(1))
	encoded, err := storeformat.EncodeFrame(storeformat.Frame{Type: storeformat.FrameTypePutRecord, FrameSeq: 1, BatchID: 1, RecordID: 1, Payload: []byte("payload")}, 1024)
	if err != nil {
		t.Fatal(err)
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.Write(encoded[:len(encoded)-1]); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	segment, err := OpenActiveData(root, uuid, 1, 1<<20, 1024)
	if err != nil {
		t.Fatal(err)
	}
	if segment.End() != storeformat.SegmentHeaderSize {
		t.Fatalf("end=%d", segment.End())
	}
	if err := segment.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, append(mustHeader(t, uuid, 1), encoded...), 0o600); err != nil {
		t.Fatal(err)
	}
	corrupt, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := corrupt.WriteAt([]byte{0xff}, storeformat.SegmentHeaderSize+storeformat.FrameHeaderSize); err != nil {
		t.Fatal(err)
	}
	if err := corrupt.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenActiveData(root, uuid, 1, 1<<20, 1024); !errors.Is(err, base.ErrCorrupt) {
		t.Fatalf("complete corruption error=%v", err)
	}
}

func TestActiveDataSealCreatesStrictImmutableSegment(t *testing.T) {
	root := t.TempDir()
	uuid := base.StoreUUID{1}
	createActiveDataFile(t, root, uuid, 1)
	active, err := OpenActiveData(root, uuid, 1, 1<<20, 1024)
	if err != nil {
		t.Fatal(err)
	}
	addr, _, err := active.Append(storeformat.Frame{Type: storeformat.FrameTypePutRecord, FrameSeq: 1, BatchID: 1, RecordID: 1, Payload: []byte("value")})
	if err != nil {
		t.Fatal(err)
	}
	summary, err := active.Seal(2)
	if err != nil {
		t.Fatal(err)
	}
	if summary.FileID != 1 || summary.FirstSeq != 1 || summary.LastSeq != 2 {
		t.Fatalf("summary=%+v", summary)
	}
	sealed, err := OpenSealedData(root, uuid, summary, 1<<20, 1024)
	if err != nil {
		t.Fatal(err)
	}
	defer sealed.Close()
	frame, err := sealed.ReadFrame(addr)
	if err != nil || string(frame.Payload) != "value" {
		t.Fatalf("frame=%+v error=%v", frame, err)
	}
}

func TestResumeSealCompletesFooterAndRename(t *testing.T) {
	root := t.TempDir()
	uuid := base.StoreUUID{1}
	createActiveDataFile(t, root, uuid, 1)
	active, err := OpenActiveData(root, uuid, 1, 1<<20, 1024)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := active.Append(storeformat.Frame{Type: storeformat.FrameTypePutRecord, FrameSeq: 1, BatchID: 1, RecordID: 1}); err != nil {
		t.Fatal(err)
	}
	end := active.End()
	if err := active.Close(); err != nil {
		t.Fatal(err)
	}
	payload, err := storeformat.EncodeSegmentSealPayload(storeformat.SegmentSealPayload{
		SegmentID: 1, ValidDataEnd: end + storeformat.FrameHeaderSize + 64,
		FirstFrameSeq: 1, LastFrameSeq: 2, FrameCount: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := storeformat.EncodeFrame(storeformat.Frame{Type: storeformat.FrameTypeSegmentSeal, FrameSeq: 2, Payload: payload[:]}, 1024)
	if err != nil {
		t.Fatal(err)
	}
	file, err := os.OpenFile(filepath.Join(root, "data", ActiveDataFileName(1)), os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.Write(encoded); err != nil {
		t.Fatal(err)
	}
	if err := file.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	summary, err := ResumeSeal(root, uuid, 1, 1<<20, 1024, 1)
	if err != nil {
		t.Fatal(err)
	}
	if summary.LastSeq != 2 {
		t.Fatalf("summary=%+v", summary)
	}
	if err := ValidateSealedData(root, uuid, summary, 1<<20, 1024); err != nil {
		t.Fatal(err)
	}
}

func mustHeader(t *testing.T, uuid base.StoreUUID, id base.DataSegmentID) []byte {
	t.Helper()
	header, err := storeformat.EncodeSegmentHeader(storeformat.SegmentHeader{Kind: storeformat.SegmentKindData, StoreUUID: uuid, FileID: uint32(id), FirstSeq: 1})
	if err != nil {
		t.Fatal(err)
	}
	return append([]byte(nil), header[:]...)
}

package segment

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/akzj/ridstore/internal/base"
	storeformat "github.com/akzj/ridstore/internal/format"
)

func TestValidateSealedData(t *testing.T) {
	root := t.TempDir()
	uuid := base.StoreUUID{1, 2, 3}
	if err := os.Mkdir(filepath.Join(root, "data"), 0o700); err != nil {
		t.Fatal(err)
	}
	header := mustHeader(t, uuid, 1)
	put, err := storeformat.EncodeFrame(storeformat.Frame{Type: storeformat.FrameTypePutRecord, FrameSeq: 1, BatchID: 1, RecordID: 1, Payload: []byte("v")}, 1024)
	if err != nil {
		t.Fatal(err)
	}
	validEnd := uint64(len(header) + len(put) + storeformat.FrameHeaderSize + 64)
	sealPayload, err := storeformat.EncodeSegmentSealPayload(storeformat.SegmentSealPayload{
		SegmentID: 1, ValidDataEnd: validEnd, FirstFrameSeq: 1, LastFrameSeq: 2, FrameCount: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	seal, err := storeformat.EncodeFrame(storeformat.Frame{Type: storeformat.FrameTypeSegmentSeal, FrameSeq: 2, Payload: sealPayload[:]}, 1024)
	if err != nil {
		t.Fatal(err)
	}
	footer, err := storeformat.EncodeDataSegmentFooter(storeformat.DataSegmentFooter{
		SegmentID: 1, ValidDataEnd: validEnd, FirstFrameSeq: 1, LastFrameSeq: 2, FrameCount: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	data := append(header, put...)
	data = append(data, seal...)
	data = append(data, footer[:]...)
	path := filepath.Join(root, "data", SealedDataFileName(1))
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	summary := storeformat.FileSummary{FileID: 1, ValidEnd: validEnd, FirstSeq: 1, LastSeq: 2}
	if err := ValidateSealedData(root, uuid, summary, 1<<20, 1024); err != nil {
		t.Fatal(err)
	}
	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteAt([]byte{0xff}, int64(storeformat.SegmentHeaderSize+storeformat.FrameHeaderSize)); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	sealed, err := OpenSealedData(root, uuid, summary, 1<<20, 1024)
	if err != nil {
		t.Fatalf("lazy envelope open rejected middle payload corruption: %v", err)
	}
	addr, err := base.NewVAddr(1, storeformat.SegmentHeaderSize)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sealed.ReadFrame(addr); !errors.Is(err, base.ErrCorrupt) {
		t.Fatalf("lazy read corruption error=%v", err)
	}
	if err := sealed.Close(); err != nil {
		t.Fatal(err)
	}
	if err := ValidateSealedData(root, uuid, summary, 1<<20, 1024); !errors.Is(err, base.ErrCorrupt) {
		t.Fatalf("corruption error=%v", err)
	}
}

func TestActiveScanVisitsFramesInPhysicalOrder(t *testing.T) {
	root := t.TempDir()
	uuid := base.StoreUUID{1}
	createActiveDataFile(t, root, uuid, 1)
	segment, err := OpenActiveData(root, uuid, 1, 1<<20, 1024)
	if err != nil {
		t.Fatal(err)
	}
	defer segment.Close()
	for i := 1; i <= 3; i++ {
		if _, _, err := segment.Append(storeformat.Frame{Type: storeformat.FrameTypePutRecord, FrameSeq: base.FrameSeq(i), BatchID: 1, RecordID: base.ID(i)}); err != nil {
			t.Fatal(err)
		}
	}
	var sequences []base.FrameSeq
	if err := segment.Scan(func(_ base.VAddr, frame storeformat.Frame) error {
		sequences = append(sequences, frame.FrameSeq)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(sequences) != 3 || sequences[0] != 1 || sequences[2] != 3 {
		t.Fatalf("sequences=%v", sequences)
	}
}

func TestInspectActiveDataIsReadOnlyAndStrict(t *testing.T) {
	root := t.TempDir()
	uuid := base.StoreUUID{1}
	createActiveDataFile(t, root, uuid, 1)
	active, err := OpenActiveData(root, uuid, 1, 1<<20, 1024)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := active.Append(storeformat.Frame{Type: storeformat.FrameTypePutRecord, FrameSeq: 1, BatchID: 1, RecordID: 1, Payload: []byte("value")}); err != nil {
		t.Fatal(err)
	}
	if err := active.Close(); err != nil {
		t.Fatal(err)
	}
	visited := 0
	if err := InspectActiveData(root, uuid, 1, 1<<20, 1024, func(addr base.VAddr, frame storeformat.Frame, physical uint64) error {
		visited++
		if addr.SegmentID() != 1 || frame.RecordID != 1 || physical == 0 {
			t.Fatalf("addr=%x frame=%+v physical=%d", addr, frame, physical)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if visited != 1 {
		t.Fatalf("visited=%d", visited)
	}
	path := filepath.Join(root, "data", ActiveDataFileName(1))
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.Write([]byte("partial")); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	before, _ := os.Stat(path)
	if err := InspectActiveData(root, uuid, 1, 1<<20, 1024, nil); !errors.Is(err, base.ErrCorrupt) {
		t.Fatalf("error=%v", err)
	}
	after, _ := os.Stat(path)
	if before.Size() != after.Size() {
		t.Fatalf("inspect changed size from %d to %d", before.Size(), after.Size())
	}
}

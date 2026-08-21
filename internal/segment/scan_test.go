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

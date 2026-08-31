package recordlog

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestVerifyFilesAcceptsCompleteLiveSet(t *testing.T) {
	root := t.TempDir()
	snapshot := initialCatalog(1024, 512)
	if err := CreateInitialSegment(root, snapshot.LogID, snapshot.SegmentSize); err != nil {
		t.Fatal(err)
	}
	report, err := VerifyFiles(context.Background(), root, snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if report.Segments != 1 || report.SealedSegments != 0 || report.Records != 0 || report.PhysicalBytes != uint64(SegmentHeaderSize) || report.ActiveEnd != (LogPos{SegmentID: 1, Offset: SegmentHeaderSize}) {
		t.Fatalf("report=%+v", report)
	}
}

func TestVerifyFilesScansSealedPayloads(t *testing.T) {
	root := t.TempDir()
	snapshot := initialCatalog(512, 256)
	if err := CreateInitialSegment(root, snapshot.LogID, snapshot.SegmentSize); err != nil {
		t.Fatal(err)
	}
	catalog := &memoryCatalog{state: snapshot}
	log, err := Open(root, testLogConfig(), catalog)
	if err != nil {
		t.Fatal(err)
	}
	first, err := log.Append(context.Background(), make([]byte, 200), false)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := log.Append(context.Background(), make([]byte, 200), true); err != nil {
		t.Fatal(err)
	}
	if err := log.Close(); err != nil {
		t.Fatal(err)
	}
	snapshot = catalog.SnapshotRecordLog()
	report, err := VerifyFiles(context.Background(), root, snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if report.Segments != 2 || report.SealedSegments != 1 || report.Records != 2 {
		t.Fatalf("report=%+v", report)
	}

	path := filepath.Join(recordsPath(root), sealedSegmentName(first.Addr.SegmentID()))
	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	offset := int64(first.Addr.Offset() + RecordHeaderSize)
	if _, err := file.WriteAt([]byte{0xff}, offset); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := VerifyFiles(context.Background(), root, snapshot); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("corrupt verify err=%v", err)
	}
}

func TestOpenVerifiedReaderScansAndReadsAcrossSegments(t *testing.T) {
	root := t.TempDir()
	snapshot := initialCatalog(512, 256)
	if err := CreateInitialSegment(root, snapshot.LogID, snapshot.SegmentSize); err != nil {
		t.Fatal(err)
	}
	catalog := &memoryCatalog{state: snapshot}
	log, err := Open(root, testLogConfig(), catalog)
	if err != nil {
		t.Fatal(err)
	}
	firstPayload := make([]byte, 200)
	firstPayload[0] = 1
	first, err := log.Append(context.Background(), firstPayload, false)
	if err != nil {
		t.Fatal(err)
	}
	secondPayload := make([]byte, 200)
	secondPayload[0] = 2
	second, err := log.Append(context.Background(), secondPayload, true)
	if err != nil {
		t.Fatal(err)
	}
	if err := log.Close(); err != nil {
		t.Fatal(err)
	}

	reader, report, err := OpenVerifiedReader(context.Background(), root, catalog.SnapshotRecordLog())
	if err != nil {
		t.Fatal(err)
	}
	if report.Records != 2 {
		t.Fatalf("report=%+v", report)
	}
	got, err := reader.Read(context.Background(), first.Addr)
	if err != nil || len(got) != len(firstPayload) || got[0] != 1 {
		t.Fatalf("read len=%d first=%d err=%v", len(got), got[0], err)
	}
	var addresses []VAddr
	if err := reader.Scan(context.Background(), first.End, func(result AppendResult, payload []byte) error {
		addresses = append(addresses, result.Addr)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(addresses) != 1 || addresses[0] != second.Addr {
		t.Fatalf("addresses=%v want=%v", addresses, second.Addr)
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := reader.Read(context.Background(), first.Addr); !errors.Is(err, ErrClosed) {
		t.Fatalf("closed read err=%v", err)
	}
}

func TestVerifyFilesRejectsSealedSummaryThatDoesNotMatchRecords(t *testing.T) {
	root := t.TempDir()
	snapshot := initialCatalog(512, 256)
	if err := CreateInitialSegment(root, snapshot.LogID, snapshot.SegmentSize); err != nil {
		t.Fatal(err)
	}
	catalog := &memoryCatalog{state: snapshot}
	log, err := Open(root, testLogConfig(), catalog)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := log.Append(context.Background(), make([]byte, 200), false); err != nil {
		t.Fatal(err)
	}
	if _, err := log.Append(context.Background(), make([]byte, 200), true); err != nil {
		t.Fatal(err)
	}
	if err := log.Close(); err != nil {
		t.Fatal(err)
	}
	snapshot = catalog.SnapshotRecordLog()
	snapshot.SealedSegments[0].RecordCount++
	summary := snapshot.SealedSegments[0]
	footer, err := EncodeSegmentFooter(SegmentFooter{
		SegmentID: summary.SegmentID, DataEnd: summary.ValidEnd, FirstAddr: summary.FirstAddr,
		LastAddr: summary.LastAddr, RecordCount: summary.RecordCount,
	})
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(recordsPath(root), sealedSegmentName(summary.SegmentID))
	file, err := os.OpenFile(path, os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteAt(footer[:], int64(summary.ValidEnd)); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := VerifyFiles(context.Background(), root, snapshot); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("verify err=%v", err)
	}
}

func TestVerifyFilesRejectsPartialTailWithoutRepair(t *testing.T) {
	root := t.TempDir()
	snapshot := initialCatalog(1024, 512)
	if err := CreateInitialSegment(root, snapshot.LogID, snapshot.SegmentSize); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(recordsPath(root), activeSegmentName(1))
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.Write([]byte{1, 2, 3}); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	before, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := VerifyFiles(context.Background(), root, snapshot); !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("verify err=%v", err)
	}
	after, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if after.Size() != before.Size() {
		t.Fatalf("verify changed active size from %d to %d", before.Size(), after.Size())
	}
}

func TestVerifyFilesRejectsUnexpectedAndRecoveryFiles(t *testing.T) {
	for _, test := range []struct {
		name string
		file string
		want error
	}{
		{name: "unknown", file: "unknown", want: ErrCorrupt},
		{name: "creating", file: creatingSegmentName(2), want: ErrRecoveryRequired},
	} {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			snapshot := initialCatalog(1024, 512)
			if err := CreateInitialSegment(root, snapshot.LogID, snapshot.SegmentSize); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(recordsPath(root), test.file), nil, 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := VerifyFiles(context.Background(), root, snapshot); !errors.Is(err, test.want) {
				t.Fatalf("verify err=%v", err)
			}
		})
	}
}

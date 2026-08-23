package segment

import (
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"syscall"
	"testing"

	"github.com/akzj/ridstore/internal/base"
	"github.com/akzj/ridstore/internal/failpoint"
	storeformat "github.com/akzj/ridstore/internal/format"
)

func TestActiveDataAppendBatchWritesContiguousFrames(t *testing.T) {
	root := t.TempDir()
	uuid := base.StoreUUID{1}
	createActiveDataFile(t, root, uuid, 1)
	active, err := OpenActiveData(root, uuid, 1, 1<<20, 1024)
	if err != nil {
		t.Fatal(err)
	}
	defer active.Close()
	var writes atomic.Int32
	active.SetHook(failpoint.Func(func(point failpoint.Point) error {
		if point == PointBeforeAppendWrite {
			writes.Add(1)
		}
		return nil
	}))
	frames := []storeformat.Frame{
		{Type: storeformat.FrameTypePutRecord, FrameSeq: 1, BatchID: 1, RecordID: 1, Payload: []byte("one")},
		{Type: storeformat.FrameTypePutRecord, FrameSeq: 2, BatchID: 1, RecordID: 2, Payload: []byte("longer-value")},
		{Type: storeformat.FrameTypePutRecord, FrameSeq: 3, BatchID: 1, RecordID: 3},
	}
	results, written, err := active.AppendBatch(frames)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != len(frames) || writes.Load() != 1 {
		t.Fatalf("results=%d append writes=%d", len(results), writes.Load())
	}
	var sum uint64
	for i, result := range results {
		if i > 0 && result.Addr.Offset() != results[i-1].Addr.Offset()+uint32(results[i-1].Size) {
			t.Fatalf("non-contiguous result %d: %+v after %+v", i, result, results[i-1])
		}
		got, err := active.ReadFrame(result.Addr)
		if err != nil || got.FrameSeq != frames[i].FrameSeq || got.RecordID != frames[i].RecordID || string(got.Payload) != string(frames[i].Payload) {
			t.Fatalf("frame %d=%+v error=%v", i, got, err)
		}
		sum += result.Size
	}
	if written != sum || active.End() != storeformat.SegmentHeaderSize+sum {
		t.Fatalf("written=%d sum=%d end=%d", written, sum, active.End())
	}
}

func TestActiveDataAppendBatchPreflightAndPoison(t *testing.T) {
	root := t.TempDir()
	uuid := base.StoreUUID{1}
	createActiveDataFile(t, root, uuid, 1)
	segmentSize := uint64(storeformat.SegmentHeaderSize + storeformat.SegmentFooterSize + storeformat.FrameHeaderSize)
	active, err := OpenActiveData(root, uuid, 1, segmentSize, 1024)
	if err != nil {
		t.Fatal(err)
	}
	defer active.Close()
	before := active.End()
	if _, _, err := active.AppendBatch(nil); !errors.Is(err, base.ErrInvalidConfig) {
		t.Fatalf("empty group error=%v", err)
	}
	frames := []storeformat.Frame{
		{Type: storeformat.FrameTypePutRecord, FrameSeq: 1, BatchID: 1, RecordID: 1},
		{Type: storeformat.FrameTypePutRecord, FrameSeq: 2, BatchID: 1, RecordID: 2},
	}
	if _, _, err := active.AppendBatch(frames); !errors.Is(err, ErrFull) {
		t.Fatalf("oversize group error=%v", err)
	}
	if active.End() != before {
		t.Fatalf("preflight changed end from %d to %d", before, active.End())
	}
	injected := errors.New("injected append failure")
	active.SetHook(failpoint.Func(func(point failpoint.Point) error {
		if point == PointBeforeAppendWrite {
			return injected
		}
		return nil
	}))
	if _, _, err := active.AppendBatch(frames[:1]); !errors.Is(err, injected) {
		t.Fatalf("append error=%v", err)
	}
	if _, _, err := active.AppendBatch(frames[:1]); !errors.Is(err, ErrPoisoned) {
		t.Fatalf("append after failure=%v", err)
	}
}

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

func TestActiveDataSyscallErrorsPoisonAppendAndSync(t *testing.T) {
	for _, point := range []failpoint.Point{PointBeforeAppendWrite, PointBeforeSync} {
		point := point
		for _, injected := range []struct {
			name string
			err  error
		}{{"EIO", syscall.EIO}, {"ENOSPC", syscall.ENOSPC}, {"EACCES", syscall.EACCES}} {
			injected := injected
			t.Run(string(point)+"/"+injected.name, func(t *testing.T) {
				root := t.TempDir()
				uuid := base.StoreUUID{1}
				createActiveDataFile(t, root, uuid, 1)
				active, err := OpenActiveData(root, uuid, 1, 1<<20, 1024)
				if err != nil {
					t.Fatal(err)
				}
				defer active.Close()
				if point == PointBeforeSync {
					if _, _, err := active.Append(storeformat.Frame{Type: storeformat.FrameTypePutRecord, FrameSeq: 1, BatchID: 1, RecordID: 1}); err != nil {
						t.Fatal(err)
					}
				}
				active.SetHook(failpoint.Func(func(got failpoint.Point) error {
					if got == point {
						return injected.err
					}
					return nil
				}))
				if point == PointBeforeAppendWrite {
					_, _, err = active.Append(storeformat.Frame{Type: storeformat.FrameTypePutRecord, FrameSeq: 1, BatchID: 1, RecordID: 1})
				} else {
					err = active.Sync()
				}
				if !errors.Is(err, injected.err) {
					t.Fatalf("operation error=%v", err)
				}
				if _, _, err := active.Append(storeformat.Frame{Type: storeformat.FrameTypePutRecord, FrameSeq: 2, BatchID: 1, RecordID: 2}); !errors.Is(err, ErrPoisoned) {
					t.Fatalf("append after syscall error=%v", err)
				}
			})
		}
	}
}

func TestActiveDataSealSyscallErrorsResumeToValidImmutableSegment(t *testing.T) {
	points := []failpoint.Point{
		PointBeforeSealWrite, PointBeforeSealSync, PointBeforeFooterWrite, PointBeforeFooterSync,
		PointBeforeSealRename, PointBeforeDataDirSync,
	}
	for _, point := range points {
		point := point
		for _, injected := range []struct {
			name string
			err  error
		}{{"EIO", syscall.EIO}, {"ENOSPC", syscall.ENOSPC}, {"EACCES", syscall.EACCES}} {
			injected := injected
			t.Run(string(point)+"/"+injected.name, func(t *testing.T) {
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
				active.SetHook(failpoint.Func(func(got failpoint.Point) error {
					if got == point {
						return injected.err
					}
					return nil
				}))
				if _, err := active.Seal(2); !errors.Is(err, injected.err) {
					t.Fatalf("Seal error=%v", err)
				}
				_ = active.Close()

				sealedPath := filepath.Join(root, "data", SealedDataFileName(1))
				var summary storeformat.FileSummary
				var recoveryErr error
				if _, statErr := os.Lstat(sealedPath); statErr == nil {
					summary, recoveryErr = LoadSealedDataSummary(root, 1)
				} else if errors.Is(statErr, os.ErrNotExist) {
					summary, recoveryErr = ResumeSeal(root, uuid, 1, 1<<20, 1024, 2)
				} else {
					recoveryErr = statErr
				}
				if recoveryErr != nil {
					t.Fatal(recoveryErr)
				}
				if err := ValidateSealedData(root, uuid, summary, 1<<20, 1024); err != nil {
					t.Fatalf("recovered sealed data: %v", err)
				}
			})
		}
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

func TestOpenActiveDataTailRepairSyscallErrorsAreRetryable(t *testing.T) {
	points := []failpoint.Point{PointBeforeTailTruncate, PointBeforeTailSync}
	causes := []struct {
		name string
		err  error
	}{{"EIO", syscall.EIO}, {"ENOSPC", syscall.ENOSPC}, {"EACCES", syscall.EACCES}}
	for _, point := range points {
		point := point
		for _, cause := range causes {
			cause := cause
			t.Run(string(point)+"/"+cause.name, func(t *testing.T) {
				root := t.TempDir()
				uuid := base.StoreUUID{1}
				createActiveDataFile(t, root, uuid, 1)
				path := filepath.Join(root, "data", ActiveDataFileName(1))
				file, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := file.Write([]byte("incomplete-tail")); err != nil {
					t.Fatal(err)
				}
				if err := file.Sync(); err != nil {
					t.Fatal(err)
				}
				if err := file.Close(); err != nil {
					t.Fatal(err)
				}
				before, err := os.Stat(path)
				if err != nil {
					t.Fatal(err)
				}
				hook := failpoint.Func(func(got failpoint.Point) error {
					if got == point {
						return cause.err
					}
					return nil
				})
				if _, err := OpenActiveDataWithHook(root, uuid, 1, 1<<20, 1024, hook); !errors.Is(err, cause.err) {
					t.Fatalf("Open error=%v", err)
				}
				after, err := os.Stat(path)
				if err != nil {
					t.Fatal(err)
				}
				if point == PointBeforeTailTruncate && after.Size() != before.Size() {
					t.Fatalf("truncate failpoint changed size from %d to %d", before.Size(), after.Size())
				}
				recovered, err := OpenActiveData(root, uuid, 1, 1<<20, 1024)
				if err != nil {
					t.Fatalf("retry Open: %v", err)
				}
				if recovered.End() != storeformat.SegmentHeaderSize {
					t.Fatalf("recovered end=%d", recovered.End())
				}
				if err := recovered.Close(); err != nil {
					t.Fatal(err)
				}
			})
		}
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

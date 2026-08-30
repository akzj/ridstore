package recordlog

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func testSegmentHeader(id, previous SegmentID) SegmentHeader {
	return SegmentHeader{
		LogID:           LogID{1, 2, 3, 4},
		SegmentID:       id,
		PreviousSegment: previous,
		SegmentSize:     16 << 10,
	}
}

func appendTestRecords(t *testing.T, segment *activeSegment, payloads ...[]byte) []AppendResult {
	t.Helper()
	encoded := make([]byte, 0)
	extents := make([]recordExtent, 0, len(payloads))
	results := make([]AppendResult, 0, len(payloads))
	offset := segment.summary().ValidEnd
	for _, payload := range payloads {
		size, err := PhysicalRecordSize(uint64(len(payload)))
		if err != nil {
			t.Fatal(err)
		}
		addr, err := NewVAddr(segment.header.SegmentID, offset, size)
		if err != nil {
			t.Fatal(err)
		}
		value, err := EncodeRecord(addr, payload)
		if err != nil {
			t.Fatal(err)
		}
		result, err := NewAppendResult(addr, size)
		if err != nil {
			t.Fatal(err)
		}
		encoded = append(encoded, value...)
		extents = append(extents, recordExtent{Result: result, Size: size})
		results = append(results, result)
		offset += size
	}
	if written, err := segment.appendEncoded(encoded, extents); err != nil || written != len(encoded) {
		t.Fatalf("append: written=%d err=%v", written, err)
	}
	return results
}

type blockingReadFile struct {
	fileHandle
	reached chan struct{}
	release chan struct{}
	once    sync.Once
}

func (f *blockingReadFile) ReadAt(value []byte, offset int64) (int, error) {
	f.once.Do(func() {
		close(f.reached)
		<-f.release
	})
	return f.fileHandle.ReadAt(value, offset)
}

func TestActiveSegmentAppendDoesNotWaitForReadIO(t *testing.T) {
	root := t.TempDir()
	segment, err := createActiveSegment(root, testSegmentHeader(1, 0), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer segment.close()
	first := appendTestRecords(t, segment, []byte("first"))[0]
	reached := make(chan struct{})
	release := make(chan struct{})
	released := false
	defer func() {
		if !released {
			close(release)
		}
	}()
	segment.mu.Lock()
	segment.file = &blockingReadFile{fileHandle: segment.file, reached: reached, release: release}
	segment.mu.Unlock()

	readDone := make(chan error, 1)
	go func() {
		value, readErr := segment.read(first.Addr)
		if readErr == nil && string(value) != "first" {
			readErr = errors.New("wrong payload")
		}
		readDone <- readErr
	}()
	select {
	case <-reached:
	case <-time.After(time.Second):
		t.Fatal("read did not reach blocked I/O")
	}

	appendDone := make(chan error, 1)
	go func() {
		appendTestRecordsWithoutFatal(segment, []byte("second"), appendDone)
	}()
	select {
	case err := <-appendDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("append completion blocked behind existing record read I/O")
	}
	close(release)
	released = true
	if err := <-readDone; err != nil {
		t.Fatal(err)
	}
}

func appendTestRecordsWithoutFatal(segment *activeSegment, payload []byte, done chan<- error) {
	physical, err := PhysicalRecordSize(uint64(len(payload)))
	if err != nil {
		done <- err
		return
	}
	addr, err := NewVAddr(segment.header.SegmentID, segment.summary().ValidEnd, physical)
	if err != nil {
		done <- err
		return
	}
	encoded, err := EncodeRecord(addr, payload)
	if err != nil {
		done <- err
		return
	}
	result, err := NewAppendResult(addr, physical)
	if err != nil {
		done <- err
		return
	}
	_, err = segment.appendEncoded(encoded, []recordExtent{{Result: result, Size: physical}})
	done <- err
}

func TestActiveSegmentReadDoesNotWaitForAppendIO(t *testing.T) {
	root := t.TempDir()
	segment, err := createActiveSegment(root, testSegmentHeader(1, 0), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer segment.close()
	first := appendTestRecords(t, segment, []byte("first"))[0]

	reached := make(chan struct{})
	release := make(chan struct{})
	released := false
	defer func() {
		if !released {
			close(release)
		}
	}()
	var once sync.Once
	segment.hook = func(point segmentFaultPoint) error {
		if point == faultBeforeAppendWrite {
			once.Do(func() {
				close(reached)
				<-release
			})
		}
		return nil
	}
	payload := []byte("second")
	physical, err := PhysicalRecordSize(uint64(len(payload)))
	if err != nil {
		t.Fatal(err)
	}
	secondAddr, err := NewVAddr(1, segment.summary().ValidEnd, physical)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := EncodeRecord(secondAddr, payload)
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewAppendResult(secondAddr, physical)
	if err != nil {
		t.Fatal(err)
	}
	appendDone := make(chan error, 1)
	go func() {
		_, appendErr := segment.appendEncoded(encoded, []recordExtent{{Result: second, Size: physical}})
		appendDone <- appendErr
	}()
	select {
	case <-reached:
	case <-time.After(time.Second):
		t.Fatal("append did not reach blocked I/O")
	}
	readDone := make(chan error, 1)
	go func() {
		value, readErr := segment.read(first.Addr)
		if readErr == nil && string(value) != "first" {
			readErr = errors.New("wrong payload")
		}
		readDone <- readErr
	}()
	select {
	case err := <-readDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("existing record read blocked behind append I/O")
	}
	close(release)
	released = true
	if err := <-appendDone; err != nil {
		t.Fatal(err)
	}
}

func TestActiveSegmentReadDoesNotWaitForSealIO(t *testing.T) {
	root := t.TempDir()
	segment, err := createActiveSegment(root, testSegmentHeader(1, 0), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	first := appendTestRecords(t, segment, []byte("first"))[0]
	reached := make(chan struct{})
	release := make(chan struct{})
	released := false
	defer func() {
		if !released {
			close(release)
		}
	}()
	var once sync.Once
	segment.hook = func(point segmentFaultPoint) error {
		if point == faultBeforeFooterSync {
			once.Do(func() {
				close(reached)
				<-release
			})
		}
		return nil
	}
	type sealResult struct {
		sealed *sealedSegment
		err    error
	}
	sealDone := make(chan sealResult, 1)
	go func() {
		sealed, _, sealErr := segment.seal()
		sealDone <- sealResult{sealed: sealed, err: sealErr}
	}()
	select {
	case <-reached:
	case <-time.After(time.Second):
		t.Fatal("seal did not reach blocked I/O")
	}
	readDone := make(chan error, 1)
	go func() {
		value, readErr := segment.read(first.Addr)
		if readErr == nil && string(value) != "first" {
			readErr = errors.New("wrong payload")
		}
		readDone <- readErr
	}()
	select {
	case err := <-readDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("existing record read blocked behind seal I/O")
	}
	close(release)
	released = true
	result := <-sealDone
	if result.err != nil {
		t.Fatal(result.err)
	}
	if err := segment.transferSealedOwnership(result.sealed); err != nil {
		t.Fatal(err)
	}
	if err := segment.close(); err != nil {
		t.Fatal(err)
	}
	if err := result.sealed.close(); err != nil {
		t.Fatal(err)
	}
}

func TestActiveSegmentRecoversOnlyIncompleteTail(t *testing.T) {
	root := t.TempDir()
	header := testSegmentHeader(1, 0)
	segment, err := createActiveSegment(root, header, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	results := appendTestRecords(t, segment, []byte("first"), make([]byte, 300))
	wantEnd := segment.summary().ValidEnd

	thirdSize, err := PhysicalRecordSize(200)
	if err != nil {
		t.Fatal(err)
	}
	thirdAddr, err := NewVAddr(1, wantEnd, thirdSize)
	if err != nil {
		t.Fatal(err)
	}
	partial, err := EncodeRecord(thirdAddr, make([]byte, 200))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := writeFullAt(segment.file, partial[:RecordHeaderSize+17], int64(wantEnd)); err != nil {
		t.Fatal(err)
	}
	if err := segment.close(); err != nil {
		t.Fatal(err)
	}

	recovered, repaired, err := openActiveSegment(root, header, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer recovered.close()
	if !repaired || recovered.summary().ValidEnd != wantEnd {
		t.Fatalf("repair=%v summary=%+v", repaired, recovered.summary())
	}
	info, err := os.Stat(filepath.Join(recordsPath(root), activeSegmentName(1)))
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() != int64(wantEnd) {
		t.Fatalf("file size=%d want=%d", info.Size(), wantEnd)
	}
	for i, result := range results {
		payload, err := recovered.read(result.Addr)
		if err != nil {
			t.Fatalf("read %d: %v", i, err)
		}
		if i == 0 && string(payload) != "first" {
			t.Fatalf("payload=%q", payload)
		}
	}
}

func TestActiveSegmentRejectsCompleteCorruptRecord(t *testing.T) {
	root := t.TempDir()
	header := testSegmentHeader(1, 0)
	segment, err := createActiveSegment(root, header, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	result := appendTestRecords(t, segment, []byte("durable"))[0]
	if err := segment.sync(); err != nil {
		t.Fatal(err)
	}
	if _, err := segment.file.WriteAt([]byte{0xff}, int64(result.Addr.Offset()+RecordHeaderSize)); err != nil {
		t.Fatal(err)
	}
	if err := segment.close(); err != nil {
		t.Fatal(err)
	}
	_, _, err = openActiveSegment(root, header, nil, nil)
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("error=%v", err)
	}
}

func TestSealAndOpenRequireExactSummary(t *testing.T) {
	root := t.TempDir()
	header := testSegmentHeader(4, 3)
	active, err := createActiveSegment(root, header, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	results := appendTestRecords(t, active, []byte("small"), make([]byte, 5000))
	sealed, summary, err := active.seal()
	if err != nil {
		t.Fatal(err)
	}
	if err := sealed.close(); err != nil {
		t.Fatal(err)
	}

	opened, err := openSealedSegment(root, header, summary, nil)
	if err != nil {
		t.Fatal(err)
	}
	for i, result := range results {
		payload, err := opened.read(result.Addr)
		if err != nil {
			t.Fatalf("read %d: %v", i, err)
		}
		if i == 0 && string(payload) != "small" {
			t.Fatalf("payload=%q", payload)
		}
	}
	var scanned []string
	if err := opened.scan(SegmentHeaderSize, func(_ AppendResult, payload []byte) error {
		scanned = append(scanned, string(payload))
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(scanned) != 2 || scanned[0] != "small" || len(scanned[1]) != 5000 {
		t.Fatalf("scanned payloads=%d", len(scanned))
	}
	if err := opened.close(); err != nil {
		t.Fatal(err)
	}

	wrong := summary
	wrong.RecordCount++
	if _, err := openSealedSegment(root, header, wrong, nil); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("summary mismatch error=%v", err)
	}
}

func TestSegmentFaultBoundaries(t *testing.T) {
	injected := errors.New("injected")
	for _, point := range []segmentFaultPoint{
		faultBeforeHeaderWrite,
		faultBeforeHeaderSync,
		faultBeforeCreateRename,
		faultBeforeCreateDirSync,
	} {
		t.Run(string(point), func(t *testing.T) {
			root := t.TempDir()
			_, err := createActiveSegment(root, testSegmentHeader(1, 0), nil, func(got segmentFaultPoint) error {
				if got == point {
					return injected
				}
				return nil
			})
			if !errors.Is(err, injected) {
				t.Fatalf("error=%v", err)
			}
		})
	}
}

type stepWriterAt struct {
	data  []byte
	steps []int
}

func (w *stepWriterAt) WriteAt(value []byte, offset int64) (int, error) {
	if len(w.steps) == 0 {
		return 0, nil
	}
	n := w.steps[0]
	w.steps = w.steps[1:]
	if n > len(value) {
		n = len(value)
	}
	copy(w.data[int(offset):], value[:n])
	return n, nil
}

func TestWriteFullAtHandlesPartialAndZeroWrites(t *testing.T) {
	writer := &stepWriterAt{data: make([]byte, 6), steps: []int{2, 1, 3}}
	if n, err := writeFullAt(writer, []byte("abcdef"), 0); err != nil || n != 6 || string(writer.data) != "abcdef" {
		t.Fatalf("n=%d data=%q err=%v", n, writer.data, err)
	}
	writer = &stepWriterAt{data: make([]byte, 2), steps: []int{0}}
	if n, err := writeFullAt(writer, []byte("ab"), 0); n != 0 || !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("n=%d err=%v", n, err)
	}
}

func TestRegistryRetirementWaitsForReaderPins(t *testing.T) {
	root := t.TempDir()
	firstHeader := testSegmentHeader(1, 0)
	first, err := createActiveSegment(root, firstHeader, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	result := appendTestRecords(t, first, []byte("value"))[0]
	sealed, _, err := first.seal()
	if err != nil {
		t.Fatal(err)
	}
	active, err := createActiveSegment(root, testSegmentHeader(2, 1), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	registry, err := newSegmentRegistry(active, []*sealedSegment{sealed})
	if err != nil {
		t.Fatal(err)
	}

	pin, err := registry.pin(1)
	if err != nil {
		t.Fatal(err)
	}
	if payload, err := pin.read(result.Addr); err != nil || string(payload) != "value" {
		t.Fatalf("payload=%q err=%v", payload, err)
	}
	if err := registry.beginRetire(1); err != nil {
		t.Fatal(err)
	}
	if _, err := registry.pin(1); !errors.Is(err, ErrSegmentRetiring) {
		t.Fatalf("pin retiring error=%v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := registry.waitNoReaders(ctx, 1); !errors.Is(err, context.Canceled) {
		t.Fatalf("wait error=%v", err)
	}
	if _, err := registry.detachRetired(1); !errors.Is(err, ErrReadersActive) {
		t.Fatalf("detach error=%v", err)
	}
	if err := pin.release(); err != nil {
		t.Fatal(err)
	}
	if err := registry.waitNoReaders(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	detached, err := registry.detachRetired(1)
	if err != nil {
		t.Fatal(err)
	}
	if err := detached.close(); err != nil {
		t.Fatal(err)
	}
	if err := registry.close(); err != nil {
		t.Fatal(err)
	}
}

func TestRegistryRotationPreservesExistingPin(t *testing.T) {
	root := t.TempDir()
	old, err := createActiveSegment(root, testSegmentHeader(1, 0), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	result := appendTestRecords(t, old, []byte("before rotation"))[0]
	registry, err := newSegmentRegistry(old, nil)
	if err != nil {
		t.Fatal(err)
	}
	pin, err := registry.pin(1)
	if err != nil {
		t.Fatal(err)
	}
	sealed, _, err := old.seal()
	if err != nil {
		t.Fatal(err)
	}
	active, err := createActiveSegment(root, testSegmentHeader(2, 1), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.publishRotation(1, sealed, active); err != nil {
		t.Fatal(err)
	}
	if payload, err := pin.read(result.Addr); err != nil || string(payload) != "before rotation" {
		t.Fatalf("payload=%q err=%v", payload, err)
	}
	if err := pin.release(); err != nil {
		t.Fatal(err)
	}
	newPin, err := registry.pin(1)
	if err != nil {
		t.Fatal(err)
	}
	if err := newPin.release(); err != nil {
		t.Fatal(err)
	}
	if err := registry.close(); err != nil {
		t.Fatal(err)
	}
}

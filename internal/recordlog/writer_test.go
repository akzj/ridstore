package recordlog

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func testLogConfig() Config {
	return Config{
		MaxQueuedBytes: 1 << 20,
		QueueCapacity:  128,
		BufferBytes:    64 << 10,
		BufferRecords:  128,
	}
}

func testRotate(root string, registry *segmentRegistry) rotateActive {
	return func(old *activeSegment) (*activeSegment, error) {
		sealed, _, err := old.seal()
		if err != nil {
			return nil, err
		}
		nextHeader := old.header
		nextHeader.PreviousSegment = old.header.SegmentID
		nextHeader.SegmentID++
		next, err := createActiveSegment(root, nextHeader, nil, nil)
		if err != nil {
			return nil, err
		}
		if err := registry.publishRotation(old.header.SegmentID, sealed, next); err != nil {
			return nil, err
		}
		return next, nil
	}
}

func openTestLog(t *testing.T, header SegmentHeader, hook segmentFaultHook) (*Log, string) {
	t.Helper()
	root := t.TempDir()
	active, err := createActiveSegment(root, header, nil, hook)
	if err != nil {
		t.Fatal(err)
	}
	registry, err := newSegmentRegistry(active, nil)
	if err != nil {
		t.Fatal(err)
	}
	log, err := newLog(testLogConfig(), 8<<10, active, registry, testRotate(root, registry))
	if err != nil {
		t.Fatal(err)
	}
	return log, root
}

func TestLogAppendReadScanAndReopen(t *testing.T) {
	header := testSegmentHeader(1, 0)
	log, root := openTestLog(t, header, nil)

	value := []byte("owned after append")
	first, err := log.Append(context.Background(), value, false)
	if err != nil {
		t.Fatal(err)
	}
	clear(value)
	got, err := log.Read(context.Background(), first.Addr)
	if err != nil || string(got) != "owned after append" {
		t.Fatalf("read=%q err=%v", got, err)
	}
	second, err := log.Append(context.Background(), make([]byte, 5000), true)
	if err != nil {
		t.Fatal(err)
	}
	status := log.Status()
	if status.Watermarks.Reserved != second.End || status.Watermarks.Written != second.End || status.Watermarks.Durable != second.End {
		t.Fatalf("watermarks=%+v end=%+v", status.Watermarks, second.End)
	}

	var addresses []VAddr
	if err := log.Scan(context.Background(), LogPos{SegmentID: 1, Offset: SegmentHeaderSize}, func(result AppendResult, _ []byte) error {
		addresses = append(addresses, result.Addr)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(addresses) != 2 || addresses[0] != first.Addr || addresses[1] != second.Addr {
		t.Fatalf("addresses=%v", addresses)
	}
	if err := log.Close(); err != nil {
		t.Fatal(err)
	}

	recovered, repaired, err := openActiveSegment(root, header, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if repaired || recovered.summary().ValidEnd != second.End.Offset {
		t.Fatalf("repaired=%v summary=%+v", repaired, recovered.summary())
	}
	if err := recovered.close(); err != nil {
		t.Fatal(err)
	}
}

func TestLogRotatesBeforeAssigningAddress(t *testing.T) {
	header := testSegmentHeader(1, 0)
	header.SegmentSize = 512
	root := t.TempDir()
	active, err := createActiveSegment(root, header, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	registry, err := newSegmentRegistry(active, nil)
	if err != nil {
		t.Fatal(err)
	}
	cfg := testLogConfig()
	log, err := newLog(cfg, 256, active, registry, testRotate(root, registry))
	if err != nil {
		t.Fatal(err)
	}
	first, err := log.Append(context.Background(), make([]byte, 200), false)
	if err != nil {
		t.Fatal(err)
	}
	second, err := log.Append(context.Background(), make([]byte, 200), true)
	if err != nil {
		t.Fatal(err)
	}
	if first.Addr.SegmentID() != 1 || second.Addr.SegmentID() != 2 || second.Addr.Offset() != SegmentHeaderSize {
		t.Fatalf("first=%+v second=%+v", first, second)
	}
	var segments []SegmentID
	if err := log.Scan(context.Background(), LogPos{SegmentID: 1, Offset: SegmentHeaderSize}, func(result AppendResult, _ []byte) error {
		segments = append(segments, result.Addr.SegmentID())
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(segments) != 2 || segments[0] != 1 || segments[1] != 2 {
		t.Fatalf("segments=%v", segments)
	}
	if err := log.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestLogWriteFailurePoisonsFutureAppends(t *testing.T) {
	injected := errors.New("write failed")
	log, _ := openTestLog(t, testSegmentHeader(1, 0), func(point segmentFaultPoint) error {
		if point == faultBeforeAppendWrite {
			return injected
		}
		return nil
	})
	if _, err := log.Append(context.Background(), []byte("commit"), true); !errors.Is(err, ErrPoisoned) || !errors.Is(err, injected) {
		t.Fatalf("append error=%v", err)
	}
	if _, err := log.Append(context.Background(), []byte("later"), false); !errors.Is(err, ErrPoisoned) {
		t.Fatalf("future append error=%v", err)
	}
	if err := log.Close(); !errors.Is(err, ErrPoisoned) {
		t.Fatalf("close error=%v", err)
	}
}

type countingFile struct {
	fileHandle
	mu     sync.Mutex
	writes int
	syncs  int
}

func (f *countingFile) WriteAt(value []byte, offset int64) (int, error) {
	f.mu.Lock()
	f.writes++
	f.mu.Unlock()
	return f.fileHandle.WriteAt(value, offset)
}

func (f *countingFile) Sync() error {
	f.mu.Lock()
	f.syncs++
	f.mu.Unlock()
	return f.fileHandle.Sync()
}

func (f *countingFile) counts() (int, int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.writes, f.syncs
}

func newUnstartedTestLog(cfg Config, maxPayload uint32, active *activeSegment, registry *segmentRegistry, rotate rotateActive) *Log {
	initial := LogPos{SegmentID: active.header.SegmentID, Offset: active.summary().ValidEnd}
	log := &Log{
		cfg: cfg, requests: make(chan *appendRequest, cfg.QueueCapacity), done: make(chan struct{}),
		budget: newByteBudget(cfg.MaxQueuedBytes), registry: registry, rotate: rotate, maxPayloadBytes: maxPayload,
		pending: make(map[VAddr][]byte), closeDone: make(chan struct{}),
	}
	log.status.Watermarks = Watermarks{Reserved: initial, Written: initial, Durable: initial}
	return log
}

func TestWriterNaturallyGroupsQueuedSyncRequests(t *testing.T) {
	root := t.TempDir()
	active, err := createActiveSegment(root, testSegmentHeader(1, 0), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	counter := &countingFile{fileHandle: active.file}
	active.file = counter
	registry, err := newSegmentRegistry(active, nil)
	if err != nil {
		t.Fatal(err)
	}
	cfg := testLogConfig()
	log := newUnstartedTestLog(cfg, 1024, active, registry, testRotate(root, registry))

	const requests = 32
	errorsCh := make(chan error, requests)
	for i := 0; i < requests; i++ {
		go func() {
			_, err := log.Append(context.Background(), []byte("x"), true)
			errorsCh <- err
		}()
	}
	deadline := time.Now().Add(time.Second)
	for len(log.requests) != requests && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if len(log.requests) != requests {
		t.Fatalf("queued=%d", len(log.requests))
	}
	go log.runWriter(active)
	for i := 0; i < requests; i++ {
		if err := <-errorsCh; err != nil {
			t.Fatal(err)
		}
	}
	writes, syncs := counter.counts()
	if writes != 1 || syncs != 1 {
		t.Fatalf("writes=%d syncs=%d", writes, syncs)
	}
	if err := log.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestWriterRejectsCanceledAcceptedRequestBeforeReservation(t *testing.T) {
	root := t.TempDir()
	active, err := createActiveSegment(root, testSegmentHeader(1, 0), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	registry, err := newSegmentRegistry(active, nil)
	if err != nil {
		t.Fatal(err)
	}
	cfg := testLogConfig()
	log := newUnstartedTestLog(cfg, 1024, active, registry, testRotate(root, registry))
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		_, err := log.Append(ctx, []byte("canceled"), false)
		result <- err
	}()
	deadline := time.Now().Add(time.Second)
	for len(log.requests) != 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if len(log.requests) != 1 {
		t.Fatal("request was not accepted")
	}
	cancel()
	go log.runWriter(active)
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("append error=%v", err)
	}
	status := log.Status()
	if status.Watermarks.Reserved.Offset != SegmentHeaderSize {
		t.Fatalf("reserved=%+v", status.Watermarks.Reserved)
	}
	if err := log.Close(); err != nil {
		t.Fatal(err)
	}
}

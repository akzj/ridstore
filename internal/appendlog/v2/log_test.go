package v2

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func testConfig(t *testing.T) Config {
	t.Helper()
	cfg := DefaultConfig(t.TempDir())
	cfg.SegmentSize = 4096
	cfg.MaxPayloadSize = 1024
	cfg.MaxBufferBytes = 2048
	cfg.MaxBufferRecords = 32
	cfg.ChannelCapacity = 64
	cfg.MaxQueuedBytes = 8192
	return cfg
}

func TestAppendPendingReadAndCommitSync(t *testing.T) {
	cfg := testConfig(t)
	log, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()

	first, err := log.Append(context.Background(), []byte("page-a"), false)
	if err != nil {
		t.Fatal(err)
	}
	second, err := log.Append(context.Background(), []byte("page-b"), false)
	if err != nil {
		t.Fatal(err)
	}
	if first >= second {
		t.Fatalf("addresses are not increasing: %v >= %v", first, second)
	}
	if got, err := log.Read(context.Background(), first); err != nil || string(got) != "page-a" {
		t.Fatalf("pending read = %q, %v", got, err)
	}
	before := log.Status()
	if before.WriteCalls != 0 || before.SyncCalls != 0 || before.PendingRecords != 2 {
		t.Fatalf("before commit status = %+v", before)
	}

	commit, err := log.Append(context.Background(), []byte("commit"), true)
	if err != nil {
		t.Fatal(err)
	}
	if second >= commit {
		t.Fatalf("commit address = %v after %v", commit, second)
	}
	after := log.Status()
	if after.WriteCalls != 1 || after.SyncCalls != 1 || after.PendingRecords != 0 {
		t.Fatalf("after commit status = %+v", after)
	}
	if after.Watermarks.Durable != after.Watermarks.Written || after.Watermarks.Written != after.Watermarks.Reserved {
		t.Fatalf("watermarks = %+v", after.Watermarks)
	}
}

func TestAppendOwnsPayloadBeforeReturning(t *testing.T) {
	cfg := testConfig(t)
	log, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()

	payload := []byte("caller-owned")
	addr, err := log.Append(context.Background(), payload, false)
	if err != nil {
		t.Fatal(err)
	}
	clear(payload)
	got, err := log.Read(context.Background(), addr)
	if err != nil || string(got) != "caller-owned" {
		t.Fatalf("read after caller reuse = %q, %v", got, err)
	}
}

func TestClosePersistsAndOpenRecovers(t *testing.T) {
	cfg := testConfig(t)
	log, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	addr, err := log.Append(context.Background(), []byte("survives-close"), false)
	if err != nil {
		t.Fatal(err)
	}
	if err := log.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	got, err := reopened.Read(context.Background(), addr)
	if err != nil || string(got) != "survives-close" {
		t.Fatalf("recovered read = %q, %v", got, err)
	}
	next, err := reopened.Append(context.Background(), []byte("next"), true)
	if err != nil {
		t.Fatal(err)
	}
	if next <= addr {
		t.Fatalf("recovered append address %v <= %v", next, addr)
	}
}

func TestRotationAndRead(t *testing.T) {
	cfg := testConfig(t)
	cfg.SegmentSize = 512
	cfg.MaxPayloadSize = 128
	cfg.MaxBufferBytes = 256
	log, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	var addresses []VAddr
	for i := 0; i < 12; i++ {
		addr, err := log.Append(context.Background(), []byte{byte(i), 1, 2, 3, 4, 5, 6, 7}, i == 11)
		if err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
		addresses = append(addresses, addr)
	}
	if addresses[0].SegmentID() == addresses[len(addresses)-1].SegmentID() {
		t.Fatal("test did not rotate")
	}
	for i, addr := range addresses {
		got, err := log.Read(context.Background(), addr)
		if err != nil || len(got) == 0 || got[0] != byte(i) {
			t.Fatalf("read %v = %v, %v", addr, got, err)
		}
	}
	if err := log.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	for i, addr := range addresses {
		got, err := reopened.Read(context.Background(), addr)
		if err != nil || got[0] != byte(i) {
			t.Fatalf("reopened read %v = %v, %v", addr, got, err)
		}
	}
}

func TestConcurrentAppendAddressesAreUnique(t *testing.T) {
	cfg := testConfig(t)
	log, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()
	const count = 64
	addresses := make(chan VAddr, count)
	errs := make(chan error, count)
	var wg sync.WaitGroup
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func(value byte) {
			defer wg.Done()
			addr, err := log.Append(context.Background(), []byte{value}, true)
			addresses <- addr
			errs <- err
		}(byte(i))
	}
	wg.Wait()
	close(addresses)
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	seen := make(map[VAddr]struct{}, count)
	for addr := range addresses {
		if _, exists := seen[addr]; exists {
			t.Fatalf("duplicate address %v", addr)
		}
		seen[addr] = struct{}{}
	}
	if len(seen) != count {
		t.Fatalf("addresses = %d", len(seen))
	}
	if log.Status().SyncCalls >= count {
		t.Fatalf("requests were not naturally grouped: %+v", log.Status())
	}
}

func TestOpenRepairsShortActiveTail(t *testing.T) {
	cfg := testConfig(t)
	log, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	addr, err := log.Append(context.Background(), []byte("valid"), true)
	if err != nil {
		t.Fatal(err)
	}
	if err := log.Close(); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(cfg.Dir, activeSegmentName(addr.SegmentID()))
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte{1, 2, 3}); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if got, err := reopened.Read(context.Background(), addr); err != nil || string(got) != "valid" {
		t.Fatalf("read after repair = %q, %v", got, err)
	}
}

func TestOpenRepairsPartialFooter(t *testing.T) {
	cfg := testConfig(t)
	log, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	addr, err := log.Append(context.Background(), []byte("valid"), true)
	if err != nil {
		t.Fatal(err)
	}
	if err := log.Close(); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(cfg.Dir, activeSegmentName(addr.SegmentID()))
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write(segmentFooterMagic[:5]); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	next, err := reopened.Append(context.Background(), []byte("next"), true)
	if err != nil || next.SegmentID() != addr.SegmentID() {
		t.Fatalf("append after footer repair = %v, %v", next, err)
	}
}

func TestOpenCompletesFooterWrittenRotation(t *testing.T) {
	cfg := testConfig(t)
	log, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	addr, err := log.Append(context.Background(), []byte("valid"), true)
	if err != nil {
		t.Fatal(err)
	}
	if err := log.Close(); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(cfg.Dir, activeSegmentName(addr.SegmentID()))
	recovered, _, err := scanSegment(path, false, false)
	if err != nil {
		t.Fatal(err)
	}
	footer, err := encodeSegmentFooter(segmentFooter{
		SegmentID: recovered.header.SegmentID, DataEnd: recovered.end, FirstAddr: recovered.first, LastAddr: recovered.last, RecordCount: recovered.records,
	})
	if err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(path, os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := writeFullAt(f, footer[:], int64(recovered.end)); err != nil {
		t.Fatal(err)
	}
	if err := f.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	var scanned []string
	if err := reopened.Scan(context.Background(), 0, func(_ VAddr, payload []byte) error {
		scanned = append(scanned, string(payload))
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(scanned) != 1 || scanned[0] != "valid" {
		t.Fatalf("scan after empty active recovery = %q", scanned)
	}
	next, err := reopened.Append(context.Background(), []byte("next"), true)
	if err != nil {
		t.Fatal(err)
	}
	if next.SegmentID() != addr.SegmentID()+1 {
		t.Fatalf("next segment = %d", next.SegmentID())
	}
	if _, err := os.Stat(filepath.Join(cfg.Dir, sealedSegmentName(addr.SegmentID()))); err != nil {
		t.Fatalf("sealed segment: %v", err)
	}
}

func TestDirectoryLock(t *testing.T) {
	cfg := testConfig(t)
	first, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	second, err := Open(cfg)
	if err == nil {
		second.Close()
		t.Fatal("second writer opened the same directory")
	}
}

func TestScanIncludesWrittenAndPendingAtSnapshot(t *testing.T) {
	cfg := testConfig(t)
	log, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()
	first, err := log.Append(context.Background(), []byte("written"), true)
	if err != nil {
		t.Fatal(err)
	}
	second, err := log.Append(context.Background(), []byte("pending"), false)
	if err != nil {
		t.Fatal(err)
	}
	var addresses []VAddr
	var values []string
	if err := log.Scan(context.Background(), 0, func(addr VAddr, value []byte) error {
		addresses = append(addresses, addr)
		values = append(values, string(value))
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(addresses) != 2 || addresses[0] != first || addresses[1] != second || values[0] != "written" || values[1] != "pending" {
		t.Fatalf("scan = %v %v", addresses, values)
	}
	var fromValues []string
	if err := log.Scan(context.Background(), second, func(_ VAddr, value []byte) error {
		fromValues = append(fromValues, string(value))
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(fromValues) != 1 || fromValues[0] != "pending" {
		t.Fatalf("scan from = %v", fromValues)
	}
}

func TestScanCallbackCanCloseLog(t *testing.T) {
	cfg := testConfig(t)
	log, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := log.Append(context.Background(), []byte("value"), true); err != nil {
		t.Fatal(err)
	}
	called := false
	if err := log.Scan(context.Background(), 0, func(_ VAddr, _ []byte) error {
		called = true
		return log.Close()
	}); err != nil {
		t.Fatal(err)
	}
	if !called {
		t.Fatal("scan callback was not called")
	}
}

func TestScanDoesNotIncludeAppendAfterSnapshot(t *testing.T) {
	cfg := testConfig(t)
	log, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()
	if _, err := log.Append(context.Background(), []byte("before"), true); err != nil {
		t.Fatal(err)
	}
	var values []string
	if err := log.Scan(context.Background(), 0, func(_ VAddr, value []byte) error {
		values = append(values, string(value))
		if len(values) == 1 {
			_, err := log.Append(context.Background(), []byte("after"), true)
			return err
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(values) != 1 || values[0] != "before" {
		t.Fatalf("snapshot scan = %v", values)
	}
}

func TestAppendRejectsCanceledContextBeforeAdmission(t *testing.T) {
	cfg := testConfig(t)
	log, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := log.Append(ctx, []byte("x"), false); !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v", err)
	}
}

func TestConfigPreventsAdmissionBudgetDeadlock(t *testing.T) {
	cfg := testConfig(t)
	cfg.MaxQueuedBytes = cfg.MaxBufferBytes
	if _, err := Open(cfg); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("error = %v", err)
	}
}

func TestWriteFailurePoisonsLogAndCompletesWaiter(t *testing.T) {
	cfg := testConfig(t)
	injected := errors.New("injected write failure")
	cfg.FaultHook = func(point FaultPoint) error {
		if point == FaultBeforeAppendWrite {
			return injected
		}
		return nil
	}
	log, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := log.Append(context.Background(), []byte("reserved"), false); err != nil {
		t.Fatal(err)
	}
	if _, err := log.Append(context.Background(), []byte("commit"), true); !errors.Is(err, injected) || !errors.Is(err, ErrPoisoned) {
		t.Fatalf("commit error = %v", err)
	}
	if _, err := log.Append(context.Background(), []byte("later"), true); !errors.Is(err, ErrPoisoned) {
		t.Fatalf("later error = %v", err)
	}
	if !log.Status().Poisoned {
		t.Fatal("status is not poisoned")
	}
	if err := log.Close(); !errors.Is(err, injected) {
		t.Fatalf("close error = %v", err)
	}
}

func TestSyncFailurePoisonsAllConcurrentWaiters(t *testing.T) {
	cfg := testConfig(t)
	injected := errors.New("injected sync failure")
	var mu sync.Mutex
	fail := false
	cfg.FaultHook = func(point FaultPoint) error {
		mu.Lock()
		defer mu.Unlock()
		if point == FaultBeforeSync && fail {
			return injected
		}
		return nil
	}
	log, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	fail = true
	mu.Unlock()
	const count = 8
	results := make(chan error, count)
	var wg sync.WaitGroup
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := log.Append(context.Background(), []byte("sync"), true)
			results <- err
		}()
	}
	wg.Wait()
	close(results)
	for err := range results {
		if !errors.Is(err, ErrPoisoned) {
			t.Fatalf("waiter error = %v", err)
		}
	}
	_ = log.Close()
}

func TestBlockedSyncNaturallyGroupsQueuedRequests(t *testing.T) {
	cfg := testConfig(t)
	entered := make(chan struct{})
	release := make(chan struct{})
	var blocked atomic.Bool
	cfg.FaultHook = func(point FaultPoint) error {
		if point == FaultBeforeSync && blocked.CompareAndSwap(false, true) {
			close(entered)
			<-release
		}
		return nil
	}
	log, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()

	const queued = 16
	results := make(chan error, queued+1)
	go func() {
		_, err := log.Append(context.Background(), []byte("first"), true)
		results <- err
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("first sync did not reach fault hook")
	}
	for i := 0; i < queued; i++ {
		go func() {
			_, err := log.Append(context.Background(), []byte("queued"), true)
			results <- err
		}()
	}
	deadline := time.Now().Add(5 * time.Second)
	for log.Status().QueueRequests != queued {
		if time.Now().After(deadline) {
			t.Fatalf("queued requests = %d, want %d", log.Status().QueueRequests, queued)
		}
		time.Sleep(time.Millisecond)
	}
	close(release)
	for i := 0; i < queued+1; i++ {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
	status := log.Status()
	if status.WriteCalls != 2 || status.SyncCalls != 2 {
		t.Fatalf("writes = %d, syncs = %d, want 2 and 2", status.WriteCalls, status.SyncCalls)
	}
}

func TestRotationFailuresRecoverWithoutLosingPrefix(t *testing.T) {
	points := []FaultPoint{FaultBeforeFooterWrite, FaultBeforeFooterSync, FaultBeforeRename, FaultBeforeSealDirSync}
	for _, point := range points {
		t.Run(string(point), func(t *testing.T) {
			cfg := testConfig(t)
			cfg.SegmentSize = 512
			cfg.MaxPayloadSize = 128
			cfg.MaxBufferBytes = 256
			injected := errors.New("rotation failure")
			armed := false
			cfg.FaultHook = func(got FaultPoint) error {
				if armed && got == point {
					return injected
				}
				return nil
			}
			log, err := Open(cfg)
			if err != nil {
				t.Fatal(err)
			}
			first, err := log.Append(context.Background(), make([]byte, 128), false)
			if err != nil {
				t.Fatal(err)
			}
			secondValue := make([]byte, 128)
			secondValue[0] = 2
			second, err := log.Append(context.Background(), secondValue, false)
			if err != nil {
				t.Fatal(err)
			}
			armed = true
			if _, err := log.Append(context.Background(), make([]byte, 128), true); !errors.Is(err, injected) {
				t.Fatalf("rotation error = %v", err)
			}
			_ = log.Close()

			cfg.FaultHook = nil
			reopened, err := Open(cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			if got, err := reopened.Read(context.Background(), first); err != nil || len(got) != 128 {
				t.Fatalf("first prefix record = %d bytes, %v", len(got), err)
			}
			if got, err := reopened.Read(context.Background(), second); err != nil || len(got) != 128 || got[0] != 2 {
				t.Fatalf("second prefix record = %d bytes, %v", len(got), err)
			}
			if _, err := reopened.Append(context.Background(), []byte("continues"), true); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestWriterPanicPoisonsAndDoesNotStrandWaiter(t *testing.T) {
	cfg := testConfig(t)
	cfg.FaultHook = func(point FaultPoint) error {
		if point == FaultBeforeAppendWrite {
			panic("injected panic")
		}
		return nil
	}
	log, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := log.Append(context.Background(), []byte("value"), true); !errors.Is(err, ErrPoisoned) {
		t.Fatalf("append error = %v", err)
	}
	if _, err := log.Append(context.Background(), []byte("later"), false); !errors.Is(err, ErrPoisoned) {
		t.Fatalf("later error = %v", err)
	}
	if err := log.Close(); !errors.Is(err, ErrPoisoned) {
		t.Fatalf("close error = %v", err)
	}
}

func TestWriterPanicDuringCloseDoesNotDeadlock(t *testing.T) {
	cfg := testConfig(t)
	var armed atomic.Bool
	cfg.FaultHook = func(point FaultPoint) error {
		if point == FaultBeforeSync && armed.Load() {
			panic("injected close panic")
		}
		return nil
	}
	log, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := log.Append(context.Background(), []byte("pending"), false); err != nil {
		t.Fatal(err)
	}
	armed.Store(true)
	closed := make(chan error, 1)
	go func() { closed <- log.Close() }()
	select {
	case err := <-closed:
		if !errors.Is(err, ErrPoisoned) {
			t.Fatalf("close error = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Close blocked after writer panic")
	}
}

func TestInterruptedSegmentCreationCanRetryOpen(t *testing.T) {
	points := []FaultPoint{FaultBeforeHeaderWrite, FaultBeforeHeaderSync, FaultBeforeActiveRename, FaultBeforeCreateDirSync}
	for _, point := range points {
		t.Run(string(point), func(t *testing.T) {
			cfg := testConfig(t)
			injected := errors.New("create failure")
			cfg.FaultHook = func(got FaultPoint) error {
				if got == point {
					return injected
				}
				return nil
			}
			if log, err := Open(cfg); !errors.Is(err, injected) {
				if log != nil {
					_ = log.Close()
				}
				t.Fatalf("open error = %v", err)
			}
			cfg.FaultHook = nil
			log, err := Open(cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer log.Close()
			if _, err := log.Append(context.Background(), []byte("works"), true); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestTailRepairFailureCanRetryOpen(t *testing.T) {
	points := []FaultPoint{FaultBeforeTailTruncate, FaultBeforeTailSync}
	for _, point := range points {
		t.Run(string(point), func(t *testing.T) {
			cfg := testConfig(t)
			log, err := Open(cfg)
			if err != nil {
				t.Fatal(err)
			}
			addr, err := log.Append(context.Background(), []byte("valid"), true)
			if err != nil {
				t.Fatal(err)
			}
			if err := log.Close(); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(cfg.Dir, activeSegmentName(addr.SegmentID()))
			file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := file.Write([]byte{1, 2, 3}); err != nil {
				t.Fatal(err)
			}
			if err := file.Close(); err != nil {
				t.Fatal(err)
			}
			injected := errors.New("tail repair failure")
			cfg.FaultHook = func(got FaultPoint) error {
				if got == point {
					return injected
				}
				return nil
			}
			if reopened, err := Open(cfg); !errors.Is(err, injected) {
				if reopened != nil {
					_ = reopened.Close()
				}
				t.Fatalf("repair error = %v", err)
			}
			cfg.FaultHook = nil
			reopened, err := Open(cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			if got, err := reopened.Read(context.Background(), addr); err != nil || string(got) != "valid" {
				t.Fatalf("recovered value = %q, %v", got, err)
			}
		})
	}
}

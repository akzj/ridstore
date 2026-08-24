package v2

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
)

type appendResult struct {
	addr VAddr
	err  error
}

type appendRequest struct {
	data      []byte
	sync      bool
	bytes     uint64
	result    chan appendResult
	snapshot  chan snapshotResult
	stop      bool
	staged    bool
	completed bool
}

type scanSnapshot struct {
	last    VAddr
	written Position
	pending map[VAddr][]byte
}

type snapshotResult struct {
	snapshot scanSnapshot
	err      error
}

type Log struct {
	cfg      Config
	requests chan *appendRequest
	done     chan struct{}
	budget   *byteBudget
	lockFile *os.File

	submitMu   sync.Mutex
	closing    bool
	terminal   error
	submitters sync.WaitGroup

	closeOnce sync.Once
	closeMu   sync.Mutex
	closeErr  error

	pendingMu sync.RWMutex
	pending   map[VAddr][]byte

	statusMu sync.RWMutex
	status   Status
}

func Open(cfg Config) (*Log, error) {
	if cfg.files == nil {
		cfg.files = osFileBackend{}
	}
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(cfg.Dir, 0o700); err != nil {
		return nil, err
	}
	lockFile, err := os.OpenFile(filepath.Join(cfg.Dir, ".appendlog-v2.lock"), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(lockFile.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return nil, errors.Join(err, lockFile.Close())
	}
	active, idValue, lastAddr, err := openSegments(cfg)
	if err != nil {
		_ = syscall.Flock(int(lockFile.Fd()), syscall.LOCK_UN)
		return nil, errors.Join(err, lockFile.Close())
	}
	l := &Log{
		cfg: cfg, requests: make(chan *appendRequest, cfg.ChannelCapacity), done: make(chan struct{}),
		budget: newByteBudget(cfg.MaxQueuedBytes), lockFile: lockFile, pending: make(map[VAddr][]byte),
	}
	initial := Position{SegmentID: active.header.SegmentID, Offset: active.end}
	l.status.Watermarks = Watermarks{Reserved: initial, Written: initial, Durable: initial}
	go l.runWriter(newWriter(l, active, idValue, lastAddr))
	return l, nil
}

func (l *Log) Append(ctx context.Context, data []byte, syncWrite bool) (VAddr, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if uint64(len(data)) > l.cfg.MaxPayloadSize {
		return 0, ErrPayloadTooBig
	}
	physicalSize, err := encodedRecordSize(uint64(len(data)))
	if err != nil {
		return 0, err
	}
	l.submitMu.Lock()
	if l.closing {
		err := l.terminal
		if err == nil {
			err = ErrClosed
		}
		l.submitMu.Unlock()
		return 0, err
	}
	if l.terminal != nil {
		err := l.terminal
		l.submitMu.Unlock()
		return 0, err
	}
	l.submitters.Add(1)
	l.submitMu.Unlock()
	defer l.submitters.Done()

	budgetBytes := physicalSize
	if err := l.budget.acquire(ctx, budgetBytes); err != nil {
		l.submitMu.Lock()
		terminal := l.terminal
		l.submitMu.Unlock()
		if terminal != nil {
			return 0, terminal
		}
		return 0, err
	}
	request := &appendRequest{
		data: data, sync: syncWrite, bytes: budgetBytes, result: make(chan appendResult, 1),
	}
	select {
	case l.requests <- request:
	case <-ctx.Done():
		l.budget.release(budgetBytes)
		return 0, ctx.Err()
	}
	result := <-request.result
	return result.addr, result.err
}

func (l *Log) Read(ctx context.Context, addr VAddr) ([]byte, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !addr.Valid() {
		return nil, ErrInvalidVAddr
	}
	l.pendingMu.RLock()
	if payload, ok := l.pending[addr]; ok {
		copyValue := append([]byte(nil), payload...)
		l.pendingMu.RUnlock()
		return copyValue, nil
	}
	l.pendingMu.RUnlock()
	active := filepath.Join(l.cfg.Dir, activeSegmentName(addr.SegmentID()))
	payload, err := readRecordFile(active, addr, l.cfg.MaxPayloadSize, l.cfg.files)
	if err == nil {
		return payload, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	return readRecordFile(filepath.Join(l.cfg.Dir, sealedSegmentName(addr.SegmentID())), addr, l.cfg.MaxPayloadSize, l.cfg.files)
}

func (l *Log) Scan(ctx context.Context, from VAddr, fn func(VAddr, []byte) error) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if fn == nil || (from != 0 && !from.Valid()) {
		return ErrInvalidVAddr
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	l.submitMu.Lock()
	if l.closing {
		l.submitMu.Unlock()
		return ErrClosed
	}
	if l.terminal != nil {
		err := l.terminal
		l.submitMu.Unlock()
		return err
	}
	l.submitters.Add(1)
	l.submitMu.Unlock()

	resultCh := make(chan snapshotResult, 1)
	request := &appendRequest{snapshot: resultCh}
	select {
	case l.requests <- request:
	case <-ctx.Done():
		l.submitters.Done()
		return ctx.Err()
	}
	result := <-resultCh
	l.submitters.Done()
	if result.err != nil {
		return result.err
	}
	return l.scanSnapshot(ctx, from, result.snapshot, fn)
}

func (l *Log) Status() Status {
	l.statusMu.RLock()
	status := l.status
	l.statusMu.RUnlock()
	status.QueueRequests = len(l.requests)
	status.OutstandingBytes = l.budget.usage()
	return status
}

func (l *Log) Close() error {
	l.closeOnce.Do(func() {
		l.submitMu.Lock()
		l.closing = true
		l.submitMu.Unlock()
		l.submitters.Wait()
		request := &appendRequest{stop: true, result: make(chan appendResult, 1)}
		l.requests <- request
		result := <-request.result
		<-l.done
		l.budget.close()
		lockErr := syscall.Flock(int(l.lockFile.Fd()), syscall.LOCK_UN)
		fileErr := l.lockFile.Close()
		l.closeMu.Lock()
		l.closeErr = errors.Join(result.err, lockErr, fileErr)
		l.closeMu.Unlock()
	})
	l.closeMu.Lock()
	defer l.closeMu.Unlock()
	return l.closeErr
}

func (l *Log) setTerminal(err error) {
	if err == nil {
		return
	}
	l.submitMu.Lock()
	if l.terminal == nil {
		l.terminal = errors.Join(ErrPoisoned, err)
	}
	l.submitMu.Unlock()
	l.statusMu.Lock()
	l.status.Poisoned = true
	l.status.LastError = err.Error()
	l.statusMu.Unlock()
	l.budget.close()
}

func (l *Log) runWriter(w *writer) {
	defer close(l.done)
	defer func() {
		if recovered := recover(); recovered != nil {
			w.recoverPanic(recovered)
		}
	}()
	w.run()
}

type segmentFile struct {
	id     uint32
	sealed bool
	path   string
}

func openSegments(cfg Config) (*segment, logID, VAddr, error) {
	entries, err := cfg.files.readDir(cfg.Dir)
	if err != nil {
		return nil, logID{}, 0, err
	}
	var files []segmentFile
	removedCreating := false
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if strings.HasPrefix(name, "segment-") && strings.HasSuffix(name, ".creating") {
			idText := strings.TrimSuffix(strings.TrimPrefix(name, "segment-"), ".creating")
			id64, parseErr := strconv.ParseUint(idText, 10, 32)
			if parseErr != nil || id64 == 0 || name != creatingSegmentName(uint32(id64)) {
				return nil, logID{}, 0, fmt.Errorf("creating segment filename %q: %w", name, ErrCorrupt)
			}
			if err := cfg.files.remove(filepath.Join(cfg.Dir, name)); err != nil {
				return nil, logID{}, 0, err
			}
			removedCreating = true
			continue
		}
		sealed := strings.HasSuffix(name, ".sealed")
		active := strings.HasSuffix(name, ".active")
		if !strings.HasPrefix(name, "segment-") || (!sealed && !active) {
			continue
		}
		idText := strings.TrimSuffix(strings.TrimSuffix(strings.TrimPrefix(name, "segment-"), ".sealed"), ".active")
		id64, err := strconv.ParseUint(idText, 10, 32)
		if err != nil || id64 == 0 {
			return nil, logID{}, 0, fmt.Errorf("segment filename %q: %w", name, ErrCorrupt)
		}
		files = append(files, segmentFile{id: uint32(id64), sealed: sealed, path: filepath.Join(cfg.Dir, name)})
	}
	if removedCreating {
		if err := cfg.files.syncDir(cfg.Dir); err != nil {
			return nil, logID{}, 0, err
		}
	}
	sort.Slice(files, func(i, j int) bool { return files[i].id < files[j].id })
	if len(files) == 0 {
		var idValue logID
		if _, err := rand.Read(idValue[:]); err != nil {
			return nil, logID{}, 0, err
		}
		active, err := createSegment(cfg.Dir, 1, 0, cfg.SegmentSize, idValue, cfg.FaultHook, cfg.files)
		return active, idValue, 0, err
	}
	var idValue logID
	var previous uint32
	var activeFile segmentFile
	var lastAddr VAddr
	for i, file := range files {
		if file.id != uint32(i+1) || file.id != previous+1 {
			return nil, logID{}, 0, fmt.Errorf("segment id sequence: %w", ErrCorrupt)
		}
		if !file.sealed && i != len(files)-1 {
			return nil, logID{}, 0, fmt.Errorf("active segment is not last: %w", ErrCorrupt)
		}
		recovered, err := scanSegmentMetadataWithHook(file.path, file.sealed, !file.sealed, cfg.FaultHook, cfg.files)
		if err != nil {
			return nil, logID{}, 0, err
		}
		if i == 0 {
			idValue = recovered.header.LogID
		} else if recovered.header.LogID != idValue || recovered.header.PreviousSegment != previous {
			return nil, logID{}, 0, fmt.Errorf("segment chain: %w", ErrCorrupt)
		}
		if recovered.header.SegmentSize != cfg.SegmentSize {
			return nil, logID{}, 0, fmt.Errorf("segment size configuration: %w", ErrInvalidConfig)
		}
		if recovered.last != 0 {
			lastAddr = recovered.last
		}
		previous = file.id
		if !file.sealed {
			if recovered.sealed {
				destination := filepath.Join(cfg.Dir, sealedSegmentName(file.id))
				if err := cfg.files.rename(file.path, destination); err != nil {
					return nil, logID{}, 0, err
				}
				if err := cfg.files.syncDir(cfg.Dir); err != nil {
					return nil, logID{}, 0, err
				}
			} else {
				activeFile = file
			}
		}
	}
	if activeFile.id == 0 {
		if previous == uint32(maxSegmentID) {
			return nil, logID{}, 0, ErrInvalidConfig
		}
		active, err := createSegment(cfg.Dir, previous+1, previous, cfg.SegmentSize, idValue, cfg.FaultHook, cfg.files)
		return active, idValue, lastAddr, err
	}
	recovered, err := scanSegmentMetadataWithHook(activeFile.path, false, true, cfg.FaultHook, cfg.files)
	if err != nil {
		return nil, logID{}, 0, err
	}
	f, err := cfg.files.openFile(activeFile.path, os.O_RDWR, 0)
	if err != nil {
		return nil, logID{}, 0, err
	}
	return &segment{file: f, path: activeFile.path, header: recovered.header, end: recovered.end, first: recovered.first, last: recovered.last, records: recovered.records, hook: cfg.FaultHook, files: cfg.files}, idValue, lastAddr, nil
}

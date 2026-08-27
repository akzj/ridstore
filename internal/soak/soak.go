// Package soak implements the v2 steady-state workload harness. Short runs
// validate the harness; only a naturally completed configured run is evidence.
package soak

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/akzj/ridstore"
	"github.com/akzj/ridstore/internal/storecatalog"
)

type Options struct {
	Dir, GitCommit                          string
	GitDirty                                bool
	Duration, SampleInterval                time.Duration
	MaintenanceInterval                     time.Duration
	MaintenanceBatches                      uint64
	LiveRecords, BatchMutations, ValueBytes int
	Seed, SegmentSize                       int64
}

type Start struct {
	Type                     string    `json:"type"`
	StartedAt                time.Time `json:"started_at"`
	GitCommit                string    `json:"git_commit"`
	GitDirty                 bool      `json:"git_dirty"`
	GoVersion                string    `json:"go_version"`
	GOOS                     string    `json:"goos"`
	GOARCH                   string    `json:"goarch"`
	KernelRelease            string    `json:"kernel_release"`
	FilesystemType           int64     `json:"filesystem_type"`
	FilesystemBlockSize      int64     `json:"filesystem_block_size"`
	Device                   int64     `json:"device"`
	DurationNanos            int64     `json:"duration_nanos"`
	SampleIntervalNanos      int64     `json:"sample_interval_nanos"`
	MaintenanceIntervalNanos int64     `json:"maintenance_interval_nanos"`
	MaintenanceBatches       uint64    `json:"maintenance_batches"`
	LiveRecords              int       `json:"live_records"`
	BatchMutations           int       `json:"batch_mutations"`
	ValueBytes               int       `json:"value_bytes"`
	Seed                     int64     `json:"seed"`
	SegmentSize              int64     `json:"segment_size"`
}

type Sample struct {
	Type                  string             `json:"type"`
	Time                  time.Time          `json:"time"`
	ElapsedNanos          int64              `json:"elapsed_nanos"`
	Batches               uint64             `json:"batches"`
	Mutations             uint64             `json:"mutations"`
	LogicalBytes          uint64             `json:"logical_bytes"`
	AllocatedBytes        uint64             `json:"allocated_bytes"`
	RSSBytes              uint64             `json:"rss_bytes"`
	FDs                   int                `json:"fds"`
	Goroutines            int                `json:"goroutines"`
	Metrics               ridstore.Metrics   `json:"metrics"`
	DataActive            int                `json:"data_active_files"`
	DataSealed            int                `json:"data_sealed_files"`
	MappingActive         int                `json:"mapping_active_files"`
	MappingSealed         int                `json:"mapping_sealed_files"`
	TrashEntries          int                `json:"trash_entries"`
	TempEntries           int                `json:"temp_entries"`
	ManifestGeneration    uint64             `json:"manifest_generation"`
	CoveredCommitSeq      ridstore.CommitSeq `json:"covered_commit_seq"`
	StatsCoveredCommitSeq ridstore.CommitSeq `json:"stats_covered_commit_seq"`
	ExactLiveBytes        uint64             `json:"exact_live_bytes"`
	Error                 string             `json:"error,omitempty"`
}

type Summary struct {
	Type               string    `json:"type"`
	StartedAt          time.Time `json:"started_at"`
	WorkloadStartedAt  time.Time `json:"workload_started_at"`
	FinishedAt         time.Time `json:"finished_at"`
	RequestedNanos     int64     `json:"requested_nanos"`
	CompletedNaturally bool      `json:"completed_naturally"`
	Batches            uint64    `json:"batches"`
	Mutations          uint64    `json:"mutations"`
	Samples            uint64    `json:"samples"`
	MaxLogicalBytes    uint64    `json:"max_logical_bytes"`
	MaxAllocatedBytes  uint64    `json:"max_allocated_bytes"`
	MaxRSSBytes        uint64    `json:"max_rss_bytes"`
	BaselineFDs        int       `json:"baseline_fds"`
	FinalFDs           int       `json:"final_fds"`
	BaselineGoroutines int       `json:"baseline_goroutines"`
	FinalGoroutines    int       `json:"final_goroutines"`
	VerifiedClean      bool      `json:"verified_clean"`
}

type Failure struct {
	Type               string    `json:"type"`
	Time               time.Time `json:"time"`
	CompletedNaturally bool      `json:"completed_naturally"`
	Batches            uint64    `json:"batches"`
	Mutations          uint64    `json:"mutations"`
	Error              string    `json:"error"`
}

type modelEntry struct {
	id      ridstore.ID
	version uint64
	present bool
}

func Run(ctx context.Context, opts Options, output io.Writer) (summary Summary, resultErr error) {
	if ctx == nil {
		ctx = context.Background()
	}
	encoder := json.NewEncoder(output)
	defer func() {
		if resultErr != nil {
			encodeErr := encoder.Encode(Failure{Type: "failure", Time: time.Now().UTC(), CompletedNaturally: summary.CompletedNaturally, Batches: summary.Batches, Mutations: summary.Mutations, Error: resultErr.Error()})
			resultErr = errors.Join(resultErr, encodeErr)
		}
	}()
	if err := normalize(&opts); err != nil {
		return summary, err
	}
	started := time.Now().UTC()
	summary = Summary{Type: "summary", StartedAt: started, RequestedNanos: int64(opts.Duration), BaselineFDs: countFDs(), BaselineGoroutines: runtime.NumGoroutine()}
	if err := ensureNewPath(opts.Dir); err != nil {
		return summary, err
	}
	start, err := startRecord(opts, started)
	if err != nil {
		return summary, err
	}
	if err := encoder.Encode(start); err != nil {
		return summary, err
	}
	store, err := ridstore.Create(ctx, createConfig(opts))
	if err != nil {
		return summary, err
	}
	closed := false
	defer func() {
		if !closed {
			resultErr = errors.Join(resultErr, store.Close())
		}
	}()

	model, err := seedRecords(ctx, store, opts)
	if err != nil {
		return summary, err
	}
	rng := rand.New(rand.NewSource(opts.Seed))
	marks := make([]uint64, len(model))
	var epoch, maintenanceCycles, lastMaintenanceBatch uint64
	workloadStarted := time.Now().UTC()
	summary.WorkloadStartedAt = workloadStarted
	deadline := workloadStarted.Add(opts.Duration)
	nextSample, nextMaintenance := workloadStarted, workloadStarted.Add(opts.MaintenanceInterval)
	for time.Now().Before(deadline) {
		if err := ctx.Err(); err != nil {
			return summary, err
		}
		epoch++
		if err := applyBatch(ctx, store, model, marks, epoch, rng, opts, &summary); err != nil {
			return summary, err
		}
		now := time.Now()
		if !now.Before(nextMaintenance) || summary.Batches-lastMaintenanceBatch >= opts.MaintenanceBatches {
			maintenanceCycles++
			if err := maintain(ctx, store, maintenanceCycles%10 == 0); err != nil {
				return summary, err
			}
			nextMaintenance, lastMaintenanceBatch = now.Add(opts.MaintenanceInterval), summary.Batches
		}
		if !now.Before(nextSample) {
			if err := emitSample(encoder, opts.Dir, store, started, &summary); err != nil {
				return summary, err
			}
			nextSample = now.Add(opts.SampleInterval)
		}
	}
	summary.CompletedNaturally = true
	if err := drainMaintenance(ctx, store); err != nil {
		return summary, err
	}
	if err := validateModel(ctx, store, model, opts); err != nil {
		return summary, err
	}
	if err := emitSample(encoder, opts.Dir, store, started, &summary); err != nil {
		return summary, err
	}
	closeErr := store.Close()
	closed = true
	if closeErr != nil {
		return summary, closeErr
	}
	report, err := ridstore.Verify(ctx, ridstore.VerifyConfig{Dir: opts.Dir, MaxLiveIDs: uint64(opts.LiveRecords) + 1, MaxReplayStatuses: 1 << 20})
	if err != nil || report.Stage != ridstore.VerifyStageExact || report.LiveIDs > uint64(opts.LiveRecords) {
		if err == nil {
			err = fmt.Errorf("verify stage=%s live_ids=%d: %w", report.Stage, report.LiveIDs, ridstore.ErrCorrupt)
		}
		return summary, err
	}
	summary.VerifiedClean = true
	runtime.GC()
	summary.FinishedAt = time.Now().UTC()
	summary.FinalFDs, summary.FinalGoroutines = countFDs(), runtime.NumGoroutine()
	if (summary.BaselineFDs >= 0 && summary.FinalFDs > summary.BaselineFDs+4) || summary.FinalGoroutines > summary.BaselineGoroutines+8 {
		return summary, fmt.Errorf("resource convergence failed: fd %d->%d goroutines %d->%d", summary.BaselineFDs, summary.FinalFDs, summary.BaselineGoroutines, summary.FinalGoroutines)
	}
	if err := encoder.Encode(summary); err != nil {
		return summary, err
	}
	return summary, nil
}

func normalize(opts *Options) error {
	if opts.Dir == "" || opts.Duration <= 0 || opts.GitCommit == "" || opts.GitCommit == "unknown" {
		return ridstore.ErrInvalidConfig
	}
	if opts.SampleInterval <= 0 {
		opts.SampleInterval = time.Minute
	}
	if opts.MaintenanceInterval <= 0 {
		opts.MaintenanceInterval = 5 * time.Minute
	}
	if opts.MaintenanceBatches == 0 {
		opts.MaintenanceBatches = 128
	}
	if opts.LiveRecords <= 0 {
		opts.LiveRecords = 10_000
	}
	if opts.BatchMutations <= 0 {
		opts.BatchMutations = 64
	}
	if opts.ValueBytes <= 0 {
		opts.ValueBytes = 1024
	}
	if opts.SegmentSize <= 0 {
		opts.SegmentSize = 16 << 20
	}
	maxInt := int(^uint(0) >> 1)
	if opts.SegmentSize < 1<<20 || opts.SegmentSize > 1<<32-1 || opts.BatchMutations > opts.LiveRecords ||
		opts.ValueBytes > int(opts.SegmentSize/4) || opts.ValueBytes > maxInt/2 || opts.BatchMutations > maxInt/opts.ValueBytes/2 {
		return ridstore.ErrInvalidConfig
	}
	abs, err := filepath.Abs(opts.Dir)
	if err != nil {
		return err
	}
	opts.Dir = abs
	return nil
}

func createConfig(opts Options) ridstore.CreateConfig {
	batchBytes := uint64(opts.ValueBytes * opts.BatchMutations * 2)
	maxPayload := uint64(opts.ValueBytes*2 + 4096)
	return ridstore.CreateConfig{Dir: opts.Dir,
		HardLimits: ridstore.HardLimits{SegmentSize: uint64(opts.SegmentSize), MaxValueSize: uint64(opts.ValueBytes * 2), MaxBatchBytes: batchBytes, MaxBatchMutations: uint64(opts.BatchMutations * 2), MaxBatchConditions: uint64(opts.BatchMutations * 2), MaxOpenBatches: 64, MaxRecordLogPayload: maxPayload, IDReserveSize: 1 << 16, BatchIDReserveSize: 1 << 14},
		Runtime:    ridstore.RuntimeConfig{MaxQueuedBytes: uint64(opts.SegmentSize), AppendQueueCapacity: 128, AppendBufferBytes: 256 << 10, AppendBufferRecords: 256, CommitQueueCapacity: 64, MaxGroupBatches: 16, MaxGroupPayload: maxPayload, MappingCacheBytes: 32 << 20, CheckpointSortBytes: 32 << 20, MaxSegmentStats: 1 << 16, DeltaSoftLimitBytes: 16 << 20, DeltaHardLimitBytes: 32 << 20, StatusRetention: 1 << 16, WriteStopFreeBytes: 1, SpaceCheckInterval: time.Second}}
}

func ensureNewPath(path string) error {
	_, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	return ridstore.ErrAlreadyExists
}

func seedRecords(ctx context.Context, store *ridstore.Store, opts Options) ([]modelEntry, error) {
	model := make([]modelEntry, opts.LiveRecords)
	for start := 0; start < len(model); start += opts.BatchMutations {
		end := min(start+opts.BatchMutations, len(model))
		batch, err := store.Begin(ctx)
		if err != nil {
			return nil, err
		}
		for i := start; i < end; i++ {
			id, err := batch.Create(ctx, valueFor(i, 1, opts.ValueBytes))
			if err != nil {
				_ = batch.Abort(context.Background())
				return nil, err
			}
			model[i] = modelEntry{id: id, version: 1, present: true}
		}
		if _, err := batch.Commit(ctx); err != nil {
			return nil, err
		}
	}
	return model, nil
}

func applyBatch(ctx context.Context, store *ridstore.Store, model []modelEntry, marks []uint64, epoch uint64, rng *rand.Rand, opts Options, summary *Summary) error {
	batch, err := store.Begin(ctx)
	if err != nil {
		return err
	}
	type change struct {
		index   int
		version uint64
		present bool
	}
	changes := make([]change, 0, opts.BatchMutations)
	for len(changes) < opts.BatchMutations {
		index := rng.Intn(len(model))
		if marks[index] == epoch {
			continue
		}
		marks[index] = epoch
		entry, present := model[index], true
		version := entry.version + 1
		if rng.Intn(100) < 20 && entry.present {
			err, present = batch.Delete(entry.id), false
		} else {
			err = batch.Put(ctx, entry.id, valueFor(index, version, opts.ValueBytes))
		}
		if err != nil {
			_ = batch.Abort(context.Background())
			return err
		}
		changes = append(changes, change{index: index, version: version, present: present})
	}
	if rng.Intn(100) < 5 {
		return batch.Abort(ctx)
	}
	if _, err := batch.Commit(ctx); err != nil {
		return err
	}
	for _, change := range changes {
		model[change.index].version, model[change.index].present = change.version, change.present
	}
	summary.Batches++
	summary.Mutations += uint64(len(changes))
	return nil
}

func maintain(ctx context.Context, store *ridstore.Store, compactMapping bool) error {
	if _, _, err := store.CompactNextSegment(ctx, ridstore.CompactionPolicy{}); err != nil {
		return err
	}
	if compactMapping {
		return store.CompactMapping(ctx)
	}
	return nil
}

func drainMaintenance(ctx context.Context, store *ridstore.Store) error {
	for i := 0; i < 256; i++ {
		_, found, err := store.CompactNextSegment(ctx, ridstore.CompactionPolicy{})
		if err != nil {
			return err
		}
		if !found {
			if err := store.CompactMapping(ctx); err != nil {
				return err
			}
			return store.Checkpoint(ctx)
		}
	}
	return errors.New("data compaction did not quiesce in 256 rounds")
}

func validateModel(ctx context.Context, store *ridstore.Store, model []modelEntry, opts Options) error {
	for index, entry := range model {
		record, err := store.Get(ctx, entry.id)
		if !entry.present {
			if err == nil {
				return fmt.Errorf("id %d unexpectedly present: %w", entry.id, ridstore.ErrCorrupt)
			}
			if !errors.Is(err, ridstore.ErrNotFound) {
				return fmt.Errorf("id %d expected absent: %w", entry.id, err)
			}
			continue
		}
		if err != nil {
			return err
		}
		if !bytes.Equal(record.Value, valueFor(index, entry.version, opts.ValueBytes)) {
			return fmt.Errorf("id %d value mismatch: %w", entry.id, ridstore.ErrCorrupt)
		}
	}
	return nil
}

func valueFor(index int, version uint64, size int) []byte {
	prefix := []byte(strconv.Itoa(index) + ":" + strconv.FormatUint(version, 10) + ":")
	value := make([]byte, size)
	for i := range value {
		value[i] = prefix[i%len(prefix)]
	}
	return value
}

func emitSample(encoder *json.Encoder, dir string, store *ridstore.Store, started time.Time, summary *Summary) error {
	sample := collectSample(dir, store, started, summary.Batches, summary.Mutations)
	if err := encoder.Encode(sample); err != nil {
		return err
	}
	if sample.Error != "" {
		return errors.New(sample.Error)
	}
	summary.Samples++
	if sample.LogicalBytes > summary.MaxLogicalBytes {
		summary.MaxLogicalBytes = sample.LogicalBytes
	}
	if sample.AllocatedBytes > summary.MaxAllocatedBytes {
		summary.MaxAllocatedBytes = sample.AllocatedBytes
	}
	if sample.RSSBytes > summary.MaxRSSBytes {
		summary.MaxRSSBytes = sample.RSSBytes
	}
	return nil
}

func collectSample(dir string, store *ridstore.Store, started time.Time, batches, mutations uint64) Sample {
	logical, allocated, diskErr := diskUsage(dir)
	files, fileErr := fileState(dir)
	manifest, manifestErr := storecatalog.Load(dir)
	sample := Sample{Type: "sample", Time: time.Now().UTC(), ElapsedNanos: time.Since(started).Nanoseconds(), Batches: batches, Mutations: mutations, LogicalBytes: logical, AllocatedBytes: allocated, RSSBytes: rssBytes(), FDs: countFDs(), Goroutines: runtime.NumGoroutine(), Metrics: store.Metrics(), DataActive: files.dataActive, DataSealed: files.dataSealed, MappingActive: files.mappingActive, MappingSealed: files.mappingSealed, TrashEntries: files.trash, TempEntries: files.temp, ManifestGeneration: manifest.Generation, CoveredCommitSeq: manifest.CoveredCommitSeq, StatsCoveredCommitSeq: manifest.StatsCoveredCommitSeq}
	for _, stat := range manifest.SegmentStats {
		sample.ExactLiveBytes += stat.LiveBytes
	}
	if err := errors.Join(diskErr, fileErr, manifestErr); err != nil {
		sample.Error = err.Error()
	}
	return sample
}

type fileStateSample struct{ dataActive, dataSealed, mappingActive, mappingSealed, trash, temp int }

func fileState(root string) (result fileStateSample, resultErr error) {
	for _, item := range []struct{ dir, kind string }{{"records", "data"}, {"mapping-v2", "mapping"}, {"trash", "trash"}, {"mapping-gc-stage-v2", "temp"}} {
		entries, err := os.ReadDir(filepath.Join(root, item.dir))
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return result, err
		}
		for _, entry := range entries {
			name := entry.Name()
			switch item.kind {
			case "trash":
				result.trash++
			case "temp":
				result.temp++
			case "data":
				if strings.HasSuffix(name, ".active") {
					result.dataActive++
				}
				if strings.HasSuffix(name, ".sealed") {
					result.dataSealed++
				}
			case "mapping":
				if strings.HasSuffix(name, ".active") {
					result.mappingActive++
				}
				if strings.HasSuffix(name, ".sealed") {
					result.mappingSealed++
				}
			}
		}
	}
	return result, nil
}

func startRecord(opts Options, started time.Time) (Start, error) {
	parent := filepath.Dir(opts.Dir)
	var fs syscall.Statfs_t
	if err := syscall.Statfs(parent, &fs); err != nil {
		return Start{}, err
	}
	info, err := os.Stat(parent)
	if err != nil {
		return Start{}, err
	}
	var device int64
	if stat, ok := info.Sys().(*syscall.Stat_t); ok {
		device = int64(stat.Dev)
	}
	kernel, err := os.ReadFile("/proc/sys/kernel/osrelease")
	if err != nil {
		return Start{}, err
	}
	return Start{Type: "start", StartedAt: started, GitCommit: opts.GitCommit, GitDirty: opts.GitDirty, GoVersion: runtime.Version(), GOOS: runtime.GOOS, GOARCH: runtime.GOARCH, KernelRelease: strings.TrimSpace(string(kernel)), FilesystemType: int64(fs.Type), FilesystemBlockSize: fs.Bsize, Device: device, DurationNanos: int64(opts.Duration), SampleIntervalNanos: int64(opts.SampleInterval), MaintenanceIntervalNanos: int64(opts.MaintenanceInterval), MaintenanceBatches: opts.MaintenanceBatches, LiveRecords: opts.LiveRecords, BatchMutations: opts.BatchMutations, ValueBytes: opts.ValueBytes, Seed: opts.Seed, SegmentSize: opts.SegmentSize}, nil
}

func diskUsage(root string) (logical, allocated uint64, resultErr error) {
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		logical += uint64(info.Size())
		if stat, ok := info.Sys().(*syscall.Stat_t); ok {
			allocated += uint64(stat.Blocks) * 512
		}
		return nil
	})
	return logical, allocated, err
}

func countFDs() int {
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		return -1
	}
	return len(entries)
}

func rssBytes() uint64 {
	data, err := os.ReadFile("/proc/self/statm")
	if err != nil {
		return 0
	}
	fields := strings.Fields(string(data))
	if len(fields) < 2 {
		return 0
	}
	pages, err := strconv.ParseUint(fields[1], 10, 64)
	if err != nil {
		return 0
	}
	return pages * uint64(os.Getpagesize())
}

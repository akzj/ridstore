// Package soak implements the long-running steady-state verification workload.
// It is deliberately separate from correctness tests: a short run validates
// the harness, while only a naturally completed configured duration is soak
// evidence.
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
	"github.com/akzj/ridstore/internal/manifest"
	"github.com/akzj/ridstore/internal/verify"
)

type Options struct {
	Dir                 string
	Duration            time.Duration
	SampleInterval      time.Duration
	MaintenanceInterval time.Duration
	MaintenanceBatches  uint64
	LiveRecords         int
	BatchMutations      int
	ValueBytes          int
	Seed                int64
	SegmentSize         int64
	GitCommit           string
}

type Start struct {
	Type                     string    `json:"type"`
	StartedAt                time.Time `json:"started_at"`
	GitCommit                string    `json:"git_commit"`
	GoVersion                string    `json:"go_version"`
	GOOS                     string    `json:"goos"`
	GOARCH                   string    `json:"goarch"`
	KernelRelease            string    `json:"kernel_release"`
	FilesystemType           int64     `json:"filesystem_type"`
	FilesystemBlockSize      int64     `json:"filesystem_block_size"`
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
	Type               string           `json:"type"`
	Time               time.Time        `json:"time"`
	ElapsedNanos       int64            `json:"elapsed_nanos"`
	Batches            uint64           `json:"batches"`
	Mutations          uint64           `json:"mutations"`
	LogicalBytes       uint64           `json:"logical_bytes"`
	AllocatedBytes     uint64           `json:"allocated_bytes"`
	RSSBytes           uint64           `json:"rss_bytes"`
	FDs                int              `json:"fds"`
	Goroutines         int              `json:"goroutines"`
	Metrics            ridstore.Metrics `json:"metrics"`
	MappingTotal       uint64           `json:"mapping_total_bytes,omitempty"`
	MappingReachable   uint64           `json:"mapping_reachable_bytes,omitempty"`
	MappingUnreachable uint64           `json:"mapping_unreachable_bytes,omitempty"`
	DataActive         int              `json:"data_active_files"`
	DataSealed         int              `json:"data_sealed_files"`
	MappingActive      int              `json:"mapping_active_files"`
	MappingSealed      int              `json:"mapping_sealed_files"`
	TrashEntries       int              `json:"trash_entries"`
	TempEntries        int              `json:"temp_entries"`
	ManifestGeneration uint64           `json:"manifest_generation"`
	CoveredCommitSeq   uint64           `json:"covered_commit_seq"`
	StatsCoveredSeq    uint64           `json:"stats_covered_commit_seq"`
	ExactLiveBytes     uint64           `json:"exact_live_bytes"`
	CheckpointPending  bool             `json:"checkpoint_pending"`
	Error              string           `json:"error,omitempty"`
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
	if err := normalize(&opts); err != nil {
		return summary, err
	}
	encoder := json.NewEncoder(output)
	defer func() {
		if resultErr != nil {
			encodeErr := encoder.Encode(Failure{Type: "failure", Time: time.Now().UTC(), CompletedNaturally: summary.CompletedNaturally, Batches: summary.Batches, Mutations: summary.Mutations, Error: resultErr.Error()})
			resultErr = errors.Join(resultErr, encodeErr)
		}
	}()
	started := time.Now().UTC()
	summary = Summary{Type: "summary", StartedAt: started, RequestedNanos: int64(opts.Duration)}
	summary.BaselineFDs = countFDs()
	summary.BaselineGoroutines = runtime.NumGoroutine()
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
	store, err := ridstore.Create(config(opts))
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
	selectionMarks := make([]uint64, len(model))
	selectionEpoch := uint64(0)
	workloadStarted := time.Now().UTC()
	summary.WorkloadStartedAt = workloadStarted
	deadline := workloadStarted.Add(opts.Duration)
	nextSample := time.Now()
	nextMaintenance := time.Now().Add(opts.MaintenanceInterval)
	maintenanceCycles := uint64(0)
	lastMaintenanceBatch := uint64(0)
	for time.Now().Before(deadline) {
		if err := ctx.Err(); err != nil {
			return summary, err
		}
		selectionEpoch++
		if err := applyBatch(ctx, store, model, selectionMarks, selectionEpoch, rng, opts, &summary); err != nil {
			return summary, err
		}
		now := time.Now()
		if !now.Before(nextMaintenance) || summary.Batches-lastMaintenanceBatch >= opts.MaintenanceBatches {
			maintenanceCycles++
			if err := maintain(ctx, store, maintenanceCycles%10 == 0); err != nil {
				return summary, err
			}
			nextMaintenance = now.Add(opts.MaintenanceInterval)
			lastMaintenanceBatch = summary.Batches
		}
		if !now.Before(nextSample) {
			sample := collectSample(ctx, opts.Dir, store, started, summary.Batches, summary.Mutations)
			if err := encoder.Encode(sample); err != nil {
				return summary, err
			}
			if sample.Error != "" {
				return summary, errors.New(sample.Error)
			}
			summary.Samples++
			maxSample(&summary, sample)
			nextSample = now.Add(opts.SampleInterval)
		}
	}
	summary.CompletedNaturally = true
	if err := drainMaintenance(ctx, store, opts.Dir); err != nil {
		return summary, err
	}
	if err := validateModel(ctx, store, model, opts); err != nil {
		return summary, err
	}
	finalSample := collectSample(ctx, opts.Dir, store, started, summary.Batches, summary.Mutations)
	if err := encoder.Encode(finalSample); err != nil {
		return summary, err
	}
	if finalSample.Error != "" {
		return summary, errors.New(finalSample.Error)
	}
	summary.Samples++
	maxSample(&summary, finalSample)
	if err := store.Close(); err != nil {
		return summary, err
	}
	closed = true
	report, err := verify.Run(ctx, opts.Dir)
	if err != nil || !report.Clean {
		if err == nil {
			err = ridstore.ErrCorrupt
		}
		return summary, err
	}
	summary.VerifiedClean = true
	runtime.GC()
	summary.FinishedAt = time.Now().UTC()
	summary.FinalFDs = countFDs()
	summary.FinalGoroutines = runtime.NumGoroutine()
	if summary.FinalFDs > summary.BaselineFDs+4 || summary.FinalGoroutines > summary.BaselineGoroutines+8 {
		return summary, fmt.Errorf("resource convergence failed: fd %d->%d goroutines %d->%d", summary.BaselineFDs, summary.FinalFDs, summary.BaselineGoroutines, summary.FinalGoroutines)
	}
	if err := encoder.Encode(summary); err != nil {
		return summary, err
	}
	return summary, nil
}

func normalize(opts *Options) error {
	if opts.Dir == "" || opts.Duration <= 0 {
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
	if opts.GitCommit == "" || opts.GitCommit == "unknown" {
		return fmt.Errorf("Git commit is required for reproducible evidence: %w", ridstore.ErrInvalidConfig)
	}
	maxInt := int(^uint(0) >> 1)
	if opts.ValueBytes > int(opts.SegmentSize/4) || opts.BatchMutations > opts.LiveRecords ||
		opts.ValueBytes > maxInt/2 || opts.BatchMutations > maxInt/opts.ValueBytes/2 {
		return ridstore.ErrInvalidConfig
	}
	abs, err := filepath.Abs(opts.Dir)
	if err != nil {
		return err
	}
	opts.Dir = abs
	return nil
}

func config(opts Options) ridstore.Config {
	batchBytes := int64(opts.ValueBytes * opts.BatchMutations * 2)
	batchMutations := opts.BatchMutations * 2
	return ridstore.Config{Dir: opts.Dir, SegmentSize: opts.SegmentSize, MaxValueSize: int64(opts.ValueBytes * 2), MaxBatchBytes: batchBytes, MaxBatchMutations: batchMutations, MaxBatchConditions: batchMutations, MaxOpenBatches: 64, IDReserveSize: 1 << 16, BatchIDReserveSize: 1 << 14, MappingCacheBytes: 32 << 20, DeltaSoftLimitBytes: 16 << 20, DeltaHardLimitBytes: 32 << 20, CheckpointMemoryBytes: 32 << 20, StatusRetention: 1 << 14, GCBatchBytes: batchBytes, GCBatchMutations: batchMutations, GCMinFreeBytes: opts.SegmentSize, GCBytesPerSecond: 64 << 20}
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

func startRecord(opts Options, started time.Time) (Start, error) {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(filepath.Dir(opts.Dir), &stat); err != nil {
		return Start{}, err
	}
	kernel, err := os.ReadFile("/proc/sys/kernel/osrelease")
	if err != nil {
		return Start{}, err
	}
	return Start{Type: "start", StartedAt: started, GitCommit: opts.GitCommit, GoVersion: runtime.Version(), GOOS: runtime.GOOS, GOARCH: runtime.GOARCH, KernelRelease: strings.TrimSpace(string(kernel)), FilesystemType: int64(stat.Type), FilesystemBlockSize: stat.Bsize, DurationNanos: int64(opts.Duration), SampleIntervalNanos: int64(opts.SampleInterval), MaintenanceIntervalNanos: int64(opts.MaintenanceInterval), MaintenanceBatches: opts.MaintenanceBatches, LiveRecords: opts.LiveRecords, BatchMutations: opts.BatchMutations, ValueBytes: opts.ValueBytes, Seed: opts.Seed, SegmentSize: opts.SegmentSize}, nil
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
			id, err := batch.Allocate(ctx)
			if err != nil {
				_ = batch.Abort(context.Background())
				return nil, err
			}
			model[i] = modelEntry{id: id, version: 1, present: true}
			if err := batch.Put(ctx, id, valueFor(i, 1, opts.ValueBytes)); err != nil {
				_ = batch.Abort(context.Background())
				return nil, err
			}
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
	indices := make([]int, 0, opts.BatchMutations)
	for len(indices) < opts.BatchMutations {
		index := rng.Intn(len(model))
		if marks[index] == epoch {
			continue
		}
		marks[index] = epoch
		indices = append(indices, index)
	}
	type change struct {
		index   int
		version uint64
		present bool
	}
	changes := make([]change, 0, len(indices))
	for _, index := range indices {
		entry := model[index]
		if rng.Intn(100) < 20 && entry.present {
			err = batch.Delete(ctx, entry.id)
			changes = append(changes, change{index: index, version: entry.version + 1})
		} else {
			version := entry.version + 1
			err = batch.Put(ctx, entry.id, valueFor(index, version, opts.ValueBytes))
			changes = append(changes, change{index: index, version: version, present: true})
		}
		if err != nil {
			_ = batch.Abort(context.Background())
			return err
		}
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

func maintain(ctx context.Context, store *ridstore.Store, mapping bool) error {
	if err := store.Checkpoint(ctx); err != nil {
		return err
	}
	if _, err := store.CompactData(ctx); err != nil {
		return err
	}
	if mapping {
		return store.CompactMapping(ctx)
	}
	return nil
}

func drainMaintenance(ctx context.Context, store *ridstore.Store, dir string) error {
	if err := store.Checkpoint(ctx); err != nil {
		return err
	}
	_, previous, err := diskUsage(dir)
	if err != nil {
		return err
	}
	stagnant := 0
	for i := 0; i < 64; i++ {
		result, err := store.CompactData(ctx)
		if err != nil {
			return err
		}
		if result.SourceSegmentID == 0 {
			break
		}
		_, current, err := diskUsage(dir)
		if err != nil {
			return err
		}
		if current >= previous {
			stagnant++
		} else {
			stagnant = 0
		}
		previous = current
		if stagnant >= 3 {
			break
		}
		if i == 63 {
			return fmt.Errorf("data GC did not reach quiescence in 64 rounds")
		}
	}
	if err := store.CompactMapping(ctx); err != nil {
		return err
	}
	return store.Checkpoint(ctx)
}

func validateModel(ctx context.Context, store *ridstore.Store, model []modelEntry, opts Options) error {
	for i, entry := range model {
		got, err := store.Get(ctx, entry.id)
		if !entry.present {
			if !errors.Is(err, ridstore.ErrNotFound) {
				return fmt.Errorf("id %d absent: %w", entry.id, err)
			}
			continue
		}
		if err != nil {
			return err
		}
		want := valueFor(i, entry.version, opts.ValueBytes)
		if !bytes.Equal(got, want) {
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

func collectSample(ctx context.Context, dir string, store *ridstore.Store, started time.Time, batches, mutations uint64) Sample {
	logical, allocated, walkErr := diskUsage(dir)
	mapping, mappingErr := store.MappingSpaceUsage(ctx)
	files, fileErr := fileState(dir)
	durable, manifestErr := manifest.LoadCurrent(dir)
	maintenance := store.MaintenanceStatus()
	sample := Sample{Type: "sample", Time: time.Now().UTC(), ElapsedNanos: time.Since(started).Nanoseconds(), Batches: batches, Mutations: mutations, LogicalBytes: logical, AllocatedBytes: allocated, RSSBytes: rssBytes(), FDs: countFDs(), Goroutines: runtime.NumGoroutine(), Metrics: store.Metrics(), MappingTotal: mapping.TotalBytes, MappingReachable: mapping.ReachableBytes, MappingUnreachable: mapping.UnreachableBytes, DataActive: files.dataActive, DataSealed: files.dataSealed, MappingActive: files.mappingActive, MappingSealed: files.mappingSealed, TrashEntries: files.trash, TempEntries: files.temp, ManifestGeneration: durable.Generation, CoveredCommitSeq: uint64(durable.CoveredCommitSeq), StatsCoveredSeq: uint64(durable.StatsCoveredCommitSeq), CheckpointPending: maintenance.CheckpointPending}
	for _, stat := range durable.SegmentStats {
		sample.ExactLiveBytes += stat.ExactLiveBytes
	}
	if err := errors.Join(walkErr, mappingErr, fileErr, manifestErr, maintenance.LastCheckpointError); err != nil {
		sample.Error = err.Error()
	}
	return sample
}

type fileStateSample struct{ dataActive, dataSealed, mappingActive, mappingSealed, trash, temp int }

func fileState(root string) (fileStateSample, error) {
	var result fileStateSample
	for _, entry := range []struct {
		dir  string
		kind string
	}{{"data", "data"}, {"mapping", "mapping"}, {"trash", "trash"}, {"tmp", "temp"}} {
		items, err := os.ReadDir(filepath.Join(root, entry.dir))
		if err != nil {
			return result, err
		}
		for _, item := range items {
			if entry.kind == "trash" {
				result.trash++
				continue
			}
			if entry.kind == "temp" {
				result.temp++
				continue
			}
			name := item.Name()
			if entry.kind == "data" && strings.HasSuffix(name, ".active") {
				result.dataActive++
			}
			if entry.kind == "data" && strings.HasSuffix(name, ".seg") {
				result.dataSealed++
			}
			if entry.kind == "mapping" && strings.HasSuffix(name, ".active") {
				result.mappingActive++
			}
			if entry.kind == "mapping" && strings.HasSuffix(name, ".seg") {
				result.mappingSealed++
			}
		}
	}
	return result, nil
}

func maxSample(summary *Summary, sample Sample) {
	if sample.LogicalBytes > summary.MaxLogicalBytes {
		summary.MaxLogicalBytes = sample.LogicalBytes
	}
	if sample.AllocatedBytes > summary.MaxAllocatedBytes {
		summary.MaxAllocatedBytes = sample.AllocatedBytes
	}
	if sample.RSSBytes > summary.MaxRSSBytes {
		summary.MaxRSSBytes = sample.RSSBytes
	}
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

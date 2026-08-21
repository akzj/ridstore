package ridstore

import (
	"fmt"
	"math"
	"path/filepath"
	"time"

	"github.com/akzj/ridstore/internal/base"
	storeformat "github.com/akzj/ridstore/internal/format"
)

const mib = int64(1 << 20)

// Config combines persisted format limits with runtime-only resource budgets.
// Zero fields use defaults during Create and use persisted hard limits during Open.
type Config struct {
	Dir string

	SegmentSize        int64
	MaxValueSize       int64
	MaxBatchBytes      int64
	MaxBatchMutations  int
	MaxBatchConditions int
	MaxOpenBatches     int
	IDReserveSize      uint64
	BatchIDReserveSize uint64

	MappingCacheBytes     int64
	DeltaSoftLimitBytes   int64
	DeltaHardLimitBytes   int64
	CheckpointMemoryBytes int64
	MaxGroupBytes         int64
	MaxGroupBatches       int
	MaxGroupDelay         time.Duration
	GCBatchBytes          int64
	GCBatchMutations      int
}

func normalizeCreateConfig(cfg Config) (Config, storeformat.HardLimits, error) {
	applyHardDefaults(&cfg)
	applyRuntimeDefaults(&cfg)
	if err := normalizeDir(&cfg); err != nil {
		return Config{}, storeformat.HardLimits{}, err
	}
	hard, err := hardLimits(cfg)
	if err != nil {
		return Config{}, storeformat.HardLimits{}, err
	}
	if err := validateRuntime(cfg, hard); err != nil {
		return Config{}, storeformat.HardLimits{}, err
	}
	return cfg, hard, nil
}

func normalizeOpenConfig(cfg Config, disk storeformat.HardLimits) (Config, error) {
	if err := matchOrAdoptHardLimits(&cfg, disk); err != nil {
		return Config{}, err
	}
	applyRuntimeDefaults(&cfg)
	if err := normalizeDir(&cfg); err != nil {
		return Config{}, err
	}
	if err := validateRuntime(cfg, disk); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func applyHardDefaults(cfg *Config) {
	if cfg.SegmentSize == 0 {
		cfg.SegmentSize = 256 * mib
	}
	if cfg.MaxValueSize == 0 {
		cfg.MaxValueSize = 64 * mib
	}
	if cfg.MaxBatchBytes == 0 {
		cfg.MaxBatchBytes = 256 * mib
	}
	if cfg.MaxBatchMutations == 0 {
		cfg.MaxBatchMutations = 1_000_000
	}
	if cfg.MaxBatchConditions == 0 {
		cfg.MaxBatchConditions = 1_000_000
	}
	if cfg.MaxOpenBatches == 0 {
		cfg.MaxOpenBatches = 1024
	}
	if cfg.IDReserveSize == 0 {
		cfg.IDReserveSize = 1 << 20
	}
	if cfg.BatchIDReserveSize == 0 {
		cfg.BatchIDReserveSize = 1 << 16
	}
}

func applyRuntimeDefaults(cfg *Config) {
	if cfg.MappingCacheBytes == 0 {
		cfg.MappingCacheBytes = 256 * mib
	}
	if cfg.DeltaSoftLimitBytes == 0 {
		cfg.DeltaSoftLimitBytes = 256 * mib
	}
	if cfg.DeltaHardLimitBytes == 0 {
		cfg.DeltaHardLimitBytes = 512 * mib
	}
	if cfg.CheckpointMemoryBytes == 0 {
		cfg.CheckpointMemoryBytes = 256 * mib
	}
	if cfg.MaxGroupBytes == 0 {
		cfg.MaxGroupBytes = 8 * mib
	}
	if cfg.MaxGroupBatches == 0 {
		cfg.MaxGroupBatches = 64
	}
	if cfg.GCBatchBytes == 0 {
		cfg.GCBatchBytes = 16 * mib
	}
	if cfg.GCBatchMutations == 0 {
		cfg.GCBatchMutations = 4096
	}
}

func normalizeDir(cfg *Config) error {
	if cfg.Dir == "" {
		return fmt.Errorf("empty directory: %w", base.ErrInvalidConfig)
	}
	abs, err := filepath.Abs(cfg.Dir)
	if err != nil {
		return fmt.Errorf("resolve directory: %w", err)
	}
	cfg.Dir = filepath.Clean(abs)
	return nil
}

func hardLimits(cfg Config) (storeformat.HardLimits, error) {
	if cfg.SegmentSize <= 0 || cfg.MaxValueSize <= 0 || cfg.MaxBatchBytes <= 0 ||
		cfg.MaxBatchMutations <= 0 || cfg.MaxBatchConditions <= 0 || cfg.MaxOpenBatches <= 0 ||
		cfg.IDReserveSize == 0 || cfg.BatchIDReserveSize == 0 {
		return storeformat.HardLimits{}, fmt.Errorf("non-positive hard limit: %w", base.ErrInvalidConfig)
	}
	if uint64(cfg.SegmentSize) > uint64(math.MaxUint32)+1 || cfg.MaxValueSize > cfg.MaxBatchBytes ||
		uint64(cfg.MaxValueSize)+storeformat.FrameHeaderSize+2*storeformat.SegmentHeaderSize > uint64(cfg.SegmentSize) {
		return storeformat.HardLimits{}, fmt.Errorf("incompatible segment/value limits: %w", base.ErrInvalidConfig)
	}
	if uint64(cfg.MaxBatchMutations) > math.MaxUint32 || uint64(cfg.MaxBatchConditions) > math.MaxUint32 || uint64(cfg.MaxOpenBatches) > math.MaxUint32 {
		return storeformat.HardLimits{}, fmt.Errorf("hard limit exceeds format count: %w", base.ErrInvalidConfig)
	}
	return storeformat.HardLimits{
		SegmentSize: uint64(cfg.SegmentSize), MaxValueSize: uint64(cfg.MaxValueSize), MaxBatchBytes: uint64(cfg.MaxBatchBytes),
		MaxBatchMutations: uint64(cfg.MaxBatchMutations), MaxBatchConditions: uint64(cfg.MaxBatchConditions), MaxOpenBatches: uint64(cfg.MaxOpenBatches),
		IDReserveSize: cfg.IDReserveSize, BatchIDReserveSize: cfg.BatchIDReserveSize,
	}, nil
}

func validateRuntime(cfg Config, hard storeformat.HardLimits) error {
	if cfg.MappingCacheBytes <= 0 || cfg.DeltaSoftLimitBytes <= 0 || cfg.DeltaHardLimitBytes <= cfg.DeltaSoftLimitBytes ||
		cfg.CheckpointMemoryBytes < 64<<10 || cfg.MaxGroupBytes <= 0 || cfg.MaxGroupBatches <= 0 || cfg.MaxGroupDelay < 0 ||
		cfg.GCBatchBytes <= 0 || uint64(cfg.GCBatchBytes) > hard.MaxBatchBytes || cfg.GCBatchMutations <= 0 || uint64(cfg.GCBatchMutations) > hard.MaxBatchMutations {
		return fmt.Errorf("invalid runtime budget: %w", base.ErrInvalidConfig)
	}
	return nil
}

func matchOrAdoptHardLimits(cfg *Config, disk storeformat.HardLimits) error {
	type pair struct {
		supplied int64
		disk     uint64
		set      func(int64)
	}
	pairs := []pair{
		{cfg.SegmentSize, disk.SegmentSize, func(v int64) { cfg.SegmentSize = v }},
		{cfg.MaxValueSize, disk.MaxValueSize, func(v int64) { cfg.MaxValueSize = v }},
		{cfg.MaxBatchBytes, disk.MaxBatchBytes, func(v int64) { cfg.MaxBatchBytes = v }},
		{int64(cfg.MaxBatchMutations), disk.MaxBatchMutations, func(v int64) { cfg.MaxBatchMutations = int(v) }},
		{int64(cfg.MaxBatchConditions), disk.MaxBatchConditions, func(v int64) { cfg.MaxBatchConditions = int(v) }},
		{int64(cfg.MaxOpenBatches), disk.MaxOpenBatches, func(v int64) { cfg.MaxOpenBatches = int(v) }},
	}
	for _, p := range pairs {
		if p.disk > math.MaxInt64 || (p.supplied != 0 && uint64(p.supplied) != p.disk) {
			return base.ErrConfigMismatch
		}
		p.set(int64(p.disk))
	}
	if cfg.IDReserveSize != 0 && cfg.IDReserveSize != disk.IDReserveSize || cfg.BatchIDReserveSize != 0 && cfg.BatchIDReserveSize != disk.BatchIDReserveSize {
		return base.ErrConfigMismatch
	}
	cfg.IDReserveSize, cfg.BatchIDReserveSize = disk.IDReserveSize, disk.BatchIDReserveSize
	return nil
}

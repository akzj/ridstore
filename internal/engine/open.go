package engine

import (
	"context"
	"errors"
	"math"
	"time"

	"github.com/akzj/ridstore/internal/base"
	"github.com/akzj/ridstore/internal/bootstrap"
	"github.com/akzj/ridstore/internal/coordinator"
	"github.com/akzj/ridstore/internal/filelock"
	"github.com/akzj/ridstore/internal/idalloc"
	"github.com/akzj/ridstore/internal/maintstate"
	"github.com/akzj/ridstore/internal/mapping"
	"github.com/akzj/ridstore/internal/mapstore"
	"github.com/akzj/ridstore/internal/radix"
	"github.com/akzj/ridstore/internal/recordcodec"
	"github.com/akzj/ridstore/internal/recordlog"
	"github.com/akzj/ridstore/internal/replay"
	"github.com/akzj/ridstore/internal/storecatalog"
	"github.com/akzj/ridstore/internal/transaction"
)

type OpenConfig struct {
	RecordLog           recordlog.Config
	Commit              coordinator.Config
	MappingCacheBytes   uint64
	CheckpointSortBytes uint64
	MaxSegmentStats     uint64
	DeltaSoftLimitBytes uint64
	DeltaHardLimitBytes uint64
	StatusRetention     uint64
	WriteStopFreeBytes  uint64
	SpaceCheckInterval  time.Duration
}

type CreateConfig struct {
	HardLimits storecatalog.HardLimits
	Runtime    OpenConfig
}

func Create(ctx context.Context, root string, config CreateConfig) (*Store, error) {
	return create(ctx, root, config, nil, nil)
}

func create(ctx context.Context, root string, config CreateConfig, bootstrapHook bootstrap.FaultHook, catalogHook storecatalog.FaultHook) (*Store, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if root == "" || !validOpenConfig(config.Runtime) {
		return nil, base.ErrInvalidConfig
	}
	if err := bootstrap.ValidateHardLimits(config.HardLimits); err != nil {
		return nil, err
	}
	if config.Runtime.StatusRetention < config.HardLimits.MaxOpenBatches {
		return nil, base.ErrInvalidConfig
	}
	if err := bootstrap.EnsureRoot(root); err != nil {
		return nil, err
	}
	dirLock, err := filelock.Acquire(root)
	if err != nil {
		return nil, err
	}
	fail := func(cause error) (*Store, error) {
		return nil, errors.Join(cause, dirLock.Close())
	}
	if _, err := bootstrap.Initialize(root, config.HardLimits, bootstrapHook); err != nil {
		return fail(err)
	}
	store, err := openLocked(ctx, root, config.Runtime, openFaultHooks{catalog: catalogHook}, dirLock)
	if err != nil {
		return fail(err)
	}
	return store, nil
}

// Open assembles the v2 runtime directly from the authoritative Catalog. It
// opens the persistent Mapping root first and replays only the RecordLog tail
// after the Manifest cut into that same runtime Mapping.
func Open(ctx context.Context, root string, config OpenConfig) (*Store, error) {
	return open(ctx, root, config, openFaultHooks{})
}

type openFaultHooks struct {
	catalog     storecatalog.FaultHook
	mapStore    mapstore.FaultHook
	recordLog   recordlog.FaultHook
	maintenance maintstate.FaultHook
}

func open(ctx context.Context, root string, config OpenConfig, hooks openFaultHooks) (*Store, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if root == "" || !validOpenConfig(config) {
		return nil, base.ErrInvalidConfig
	}
	dirLock, err := filelock.Acquire(root)
	if err != nil {
		return nil, err
	}
	failLock := func(cause error) (*Store, error) {
		return nil, errors.Join(cause, dirLock.Close())
	}
	if err := bootstrap.RequireReady(root); err != nil {
		return failLock(err)
	}
	store, err := openLocked(ctx, root, config, hooks, dirLock)
	if err != nil {
		return failLock(err)
	}
	return store, nil
}

func openLocked(ctx context.Context, root string, config OpenConfig, hooks openFaultHooks, dirLock *filelock.Lock) (*Store, error) {
	catalog, err := storecatalog.OpenManager(root, hooks.catalog)
	if err != nil {
		return nil, err
	}
	if config.StatusRetention < catalog.Snapshot().HardLimits.MaxOpenBatches {
		return nil, base.ErrInvalidConfig
	}
	if err := recoverMaintenance(root, catalog, hooks.maintenance, hooks.recordLog); err != nil {
		return nil, err
	}
	log, err := recordlog.OpenWithFaultHook(root, config.RecordLog, catalog, hooks.recordLog)
	if err != nil {
		return nil, err
	}
	failLog := func(cause error) (*Store, error) {
		return nil, errors.Join(cause, log.Close())
	}
	physicalMapping, err := mapstore.OpenWithFaultHook(root, catalog, hooks.mapStore)
	if err != nil {
		return failLog(err)
	}
	fail := func(cause error) (*Store, error) {
		return nil, errors.Join(cause, physicalMapping.Close(), log.Close())
	}
	manifest := catalog.Snapshot()
	tree, err := radix.Open(physicalMapping, manifest.MappingRoot, manifest.CoveredCommitSeq, config.MappingCacheBytes)
	if err != nil {
		return fail(err)
	}
	current, err := mapping.OpenPersistent(tree, physicalMapping, persistentConfig(config))
	if err != nil {
		return fail(err)
	}
	recovered, err := replay.Recover(ctx, log, replay.Checkpoint{
		Mapping: current, ReplayStart: manifest.ReplayStart,
		ReservedIDHigh: manifest.ReservedIDHigh, ReservedBatchIDHigh: manifest.ReservedBatchIDHigh,
		OpenBatchIDs: manifest.OpenBatchIDsAtCut,
	}, replay.Config{
		MaxValueSize: manifest.HardLimits.MaxValueSize, MaxRecordPayload: manifest.HardLimits.MaxRecordLogPayload,
		MaxGroupDescriptors: uint64(manifest.HardLimits.MaxRecordLogPayload) / uint64(recordcodec.DescriptorHeadSize),
		MaxGroupMutations:   uint64(manifest.HardLimits.MaxRecordLogPayload) / uint64(recordcodec.MutationSize),
		IDReserveSize:       manifest.HardLimits.IDReserveSize, BatchIDReserveSize: manifest.HardLimits.BatchIDReserveSize,
		StatusCapacity: config.StatusRetention,
	})
	if err != nil {
		return fail(err)
	}
	ids, err := idalloc.New(idalloc.RecordID, manifest.HardLimits.IDReserveSize, recovered.ReservedIDHigh, log)
	if err != nil {
		return fail(err)
	}
	batches, err := idalloc.New(idalloc.BatchID, manifest.HardLimits.BatchIDReserveSize, recovered.ReservedBatchIDHigh, log)
	if err != nil {
		return fail(err)
	}
	if manifest.HardLimits.MaxOpenBatches > uint64(math.MaxInt) {
		return fail(base.ErrInvalidConfig)
	}
	store, err := New(log, current, ids, batches, Config{
		Batch: transaction.Limits{
			MaxValueSize: manifest.HardLimits.MaxValueSize, MaxBatchBytes: manifest.HardLimits.MaxBatchBytes,
			MaxBatchMutations: manifest.HardLimits.MaxBatchMutations, MaxBatchConditions: manifest.HardLimits.MaxBatchConditions,
		},
		Commit: config.Commit, MaxOpenBatches: int(manifest.HardLimits.MaxOpenBatches), StatusRetention: config.StatusRetention,
	})
	if err != nil {
		return fail(err)
	}
	store.mapStore = physicalMapping
	store.catalog = catalog
	store.maintenance = log
	if config.WriteStopFreeBytes != 0 {
		store.userAppender = &spaceAppender{
			next: log,
			gate: newSpaceGate(root, config.WriteStopFreeBytes, config.SpaceCheckInterval, filesystemAvailable),
		}
	}
	store.maintenanceHook = hooks.maintenance
	for id, status := range recovered.Statuses {
		state := BatchStateAborted
		if status.State == replay.BatchCommitted {
			state = BatchStateCommitted
		}
		store.addStatusLocked(BatchStatus{BatchID: id, State: state, CommitSeq: status.CommitSeq})
	}
	store.terminalTotal = recovered.TerminalCount
	store.recoveryAbortedStart = manifest.IssuedBatchIDHighAtCut
	store.recoveryAbortedEnd = recovered.ReservedBatchIDHigh
	store.recoveryAbortedValid = store.recoveryAbortedStart < store.recoveryAbortedEnd
	store.root = root
	store.maxStats = config.MaxSegmentStats
	store.dirLock = dirLock
	store.identity = [16]byte(manifest.StoreUUID)
	return store, nil
}

func validOpenConfig(config OpenConfig) bool {
	return config.MappingCacheBytes != 0 && config.MaxSegmentStats != 0 && config.StatusRetention != 0 &&
		(config.WriteStopFreeBytes == 0 || config.SpaceCheckInterval > 0) &&
		mapping.ValidatePersistentConfig(persistentConfig(config)) == nil
}

func persistentConfig(config OpenConfig) mapping.PersistentConfig {
	return mapping.PersistentConfig{
		CheckpointSortBytes: config.CheckpointSortBytes,
		DeltaSoftLimitBytes: config.DeltaSoftLimitBytes,
		DeltaHardLimitBytes: config.DeltaHardLimitBytes,
	}
}

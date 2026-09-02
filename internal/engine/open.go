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
	"github.com/akzj/ridstore/internal/mapgcstate"
	"github.com/akzj/ridstore/internal/mapping"
	"github.com/akzj/ridstore/internal/mapstore"
	"github.com/akzj/ridstore/internal/model"
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
	CheckpointInterval  time.Duration
	GCBatchBytes        uint64
	GCBatchMutations    uint64
	GCMinFreeBytes      uint64
	GCBytesPerSecond    uint64
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
	config.Runtime = normalizeGCRuntime(config.Runtime, config.HardLimits)
	if err := validateRuntimeAgainstHard(config.Runtime, config.HardLimits); err != nil {
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
		return nil, err
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
	mapGC       mapgcstate.FaultHook
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
		return nil, err
	}
	return store, nil
}

func openLocked(ctx context.Context, root string, config OpenConfig, hooks openFaultHooks, dirLock *filelock.Lock) (opened *Store, resultErr error) {
	runtimeOwnsLock := false
	defer func() {
		if resultErr != nil && !runtimeOwnsLock {
			resultErr = errors.Join(resultErr, dirLock.Close())
		}
	}()
	manifest, err := storecatalog.Load(root)
	if err != nil {
		return nil, err
	}
	config = normalizeGCRuntime(config, manifest.HardLimits)
	if err := validateRuntimeAgainstHard(config, manifest.HardLimits); err != nil {
		return nil, base.ErrInvalidConfig
	}
	catalog, err := storecatalog.OpenManager(root, hooks.catalog)
	if err != nil {
		return nil, err
	}
	if err := recoverMappingGC(ctx, root, catalog, hooks.mapGC, hooks.mapStore); err != nil {
		return nil, err
	}
	if err := recoverMaintenance(root, catalog, hooks.maintenance, hooks.recordLog); err != nil {
		return nil, err
	}
	pendingCompaction, err := recoverCompactionBeforeOpen(root, catalog)
	if err != nil {
		return nil, err
	}
	publisher := newPublishCoordinator(catalog)
	publisher.publish(catalog.Snapshot())
	log, err := recordlog.OpenWithFaultHook(root, config.RecordLog, publisher, hooks.recordLog)
	if err != nil {
		return nil, err
	}
	failLog := func(cause error) (*Store, error) {
		return nil, errors.Join(cause, log.Close())
	}
	physicalMapping, err := mapstore.OpenWithFaultHook(root, publisher, hooks.mapStore)
	if err != nil {
		return failLog(err)
	}
	fail := func(cause error) (*Store, error) {
		return nil, errors.Join(cause, physicalMapping.Close(), log.Close())
	}
	manifest = catalog.Snapshot()
	tree, err := radix.Open(physicalMapping, manifest.MappingRoot, manifest.CoveredCommitSeq, config.MappingCacheBytes)
	if err != nil {
		return fail(err)
	}
	checkpointState, err := mappingCheckpointState(manifest)
	if err != nil {
		return fail(err)
	}
	current, err := mapping.OpenPersistent(tree, physicalMapping, persistentConfig(config), checkpointState)
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
	if len(recovered.StatusOrder) != len(recovered.Statuses) {
		return fail(errors.Join(base.ErrCorrupt, errors.New("replay status order is incomplete")))
	}
	orderedStatuses := make(map[model.BatchID]struct{}, len(recovered.StatusOrder))
	for _, id := range recovered.StatusOrder {
		if _, ok := recovered.Statuses[id]; !ok {
			return fail(errors.Join(base.ErrCorrupt, errors.New("replay status order references missing batch")))
		}
		if _, duplicate := orderedStatuses[id]; duplicate {
			return fail(errors.Join(base.ErrCorrupt, errors.New("replay status order contains duplicate batch")))
		}
		orderedStatuses[id] = struct{}{}
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
	store.core.mapStore = physicalMapping
	store.core.catalog = catalog
	store.core.publisher = publisher
	store.core.compactionLog = log
	if config.WriteStopFreeBytes != 0 {
		store.core.space = newSpaceGate(root, config.WriteStopFreeBytes, config.SpaceCheckInterval, filesystemAvailable)
		store.core.userAppender = &spaceAppender{next: log, gate: store.core.space}
	}
	store.maintenance.stateHook = hooks.maintenance
	store.maintenance.mapStoreHook = hooks.mapStore
	store.maintenance.mappingGCHook = hooks.mapGC
	for _, id := range recovered.StatusOrder {
		status := recovered.Statuses[id]
		state := BatchStateAborted
		if status.State == replay.BatchCommitted {
			state = BatchStateCommitted
		}
		store.addStatusLocked(BatchStatus{BatchID: id, State: state, CommitSeq: status.CommitSeq})
	}
	store.state.terminalTotal = recovered.TerminalCount
	store.state.recoveryAbortedStart = manifest.IssuedBatchIDHighAtCut
	store.state.recoveryAbortedEnd = recovered.ReservedBatchIDHigh
	store.state.recoveryAbortedValid = store.state.recoveryAbortedStart < store.state.recoveryAbortedEnd
	store.core.root = root
	store.maintenance.maxStats = config.MaxSegmentStats
	store.maintenance.mappingCacheBytes = config.MappingCacheBytes
	store.maintenance.maxRelocationBytes = config.GCBatchBytes
	store.maintenance.maxRelocationMutations = min(store.maintenance.maxRelocationMutations, config.GCBatchMutations)
	store.maintenance.gcMinFreeBytes = config.GCMinFreeBytes
	store.maintenance.gcBytesPerSecond.Store(config.GCBytesPerSecond)
	store.maintenance.gcNow = time.Now
	store.maintenance.gcWait = waitContext
	store.checkpoints.interval = config.CheckpointInterval
	store.core.dirLock = dirLock
	runtimeOwnsLock = true
	store.core.identity = [16]byte(manifest.StoreUUID)
	initialPublished := catalog.Snapshot()
	publisher.publish(initialPublished)
	store.maintenance.gcStability.sample(initialPublished, store.maintenance.gcNow())
	store.startCheckpointWorker()
	if pendingCompaction != nil {
		if err := store.resumeCompaction(ctx, *pendingCompaction); err != nil {
			return nil, errors.Join(base.ErrRecoveryRequired, err, store.Close())
		}
	}
	return store, nil
}

func mappingCheckpointState(manifest storecatalog.Manifest) (mapping.PersistentState, error) {
	if manifest.StatsCoveredCommitSeq != manifest.CoveredCommitSeq {
		return mapping.PersistentState{}, base.ErrCorrupt
	}
	live := make(map[recordlog.SegmentID]mapping.SegmentLiveStats, len(manifest.SegmentStats))
	for _, stat := range manifest.SegmentStats {
		if stat.LiveBytes == 0 && stat.LiveRecords == 0 {
			continue
		}
		if stat.LiveBytes == 0 || stat.LiveRecords == 0 {
			return mapping.PersistentState{}, base.ErrCorrupt
		}
		live[stat.SegmentID] = mapping.SegmentLiveStats{
			LiveBytes: stat.LiveBytes, LiveRecords: stat.LiveRecords,
			LastChangedCommitSeq: manifest.StatsCoveredCommitSeq,
		}
	}
	return mapping.PersistentState{StatsCoveredCommitSeq: manifest.StatsCoveredCommitSeq, LiveStats: live}, nil
}

func validOpenConfig(config OpenConfig) bool {
	return config.MappingCacheBytes != 0 && config.MaxSegmentStats != 0 && config.StatusRetention != 0 &&
		(config.WriteStopFreeBytes == 0 || config.SpaceCheckInterval > 0) && config.CheckpointInterval > 0 &&
		coordinator.ValidateConfig(config.Commit) == nil &&
		mapping.ValidatePersistentConfig(persistentConfig(config)) == nil
}

func validateRuntimeAgainstHard(config OpenConfig, hard storecatalog.HardLimits) error {
	if !validOpenConfig(config) || config.StatusRetention < hard.MaxOpenBatches || config.Commit.MaxGroupPayload > hard.MaxRecordLogPayload ||
		config.GCBatchBytes == 0 || config.GCBatchBytes > hard.MaxBatchBytes || config.GCBatchMutations == 0 ||
		config.GCBatchMutations > hard.MaxBatchMutations || config.GCBytesPerSecond == 0 || config.GCMinFreeBytes > config.WriteStopFreeBytes {
		return base.ErrInvalidConfig
	}
	descriptor, err := recordcodec.DescriptorSize(hard.MaxBatchMutations)
	if err != nil || config.Commit.MaxGroupPayload < uint64(recordcodec.CommitGroupHeadSize)+uint64(descriptor) {
		return base.ErrInvalidConfig
	}
	return nil
}

func normalizeGCRuntime(config OpenConfig, hard storecatalog.HardLimits) OpenConfig {
	const defaultGCBatchBytes = uint64(16 << 20)
	const defaultGCBatchMutations = uint64(4096)
	const defaultGCBytesPerSecond = uint64(64 << 20)
	if config.GCBatchBytes == 0 {
		config.GCBatchBytes = min(defaultGCBatchBytes, hard.MaxBatchBytes)
	}
	if config.GCBatchMutations == 0 {
		config.GCBatchMutations = min(defaultGCBatchMutations, hard.MaxBatchMutations)
	}
	if config.GCMinFreeBytes == 0 && config.WriteStopFreeBytes != 0 {
		config.GCMinFreeBytes = min(hard.SegmentSize, config.WriteStopFreeBytes)
	}
	if config.GCBytesPerSecond == 0 {
		config.GCBytesPerSecond = defaultGCBytesPerSecond
	}
	return config
}

func persistentConfig(config OpenConfig) mapping.PersistentConfig {
	return mapping.PersistentConfig{
		CheckpointSortBytes: config.CheckpointSortBytes,
		DeltaSoftLimitBytes: config.DeltaSoftLimitBytes,
		DeltaHardLimitBytes: config.DeltaHardLimitBytes,
	}
}

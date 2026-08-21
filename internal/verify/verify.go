package verify

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"

	"github.com/akzj/ridstore/internal/base"
	"github.com/akzj/ridstore/internal/filelock"
	storeformat "github.com/akzj/ridstore/internal/format"
	"github.com/akzj/ridstore/internal/initialize"
	"github.com/akzj/ridstore/internal/manifest"
	"github.com/akzj/ridstore/internal/mapping/radix"
	"github.com/akzj/ridstore/internal/recovery"
	"github.com/akzj/ridstore/internal/segment"
)

type Report struct {
	Clean                   bool     `json:"clean"`
	StoreUUID               string   `json:"store_uuid,omitempty"`
	ManifestGeneration      uint64   `json:"manifest_generation,omitempty"`
	CoveredCommitSeq        uint64   `json:"covered_commit_seq,omitempty"`
	CurrentCommitSeq        uint64   `json:"current_commit_seq,omitempty"`
	DataFiles               uint64   `json:"data_files"`
	DataPhysicalBytes       uint64   `json:"data_physical_bytes"`
	Frames                  uint64   `json:"frames"`
	PutRecords              uint64   `json:"put_records"`
	LiveRecords             uint64   `json:"live_records"`
	LiveBytes               uint64   `json:"live_bytes"`
	DeadRecords             uint64   `json:"dead_records"`
	DeadBytes               uint64   `json:"dead_bytes"`
	SystemFrameBytes        uint64   `json:"system_frame_bytes"`
	MappingEntries          uint64   `json:"mapping_entries"`
	MappingTotalBytes       uint64   `json:"mapping_total_bytes"`
	MappingReachableBytes   uint64   `json:"mapping_reachable_bytes"`
	MappingUnreachableBytes uint64   `json:"mapping_unreachable_bytes"`
	TrashFiles              uint64   `json:"trash_files"`
	TrashBytes              uint64   `json:"trash_bytes"`
	Issues                  []string `json:"issues,omitempty"`
}

type dataScanner struct {
	id   base.DataSegmentID
	scan func(func(base.VAddr, storeformat.Frame) error) error
}

func (s dataScanner) SegmentID() base.DataSegmentID { return s.id }
func (s dataScanner) Scan(visit func(base.VAddr, storeformat.Frame) error) error {
	return s.scan(visit)
}

// Run performs an exclusive, offline, read-only verification. It refuses to
// recover journals or truncate active tails; operators must run normal Open to
// complete recovery before retrying verify.
func Run(ctx context.Context, root string) (report Report, resultErr error) {
	if err := ctx.Err(); err != nil {
		return report, err
	}
	abs, err := filepath.Abs(root)
	if err != nil {
		return report, err
	}
	lock, err := filelock.AcquireExisting(abs)
	if err != nil {
		return report, err
	}
	defer func() { resultErr = errors.Join(resultErr, lock.Close()) }()
	if dirty, err := dirtyArtifacts(abs); err != nil {
		return report, err
	} else if len(dirty) != 0 {
		report.Issues = append(report.Issues, "recovery artifacts: "+fmt.Sprint(dirty))
		return report, base.ErrRecoveryRequired
	}
	m, err := manifest.LoadCurrent(abs)
	if err != nil {
		return issue(report, err)
	}
	report.StoreUUID = hex.EncodeToString(m.StoreUUID[:])
	report.ManifestGeneration = m.Generation
	report.CoveredCommitSeq = uint64(m.CoveredCommitSeq)
	maxPayload, err := maxFramePayload(m.HardLimits)
	if err != nil {
		return issue(report, err)
	}
	if err := verifyOfficialFileSets(abs, m); err != nil {
		return issue(report, err)
	}
	mapping, err := radix.OpenReadOnly(abs, m, 8<<20)
	if err != nil {
		return issue(report, err)
	}
	defer func() { resultErr = errors.Join(resultErr, mapping.Close()) }()
	checkpointEntries := make(map[base.VAddr]base.ID)
	if err := mapping.WalkRoot(m.MappingRoot, m.CoveredCommitSeq, func(id base.ID, addr base.VAddr) error {
		if old, exists := checkpointEntries[addr]; exists && old != id {
			return fmt.Errorf("checkpoint Mapping aliases VAddr %x for IDs %d and %d: %w", addr, old, id, base.ErrCorrupt)
		}
		checkpointEntries[addr] = id
		return nil
	}); err != nil {
		return issue(report, err)
	}
	total, reachable, err := mapping.SpaceUsage(ctx)
	if err != nil || total < reachable {
		if err == nil {
			err = base.ErrCorrupt
		}
		return issue(report, err)
	}
	report.MappingTotalBytes, report.MappingReachableBytes, report.MappingUnreachableBytes = total, reachable, total-reachable
	sealedScanners := make([]recovery.DataScanner, len(m.SealedDataSegments))
	for i, summary := range m.SealedDataSegments {
		summary := summary
		id := base.DataSegmentID(summary.FileID)
		sealedScanners[i] = dataScanner{id: id, scan: func(visit func(base.VAddr, storeformat.Frame) error) error {
			return segment.InspectSealedData(abs, m.StoreUUID, summary, m.HardLimits.SegmentSize, maxPayload, func(addr base.VAddr, frame storeformat.Frame, _ uint64) error {
				return visit(addr, frame)
			})
		}}
	}
	activeID := m.ActiveDataSegmentID
	activeScanner := dataScanner{id: activeID, scan: func(visit func(base.VAddr, storeformat.Frame) error) error {
		return segment.InspectActiveData(abs, m.StoreUUID, activeID, m.HardLimits.SegmentSize, maxPayload, func(addr base.VAddr, frame storeformat.Frame, _ uint64) error {
			return visit(addr, frame)
		})
	}}
	recovered, err := recovery.RecoverIntoScanners(m, sealedScanners, activeScanner, mapping)
	if err != nil {
		return issue(report, err)
	}
	report.CurrentCommitSeq = uint64(recovered.NextCommitSeq - 1)
	current, err := mapping.Materialize()
	if err != nil {
		return issue(report, err)
	}
	report.MappingEntries = uint64(len(current.Entries))
	live := make(map[base.VAddr]base.ID, len(current.Entries))
	for id, addr := range current.Entries {
		if old, exists := live[addr]; exists && old != id {
			return issue(report, fmt.Errorf("current Mapping aliases VAddr %x for IDs %d and %d: %w", addr, old, id, base.ErrCorrupt))
		}
		live[addr] = id
	}
	checkpointStats := make(map[base.DataSegmentID]storeformat.SegmentStatsEntry)
	seenCheckpoint := make(map[base.VAddr]struct{}, len(checkpointEntries))
	seenLive := make(map[base.VAddr]struct{}, len(live))
	visit := func(addr base.VAddr, frame storeformat.Frame, physical uint64) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := add(&report.Frames, 1); err != nil {
			return err
		}
		if frame.Type != storeformat.FrameTypePutRecord {
			return add(&report.SystemFrameBytes, physical)
		}
		if err := add(&report.PutRecords, 1); err != nil {
			return err
		}
		if id, ok := checkpointEntries[addr]; ok {
			if id != frame.RecordID {
				return fmt.Errorf("checkpoint Mapping ID %d points to PutRecord ID %d at %x: %w", id, frame.RecordID, addr, base.ErrCorrupt)
			}
			if _, duplicate := seenCheckpoint[addr]; duplicate {
				return fmt.Errorf("duplicate checkpoint PutRecord address %x: %w", addr, base.ErrCorrupt)
			}
			seenCheckpoint[addr] = struct{}{}
			stat := checkpointStats[addr.SegmentID()]
			stat.SegmentID = addr.SegmentID()
			nextBytes, addErr := base.AddUint64(stat.ExactLiveBytes, physical)
			if addErr != nil {
				return addErr
			}
			nextRecords, addErr := base.AddUint64(stat.ExactLiveRecords, 1)
			if addErr != nil {
				return addErr
			}
			stat.ExactLiveBytes, stat.ExactLiveRecords = nextBytes, nextRecords
			checkpointStats[addr.SegmentID()] = stat
		}
		if id, ok := live[addr]; ok {
			if id != frame.RecordID {
				return fmt.Errorf("current Mapping ID %d points to PutRecord ID %d at %x: %w", id, frame.RecordID, addr, base.ErrCorrupt)
			}
			seenLive[addr] = struct{}{}
			if err := add(&report.LiveRecords, 1); err != nil {
				return err
			}
			if err := add(&report.LiveBytes, physical); err != nil {
				return err
			}
		} else {
			if err := add(&report.DeadRecords, 1); err != nil {
				return err
			}
			if err := add(&report.DeadBytes, physical); err != nil {
				return err
			}
		}
		return nil
	}
	for _, summary := range m.SealedDataSegments {
		if err := inspectSealedForReport(abs, m, summary, maxPayload, &report, visit); err != nil {
			return issue(report, err)
		}
	}
	if err := inspectActiveForReport(abs, m, maxPayload, &report, visit); err != nil {
		return issue(report, err)
	}
	if len(seenLive) != len(live) || len(seenCheckpoint) != len(checkpointEntries) {
		return issue(report, fmt.Errorf("Mapping references missing PutRecords current=%d/%d checkpoint=%d/%d: %w", len(seenLive), len(live), len(seenCheckpoint), len(checkpointEntries), base.ErrCorrupt))
	}
	if err := compareCheckpointStats(m.SegmentStats, checkpointStats); err != nil {
		return issue(report, err)
	}
	if err := inspectTrash(abs, &report); err != nil {
		return issue(report, err)
	}
	if report.TrashFiles != 0 {
		return issue(report, fmt.Errorf("trash contains %d files without maintenance journal: %w", report.TrashFiles, base.ErrCorrupt))
	}
	report.Clean = true
	return report, nil
}

func inspectSealedForReport(root string, m storeformat.Manifest, summary storeformat.FileSummary, maxPayload uint64, report *Report, visit segment.FrameVisitor) error {
	path := filepath.Join(root, "data", segment.SealedDataFileName(base.DataSegmentID(summary.FileID)))
	info, err := os.Stat(path)
	if err != nil {
		return err
	}
	if err := add(&report.DataFiles, 1); err != nil {
		return err
	}
	if err := add(&report.DataPhysicalBytes, uint64(info.Size())); err != nil {
		return err
	}
	return segment.InspectSealedData(root, m.StoreUUID, summary, m.HardLimits.SegmentSize, maxPayload, visit)
}

func inspectActiveForReport(root string, m storeformat.Manifest, maxPayload uint64, report *Report, visit segment.FrameVisitor) error {
	path := filepath.Join(root, "data", segment.ActiveDataFileName(m.ActiveDataSegmentID))
	info, err := os.Stat(path)
	if err != nil {
		return err
	}
	if err := add(&report.DataFiles, 1); err != nil {
		return err
	}
	if err := add(&report.DataPhysicalBytes, uint64(info.Size())); err != nil {
		return err
	}
	return segment.InspectActiveData(root, m.StoreUUID, m.ActiveDataSegmentID, m.HardLimits.SegmentSize, maxPayload, visit)
}

func compareCheckpointStats(want []storeformat.SegmentStatsEntry, got map[base.DataSegmentID]storeformat.SegmentStatsEntry) error {
	if len(want) != len(got) {
		return fmt.Errorf("checkpoint SegmentStats count want=%d got=%d: %w", len(want), len(got), base.ErrCorrupt)
	}
	for _, expected := range want {
		actual, ok := got[expected.SegmentID]
		if !ok || actual != expected {
			return fmt.Errorf("checkpoint SegmentStats segment=%d want=%+v got=%+v: %w", expected.SegmentID, expected, actual, base.ErrCorrupt)
		}
	}
	return nil
}

func dirtyArtifacts(root string) ([]string, error) {
	paths := []string{
		initialize.MarkerFileName, ".INITIALIZING.tmp", ".CURRENT.tmp",
		filepath.Join("journal", "MAINTENANCE"), filepath.Join("journal", ".MAINTENANCE.tmp"),
		filepath.Join("journal", "ROTATION"), filepath.Join("journal", ".ROTATION.tmp"),
	}
	var found []string
	for _, name := range paths {
		if _, err := os.Lstat(filepath.Join(root, name)); err == nil {
			found = append(found, name)
		} else if !errors.Is(err, os.ErrNotExist) {
			return nil, err
		}
	}
	return found, nil
}

func verifyOfficialFileSets(root string, m storeformat.Manifest) error {
	data := map[string]struct{}{segment.ActiveDataFileName(m.ActiveDataSegmentID): {}}
	for _, summary := range m.SealedDataSegments {
		data[segment.SealedDataFileName(base.DataSegmentID(summary.FileID))] = struct{}{}
	}
	mapping := map[string]struct{}{fmt.Sprintf("MAP-%08d.active", m.ActiveMapSegmentID): {}}
	for _, summary := range m.SealedMappingSegments {
		mapping[fmt.Sprintf("MAP-%08d.seg", summary.FileID)] = struct{}{}
	}
	if err := exactRegularFiles(filepath.Join(root, "data"), data); err != nil {
		return err
	}
	return exactRegularFiles(filepath.Join(root, "mapping"), mapping)
}

func exactRegularFiles(dir string, expected map[string]struct{}) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if _, ok := expected[entry.Name()]; !ok {
			return fmt.Errorf("unexpected file %s: %w", filepath.Join(dir, entry.Name()), base.ErrCorrupt)
		}
		info, err := entry.Info()
		if err != nil || !info.Mode().IsRegular() {
			return fmt.Errorf("non-regular storage file %s: %w", filepath.Join(dir, entry.Name()), base.ErrCorrupt)
		}
		delete(expected, entry.Name())
	}
	if len(expected) != 0 {
		missing := make([]string, 0, len(expected))
		for name := range expected {
			missing = append(missing, name)
		}
		sort.Strings(missing)
		return fmt.Errorf("missing files in %s: %v: %w", dir, missing, base.ErrCorrupt)
	}
	return nil
}

func inspectTrash(root string, report *Report) error {
	entries, err := os.ReadDir(filepath.Join(root, "trash"))
	if err != nil {
		return err
	}
	for _, entry := range entries {
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("non-regular trash entry %s: %w", entry.Name(), base.ErrCorrupt)
		}
		if err := add(&report.TrashFiles, 1); err != nil {
			return err
		}
		if err := add(&report.TrashBytes, uint64(info.Size())); err != nil {
			return err
		}
	}
	return nil
}

func maxFramePayload(h storeformat.HardLimits) (uint64, error) {
	descriptorBytes, err := base.MulUint64(h.MaxBatchMutations, storeformat.MutationEntrySize)
	if err != nil {
		return 0, err
	}
	minimumSegmentBytes := uint64(storeformat.SegmentHeaderSize + storeformat.SegmentFooterSize + storeformat.FrameHeaderSize)
	if h.SegmentSize <= minimumSegmentBytes {
		return 0, base.ErrCorrupt
	}
	contentBytes := h.SegmentSize - storeformat.SegmentHeaderSize - storeformat.SegmentFooterSize
	frameCapacity := contentBytes - storeformat.FrameHeaderSize
	if frameCapacity > uint64(math.MaxUint32)-storeformat.FrameHeaderSize {
		frameCapacity = uint64(math.MaxUint32) - storeformat.FrameHeaderSize
	}
	maxPart := descriptorBytes
	if maxPart > frameCapacity {
		maxPart = frameCapacity - frameCapacity%storeformat.MutationEntrySize
	}
	maxFrame := h.MaxValueSize
	if maxPart > maxFrame {
		maxFrame = maxPart
	}
	if maxFrame < storeformat.DescriptorSealSize {
		maxFrame = storeformat.DescriptorSealSize
	}
	if maxFrame > frameCapacity || maxPart < storeformat.MutationEntrySize {
		return 0, base.ErrCorrupt
	}
	return maxFrame, nil
}

func issue(report Report, err error) (Report, error) {
	report.Clean = false
	report.Issues = append(report.Issues, err.Error())
	return report, err
}

func add(dst *uint64, value uint64) error {
	next, err := base.AddUint64(*dst, value)
	if err != nil {
		return err
	}
	*dst = next
	return nil
}

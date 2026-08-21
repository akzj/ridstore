package format

import (
	"errors"
	"reflect"
	"testing"

	"github.com/akzj/ridstore/internal/base"
)

func TestInitializingMarkerRoundTrip(t *testing.T) {
	t.Parallel()
	m := InitializingMarker{StoreUUID: testStoreUUID, HardLimits: testManifest().HardLimits, Phase: InitializingDataHeaderDurable}
	b, err := EncodeInitializingMarker(m)
	if err != nil {
		t.Fatal(err)
	}
	assertGoldenSHA256(t, b, "7771c5b9f7ab6d53e2cf13341d240d159885959cda3a4fde91495a8cfb830073")
	got, err := DecodeInitializingMarker(b)
	if err != nil || got != m {
		t.Fatalf("got=%+v err=%v", got, err)
	}
}

func TestInitializingTransition(t *testing.T) {
	t.Parallel()
	old := InitializingMarker{StoreUUID: testStoreUUID, HardLimits: testManifest().HardLimits, Phase: InitializingPrepared}
	next := old
	next.Phase++
	if err := ValidateInitializingTransition(old, next); err != nil {
		t.Fatal(err)
	}
	next.Phase++
	if err := ValidateInitializingTransition(old, next); !errors.Is(err, base.ErrInvalidConfig) {
		t.Fatalf("phase jump error=%v", err)
	}
	next = old
	next.StoreUUID[0]++
	if err := ValidateInitializingTransition(old, next); !errors.Is(err, base.ErrInvalidConfig) {
		t.Fatalf("identity change error=%v", err)
	}
}

func TestMaintenanceJournalRoundTripAndTransition(t *testing.T) {
	t.Parallel()
	j := MaintenanceJournal{Generation: 2, StoreUUID: testStoreUUID, OperationID: [16]byte{1}, OperationType: MaintenanceDataGC, Phase: 1, OldManifestGeneration: 1, SourceFiles: []JournalFileRef{{Kind: FileKindData, State: FileStateSealed, FileID: 1, ValidEnd: 8192, FirstSeq: 1, LastSeq: 2}}}
	b, err := EncodeMaintenanceJournal(j)
	if err != nil {
		t.Fatal(err)
	}
	assertGoldenSHA256(t, b, "4c00575f2f36c3ec2979895559e12e478b1ad57380ad0434ca723ee9494c0ef2")
	got, err := DecodeMaintenanceJournal(b)
	if err != nil || !reflect.DeepEqual(got, j) {
		t.Fatalf("got=%+v err=%v", got, err)
	}
	next := j
	next.Phase = 2
	next.DestinationFiles = []JournalFileRef{{Kind: FileKindData, State: FileStateActive, FileID: 2, ValidEnd: 4096, FirstSeq: 3, LastSeq: 3}}
	if err := ValidateMaintenanceTransition(j, next); err != nil {
		t.Fatal(err)
	}
	extended := next
	extended.DestinationFiles[0].ValidEnd += 8
	extended.DestinationFiles[0].LastSeq++
	if err := ValidateMaintenanceTransition(next, extended); err != nil {
		t.Fatalf("durable extent growth: %v", err)
	}
	shortened := extended
	shortened.DestinationFiles = append([]JournalFileRef(nil), extended.DestinationFiles...)
	shortened.DestinationFiles[0].ValidEnd -= 16
	if err := ValidateMaintenanceTransition(extended, shortened); !errors.Is(err, base.ErrInvalidConfig) {
		t.Fatalf("durable extent shrink error=%v", err)
	}
	next.Phase = 4
	next.NewManifestGeneration = 2
	if err := ValidateMaintenanceTransition(j, next); !errors.Is(err, base.ErrInvalidConfig) {
		t.Fatalf("jump error=%v", err)
	}
}

func TestMaintenancePhaseManifestRules(t *testing.T) {
	t.Parallel()
	j := MaintenanceJournal{Generation: 2, StoreUUID: testStoreUUID, OperationID: [16]byte{1}, OperationType: MaintenanceMappingCheckpoint, Phase: 3, OldManifestGeneration: 1, NewManifestGeneration: 2}
	if _, err := EncodeMaintenanceJournal(j); !errors.Is(err, base.ErrInvalidConfig) {
		t.Fatalf("premature manifest error=%v", err)
	}
	j.Phase = 4
	j.NewManifestGeneration = 0
	if _, err := EncodeMaintenanceJournal(j); !errors.Is(err, base.ErrInvalidConfig) {
		t.Fatalf("missing manifest error=%v", err)
	}
}

func TestMaintenanceFileRefLifecycleTransition(t *testing.T) {
	t.Parallel()
	old := MaintenanceJournal{
		Generation: 2, StoreUUID: testStoreUUID, OperationID: [16]byte{1}, OperationType: MaintenanceDataGC, Phase: 3, OldManifestGeneration: 1,
		SourceFiles:      []JournalFileRef{{Kind: FileKindMapping, State: FileStateActive, FileID: 1, ValidEnd: 8192, FirstSeq: 1, LastSeq: 2}},
		DestinationFiles: []JournalFileRef{{Kind: FileKindMapping, State: FileStateTemporary, FileID: 2, ValidEnd: 4096, FirstSeq: 3, LastSeq: 3}},
	}
	next := old
	next.SourceFiles = append([]JournalFileRef(nil), old.SourceFiles...)
	next.DestinationFiles = append([]JournalFileRef(nil), old.DestinationFiles...)
	next.SourceFiles[0].State = FileStateSealed
	next.DestinationFiles[0].State = FileStateActive
	if err := ValidateMaintenanceTransition(old, next); err != nil {
		t.Fatal(err)
	}
	regressed := next
	regressed.SourceFiles = append([]JournalFileRef(nil), next.SourceFiles...)
	regressed.SourceFiles[0].State = FileStateActive
	if err := ValidateMaintenanceTransition(next, regressed); !errors.Is(err, base.ErrInvalidConfig) {
		t.Fatalf("state regression error=%v", err)
	}
}

func TestRotationJournalRoundTrip(t *testing.T) {
	t.Parallel()
	j := RotationJournal{StoreUUID: testStoreUUID, OldSegmentID: 1, NewSegmentID: 2, BaseManifestGeneration: 5, Phase: 4}
	b, err := EncodeRotationJournal(j)
	if err != nil {
		t.Fatal(err)
	}
	assertGoldenSHA256(t, b, "03f0d10f87c2c0d2bdc2504af5c250c320f80a8f5750ebba625a897917cacda8")
	got, err := DecodeRotationJournal(b)
	if err != nil || got != j {
		t.Fatalf("got=%+v err=%v", got, err)
	}
	j.Phase = 5
	j.InstalledManifestGeneration = 6
	if _, err := EncodeRotationJournal(j); err != nil {
		t.Fatal(err)
	}
}

func TestRotationTransition(t *testing.T) {
	t.Parallel()
	old := RotationJournal{StoreUUID: testStoreUUID, OldSegmentID: 1, NewSegmentID: 2, BaseManifestGeneration: 5, Phase: 4}
	next := old
	next.Phase = 5
	next.InstalledManifestGeneration = 6
	if err := ValidateRotationTransition(old, next); err != nil {
		t.Fatal(err)
	}
	changed := next
	changed.InstalledManifestGeneration = 7
	if err := ValidateRotationTransition(next, changed); !errors.Is(err, base.ErrInvalidConfig) {
		t.Fatalf("installed generation change error=%v", err)
	}
	jump := old
	jump.Phase = 5
	jump.InstalledManifestGeneration = 6
	old.Phase = 3
	if err := ValidateRotationTransition(old, jump); !errors.Is(err, base.ErrInvalidConfig) {
		t.Fatalf("phase jump error=%v", err)
	}
}

func FuzzDecodeJournals(f *testing.F) {
	m, _ := EncodeInitializingMarker(InitializingMarker{StoreUUID: testStoreUUID, HardLimits: testManifest().HardLimits, Phase: InitializingPrepared})
	r, _ := EncodeRotationJournal(RotationJournal{StoreUUID: testStoreUUID, OldSegmentID: 1, NewSegmentID: 2, BaseManifestGeneration: 1, Phase: 1})
	j, _ := EncodeMaintenanceJournal(MaintenanceJournal{Generation: 1, StoreUUID: testStoreUUID, OperationID: [16]byte{1}, OperationType: MaintenanceDataGC, Phase: 1, OldManifestGeneration: 1})
	f.Add(byte(0), m)
	f.Add(byte(1), r)
	f.Add(byte(2), j)
	f.Fuzz(func(t *testing.T, kind byte, data []byte) {
		switch kind % 3 {
		case 0:
			_, _ = DecodeInitializingMarker(data)
		case 1:
			_, _ = DecodeRotationJournal(data)
		case 2:
			_, _ = DecodeMaintenanceJournal(data)
		}
	})
}

package recordlog

import (
	"os"
	"os/exec"
	"testing"
)

const (
	crashHelperEnv = "RIDSTORE_RECORDLOG_CRASH_HELPER"
	crashRootEnv   = "RIDSTORE_RECORDLOG_CRASH_ROOT"
	crashPhaseEnv  = "RIDSTORE_RECORDLOG_CRASH_PHASE"
)

func TestRotationRecoveryAcrossProcessExit(t *testing.T) {
	for _, phase := range []string{"journal", "sealed", "new-active"} {
		t.Run(phase, func(t *testing.T) {
			root := t.TempDir()
			command := exec.Command(os.Args[0], "-test.run=^TestRecordLogRotationCrashHelper$")
			command.Env = append(os.Environ(), crashHelperEnv+"=1", crashRootEnv+"="+root, crashPhaseEnv+"="+phase)
			if output, err := command.CombinedOutput(); err != nil {
				t.Fatalf("helper: %v\n%s", err, output)
			}
			state := initialCatalog(1024, 512)
			catalog := &memoryCatalog{state: state}
			log, err := Open(root, testLogConfig(), catalog)
			if err != nil {
				t.Fatal(err)
			}
			installed := catalog.SnapshotRecordLog()
			if installed.Generation != 2 || installed.ActiveSegmentID != 2 || len(installed.SealedSegments) != 1 {
				t.Fatalf("catalog=%+v", installed)
			}
			if err := log.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestRecordLogRotationCrashHelper(t *testing.T) {
	if os.Getenv(crashHelperEnv) != "1" {
		t.Skip("subprocess helper")
	}
	root := os.Getenv(crashRootEnv)
	state := initialCatalog(1024, 512)
	active, err := createActiveSegment(root, state.headerFor(1), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	appendTestRecords(t, active, []byte("survives process exit"))
	if err := active.sync(); err != nil {
		t.Fatal(err)
	}
	journal := rotationJournal{
		BaseGeneration: 1, LogID: state.LogID, SegmentSize: state.SegmentSize,
		Old: active.summary(), NewActive: 2, NextSegmentID: 3,
	}
	if err := installRotationJournal(root, journal, osFileBackend{}, nil); err != nil {
		t.Fatal(err)
	}
	if os.Getenv(crashPhaseEnv) == "journal" {
		return
	}
	if _, _, err := active.seal(); err != nil {
		t.Fatal(err)
	}
	if os.Getenv(crashPhaseEnv) == "sealed" {
		return
	}
	created, err := createActiveSegment(root, state.headerFor(2), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := created.close(); err != nil {
		t.Fatal(err)
	}
}

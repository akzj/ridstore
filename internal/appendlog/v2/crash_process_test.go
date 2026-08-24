package v2

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

const crashExitCode = 77

func TestCrashProcessRecovery(t *testing.T) {
	cases := []struct {
		name     string
		scenario string
		point    FaultPoint
	}{
		{name: "create-header-write", scenario: "create", point: FaultBeforeHeaderWrite},
		{name: "create-header-sync", scenario: "create", point: FaultBeforeHeaderSync},
		{name: "create-active-rename", scenario: "create", point: FaultBeforeActiveRename},
		{name: "create-directory-sync", scenario: "create", point: FaultBeforeCreateDirSync},
		{name: "append-write", scenario: "rotate", point: FaultBeforeAppendWrite},
		{name: "append-sync", scenario: "rotate", point: FaultBeforeSync},
		{name: "footer-write", scenario: "rotate", point: FaultBeforeFooterWrite},
		{name: "footer-sync", scenario: "rotate", point: FaultBeforeFooterSync},
		{name: "seal-rename", scenario: "rotate", point: FaultBeforeRename},
		{name: "seal-directory-sync", scenario: "rotate", point: FaultBeforeSealDirSync},
		{name: "tail-truncate", scenario: "tail", point: FaultBeforeTailTruncate},
		{name: "tail-sync", scenario: "tail", point: FaultBeforeTailSync},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			command := exec.Command(os.Args[0], "-test.run=^TestCrashProcessHelper$")
			command.Env = append(os.Environ(),
				"RIDSTORE_V2_CRASH_HELPER=1",
				"RIDSTORE_V2_CRASH_DIR="+dir,
				"RIDSTORE_V2_CRASH_SCENARIO="+tc.scenario,
				"RIDSTORE_V2_CRASH_POINT="+string(tc.point),
			)
			err := command.Run()
			var exitErr *exec.ExitError
			if !errors.As(err, &exitErr) || exitErr.ExitCode() != crashExitCode {
				t.Fatalf("helper error = %v", err)
			}
			cfg := crashTestConfig(dir)
			log, err := Open(cfg)
			if err != nil {
				t.Fatal(err)
			}
			if tc.scenario != "create" {
				foundDurable := false
				if err := log.Scan(context.Background(), 0, func(_ VAddr, payload []byte) error {
					if string(payload) == "durable" || string(payload) == "prefix-durable" {
						foundDurable = true
					}
					return nil
				}); err != nil {
					t.Fatal(err)
				}
				if !foundDurable {
					t.Fatal("acknowledged durable prefix was not recovered")
				}
			}
			if _, err := log.Append(context.Background(), []byte("after-crash"), true); err != nil {
				t.Fatal(err)
			}
			if err := log.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestCrashProcessHelper(t *testing.T) {
	if os.Getenv("RIDSTORE_V2_CRASH_HELPER") != "1" {
		return
	}
	dir := os.Getenv("RIDSTORE_V2_CRASH_DIR")
	scenario := os.Getenv("RIDSTORE_V2_CRASH_SCENARIO")
	point := FaultPoint(os.Getenv("RIDSTORE_V2_CRASH_POINT"))
	cfg := crashTestConfig(dir)
	armed := scenario == "create"
	cfg.FaultHook = func(got FaultPoint) error {
		if armed && got == point {
			os.Exit(crashExitCode)
		}
		return nil
	}
	log, err := Open(cfg)
	if err != nil {
		os.Exit(2)
	}
	if scenario == "tail" {
		addr, appendErr := log.Append(context.Background(), []byte("durable"), true)
		if appendErr != nil || log.Close() != nil {
			os.Exit(7)
		}
		path := filepath.Join(dir, activeSegmentName(addr.SegmentID()))
		file, openErr := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
		if openErr != nil {
			os.Exit(8)
		}
		_, writeErr := file.Write([]byte{1, 2, 3})
		closeErr := file.Close()
		if writeErr != nil || closeErr != nil {
			os.Exit(9)
		}
		armed = true
		_, _ = Open(cfg)
		os.Exit(10)
	}
	if scenario != "rotate" {
		os.Exit(3)
	}
	if _, err := log.Append(context.Background(), make([]byte, 128), false); err != nil {
		os.Exit(4)
	}
	if _, err := log.Append(context.Background(), make([]byte, 128), false); err != nil {
		os.Exit(5)
	}
	if _, err := log.Append(context.Background(), []byte("prefix-durable"), true); err != nil {
		os.Exit(11)
	}
	armed = true
	_, _ = log.Append(context.Background(), make([]byte, 128), true)
	os.Exit(6)
}

func crashTestConfig(dir string) Config {
	cfg := DefaultConfig(dir)
	cfg.SegmentSize = 512
	cfg.MaxPayloadSize = 128
	cfg.MaxBufferBytes = 256
	cfg.MaxBufferRecords = 32
	cfg.ChannelCapacity = 64
	cfg.MaxQueuedBytes = 1024
	return cfg
}

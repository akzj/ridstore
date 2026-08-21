package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"runtime/debug"
	"syscall"
	"time"

	"github.com/akzj/ridstore/internal/soak"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	os.Exit(run(ctx, os.Args[1:], os.Stderr))
}

func run(ctx context.Context, args []string, stderr io.Writer) int {
	flags := flag.NewFlagSet("ridstore-soak", flag.ContinueOnError)
	flags.SetOutput(stderr)
	dir := flags.String("dir", "", "new ridstore directory used only by this run")
	report := flags.String("report", "", "new JSONL report file")
	duration := flags.Duration("duration", 72*time.Hour, "natural workload duration")
	sampleInterval := flags.Duration("sample-interval", time.Minute, "resource sample interval")
	maintenanceInterval := flags.Duration("maintenance-interval", 5*time.Minute, "checkpoint/GC interval")
	maintenanceBatches := flags.Uint64("maintenance-batches", 128, "maximum committed batches between maintenance cycles")
	liveRecords := flags.Int("live-records", 10_000, "bounded stable-ID working set")
	batchMutations := flags.Int("batch-mutations", 64, "mutations per durable batch")
	valueBytes := flags.Int("value-bytes", 1024, "value size")
	seed := flags.Int64("seed", 1, "deterministic workload seed")
	segmentSize := flags.Int64("segment-size", 16<<20, "data and mapping segment size")
	gitCommit := flags.String("git-commit", buildRevision(), "Git commit under test")
	if err := flags.Parse(args); err != nil || flags.NArg() != 0 || *dir == "" || *report == "" {
		if err == nil {
			fmt.Fprintln(stderr, "usage: ridstore-soak --dir <new-store-directory> --report <new-report.jsonl> [options]")
		}
		return 2
	}
	file, err := os.OpenFile(*report, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		fmt.Fprintf(stderr, "create report: %v\n", err)
		return 1
	}
	_, runErr := soak.Run(ctx, soak.Options{Dir: *dir, Duration: *duration, SampleInterval: *sampleInterval, MaintenanceInterval: *maintenanceInterval, MaintenanceBatches: *maintenanceBatches, LiveRecords: *liveRecords, BatchMutations: *batchMutations, ValueBytes: *valueBytes, Seed: *seed, SegmentSize: *segmentSize, GitCommit: *gitCommit}, file)
	syncErr := file.Sync()
	closeErr := file.Close()
	if err := errors.Join(runErr, syncErr, closeErr); err != nil {
		fmt.Fprintf(stderr, "soak: %v\n", err)
		return 1
	}
	return 0
}

func buildRevision() string {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return "unknown"
	}
	for _, setting := range info.Settings {
		if setting.Key == "vcs.revision" {
			return setting.Value
		}
	}
	return "unknown"
}

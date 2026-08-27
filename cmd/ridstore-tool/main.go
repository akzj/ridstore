package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	"github.com/akzj/ridstore"
	"github.com/akzj/ridstore/internal/migration"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	os.Exit(run(ctx, os.Args[1:], os.Stdout, os.Stderr))
}

func run(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	if len(args) >= 1 && args[0] == "verify" {
		return runVerify(ctx, args[1:], stdout, stderr)
	}
	if len(args) >= 2 && args[0] == "migrate" && args[1] == "plan" {
		return runMigratePlan(ctx, args[2:], stdout, stderr)
	}
	printUsage(stderr)
	return 2
}

func runMigratePlan(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("migrate plan", flag.ContinueOnError)
	flags.SetOutput(stderr)
	dir := flags.String("dir", "", "offline ridstore directory to inspect")
	if err := flags.Parse(args); err != nil || *dir == "" || flags.NArg() != 0 {
		if err == nil {
			printUsage(stderr)
		}
		return 2
	}
	plan, err := migration.Inspect(ctx, *dir)
	encoder := json.NewEncoder(stdout)
	encoder.SetIndent("", "  ")
	if encodeErr := encoder.Encode(plan); encodeErr != nil {
		fmt.Fprintf(stderr, "encode plan: %v\n", encodeErr)
		return 1
	}
	if err != nil {
		fmt.Fprintf(stderr, "migrate plan: %v\n", err)
		return 1
	}
	return 0
}

type verifyOutput struct {
	Clean  bool                  `json:"clean"`
	Report ridstore.VerifyReport `json:"report"`
}

func runVerify(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("verify", flag.ContinueOnError)
	flags.SetOutput(stderr)
	dir := flags.String("dir", "", "closed ridstore directory to verify")
	cache := flags.Uint64("mapping-cache-bytes", 256<<20, "maximum Mapping cache bytes")
	live := flags.Uint64("max-live-ids", 1<<20, "maximum live IDs retained by exact verification")
	statuses := flags.Uint64("status-limit", 1<<16, "maximum replayed terminal batch statuses")
	if err := flags.Parse(args); err != nil || *dir == "" || *cache == 0 || *live == 0 || *statuses == 0 || flags.NArg() != 0 {
		if err == nil {
			printUsage(stderr)
		}
		return 2
	}
	report, err := ridstore.Verify(ctx, ridstore.VerifyConfig{
		Dir: *dir, MappingCacheBytes: *cache, MaxLiveIDs: *live, MaxReplayStatuses: *statuses,
	})
	clean := err == nil && report.Stage == ridstore.VerifyStageExact
	encoder := json.NewEncoder(stdout)
	encoder.SetIndent("", "  ")
	if encodeErr := encoder.Encode(verifyOutput{Clean: clean, Report: report}); encodeErr != nil {
		fmt.Fprintf(stderr, "encode verify report: %v\n", encodeErr)
		return 1
	}
	if err != nil {
		fmt.Fprintf(stderr, "verify: %v\n", err)
		return 1
	}
	if !clean {
		fmt.Fprintf(stderr, "verify: terminal stage %q is not exact\n", report.Stage)
		return 1
	}
	return 0
}

func printUsage(output io.Writer) {
	fmt.Fprintln(output, "usage:")
	fmt.Fprintln(output, "  ridstore-tool verify --dir <offline-store-directory> [--mapping-cache-bytes <n>] [--max-live-ids <n>] [--status-limit <n>]")
	fmt.Fprintln(output, "  ridstore-tool migrate plan --dir <offline-store-directory>")
}

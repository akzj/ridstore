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

	"github.com/akzj/ridstore/internal/migration"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	os.Exit(run(ctx, os.Args[1:], os.Stdout, os.Stderr))
}

func run(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	if len(args) < 2 || args[0] != "migrate" || args[1] != "plan" {
		printUsage(stderr)
		return 2
	}
	flags := flag.NewFlagSet("migrate plan", flag.ContinueOnError)
	flags.SetOutput(stderr)
	dir := flags.String("dir", "", "offline ridstore directory to inspect")
	if err := flags.Parse(args[2:]); err != nil || *dir == "" || flags.NArg() != 0 {
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

func printUsage(output io.Writer) {
	fmt.Fprintln(output, "usage: ridstore-tool migrate plan --dir <offline-store-directory>")
}

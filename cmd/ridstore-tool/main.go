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

	"github.com/akzj/ridstore/internal/backup"
	"github.com/akzj/ridstore/internal/verify"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	os.Exit(run(ctx, os.Args[1:], os.Stdout, os.Stderr))
}

func run(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		printUsage(stderr)
		return 2
	}
	switch args[0] {
	case "verify":
		return runVerify(ctx, args[1:], stdout, stderr)
	case "backup":
		return runBackup(ctx, args[1:], stdout, stderr)
	default:
		printUsage(stderr)
		return 2
	}
}

func runVerify(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("verify", flag.ContinueOnError)
	flags.SetOutput(stderr)
	dir := flags.String("dir", "", "ridstore directory to verify offline")
	if err := flags.Parse(args); err != nil || *dir == "" || flags.NArg() != 0 {
		if err == nil {
			fmt.Fprintln(stderr, "usage: ridstore-tool verify --dir <store-directory>")
		}
		return 2
	}
	report, err := verify.Run(ctx, *dir)
	if encodeErr := writeJSON(stdout, report); encodeErr != nil {
		fmt.Fprintf(stderr, "encode report: %v\n", encodeErr)
		return 1
	}
	if err != nil {
		fmt.Fprintf(stderr, "verify: %v\n", err)
		return 1
	}
	return 0
}

func runBackup(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("backup", flag.ContinueOnError)
	flags.SetOutput(stderr)
	source := flags.String("source", "", "offline ridstore source directory")
	destination := flags.String("dest", "", "new backup artifact directory")
	if err := flags.Parse(args); err != nil || *source == "" || *destination == "" || flags.NArg() != 0 {
		if err == nil {
			fmt.Fprintln(stderr, "usage: ridstore-tool backup --source <store-directory> --dest <new-backup-directory>")
		}
		return 2
	}
	report, err := backup.Create(ctx, *source, *destination)
	if encodeErr := writeJSON(stdout, report); encodeErr != nil {
		fmt.Fprintf(stderr, "encode report: %v\n", encodeErr)
		return 1
	}
	if err != nil {
		fmt.Fprintf(stderr, "backup: %v\n", err)
		return 1
	}
	return 0
}

func writeJSON(output io.Writer, value any) error {
	encoder := json.NewEncoder(output)
	encoder.SetIndent("", "  ")
	return encoder.Encode(value)
}

func printUsage(output io.Writer) {
	fmt.Fprintln(output, "usage:")
	fmt.Fprintln(output, "  ridstore-tool verify --dir <store-directory>")
	fmt.Fprintln(output, "  ridstore-tool backup --source <store-directory> --dest <new-backup-directory>")
}

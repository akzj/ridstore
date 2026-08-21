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

	"github.com/akzj/ridstore/internal/verify"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	os.Exit(run(ctx, os.Args[1:], os.Stdout, os.Stderr))
}

func run(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 || args[0] != "verify" {
		fmt.Fprintln(stderr, "usage: ridstore-tool verify --dir <store-directory>")
		return 2
	}
	flags := flag.NewFlagSet("verify", flag.ContinueOnError)
	flags.SetOutput(stderr)
	dir := flags.String("dir", "", "ridstore directory to verify offline")
	if err := flags.Parse(args[1:]); err != nil || *dir == "" || flags.NArg() != 0 {
		if err == nil {
			fmt.Fprintln(stderr, "usage: ridstore-tool verify --dir <store-directory>")
		}
		return 2
	}
	report, err := verify.Run(ctx, *dir)
	encoder := json.NewEncoder(stdout)
	encoder.SetIndent("", "  ")
	if encodeErr := encoder.Encode(report); encodeErr != nil {
		fmt.Fprintf(stderr, "encode report: %v\n", encodeErr)
		return 1
	}
	if err != nil {
		fmt.Fprintf(stderr, "verify: %v\n", err)
		return 1
	}
	return 0
}

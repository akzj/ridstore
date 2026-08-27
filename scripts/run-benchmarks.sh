#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
report_dir=${BENCH_REPORT_DIR:-}
bench_time=${BENCH_TIME:-3s}
bench_count=${BENCH_COUNT:-3}

if [[ -z "$report_dir" ]]; then
	echo "BENCH_REPORT_DIR is required" >&2
	exit 2
fi
report_parent_input=$(dirname "$report_dir")
if [[ ! -d "$report_parent_input" ]]; then
	echo "report parent does not exist: $report_parent_input" >&2
	exit 2
fi
report_parent=$(cd "$report_parent_input" && pwd)
report_dir="$report_parent/$(basename "$report_dir")"
if [[ -e "$report_dir" ]]; then
	echo "report path already exists: $report_dir" >&2
	exit 2
fi
if [[ ! "$bench_count" =~ ^[1-9][0-9]*$ ]]; then
	echo "BENCH_COUNT must be a positive integer" >&2
	exit 2
fi

cd "$repo_root"
git_commit=$(git rev-parse HEAD)
git_dirty=false
if [[ -n "$(git status --porcelain)" ]]; then
	git_dirty=true
fi

mkdir "$report_dir"
finished=false
cleanup() {
	status=$?
	trap - EXIT INT TERM
	if [[ "$finished" != true && ! -e "$report_dir/FAILED" ]]; then
		printf 'finished_at=%s\nexit_status=%d\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$status" >"$report_dir/FAILED"
	fi
	exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

{
	printf 'started_at=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
	printf 'git_commit=%s\n' "$git_commit"
	printf 'git_dirty=%s\n' "$git_dirty"
	printf 'bench_time=%s\n' "$bench_time"
	printf 'bench_count=%s\n' "$bench_count"
	printf 'uname=%s\n' "$(uname -a)"
	go version
	go env GOOS GOARCH GOAMD64 CGO_ENABLED
	df -T "$repo_root"
} >"$report_dir/metadata.txt"

go test . -run '^$' -bench '^BenchmarkDurable' -benchmem -benchtime "$bench_time" -count "$bench_count" 2>&1 | tee "$report_dir/benchmark.txt"
printf 'finished_at=%s\ngit_commit=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$git_commit" >"$report_dir/COMPLETED"
finished=true

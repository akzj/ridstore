#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
report_dir=${FUZZ_REPORT_DIR:-}
fuzz_time=${FUZZ_TIME:-30m}
fuzz_parallel=${FUZZ_PARALLEL:-4}
targets=${FUZZ_TARGETS:-"internal/recordcodec:FuzzDecodePut internal/recordcodec:FuzzDecodeCommitGroup internal/recordlog:FuzzDecodeRecord internal/recordlog:FuzzDecodeSegmentHeader internal/recordlog:FuzzDecodeSegmentFooter internal/recordlog:FuzzDecodeRotationJournal internal/mapstore:FuzzDecodeNode internal/backuprestore:FuzzDecodeMetadata internal/storecatalog:FuzzDecodeManifest"}

if [[ -z "$report_dir" ]]; then
	echo "FUZZ_REPORT_DIR is required" >&2
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

cd "$repo_root"
git_commit=$(git rev-parse HEAD)
git_dirty=false
if [[ -n "$(git status --porcelain)" ]]; then
	git_dirty=true
fi

mkdir "$report_dir"
cache_dir=""
finished=false
cleanup() {
	status=$?
	trap - EXIT INT TERM
	if [[ "$finished" != true && ! -e "$report_dir/FAILED" ]]; then
		printf 'finished_at=%s\nexit_status=%d\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$status" >"$report_dir/FAILED"
	fi
	if [[ -n "$cache_dir" ]]; then
		rm -rf -- "$cache_dir"
	fi
	exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
cache_dir=$(mktemp -d "${TMPDIR:-/tmp}/ridstore-long-fuzz.XXXXXX")

{
	printf 'started_at=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
	printf 'git_commit=%s\n' "$git_commit"
	printf 'git_dirty=%s\n' "$git_dirty"
	printf 'fuzz_time=%s\n' "$fuzz_time"
	printf 'fuzz_parallel=%s\n' "$fuzz_parallel"
	printf 'targets=%s\n' "$targets"
	printf 'github_run_id=%s\n' "${GITHUB_RUN_ID:-}"
	printf 'github_run_attempt=%s\n' "${GITHUB_RUN_ATTEMPT:-}"
	printf 'uname=%s\n' "$(uname -a)"
	go version
	go env GOOS GOARCH
	df -T "$repo_root"
} >"$report_dir/metadata.txt"
printf 'package\ttarget\tstarted_at\tfinished_at\tduration_seconds\texit_status\n' >"$report_dir/summary.tsv"

for item in $targets; do
	package=${item%%:*}
	target=${item##*:}
	if [[ "$item" != *:* || ! "$package" =~ ^internal/[a-z0-9_/]+$ || ! "$target" =~ ^Fuzz[A-Za-z0-9_]+$ ]]; then
		echo "invalid fuzz target: $item" >&2
		exit 2
	fi
	log_name=${package//\//_}_${target}.log
	started_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)
	started_epoch=$(date +%s)
	set +e
	GOCACHE="$cache_dir" go test "./$package" -run '^$' -fuzz "^${target}$" -fuzztime "$fuzz_time" -parallel "$fuzz_parallel" 2>&1 | tee "$report_dir/$log_name"
	status=${PIPESTATUS[0]}
	set -e
	finished_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)
	finished_epoch=$(date +%s)
	printf '%s\t%s\t%s\t%s\t%d\t%d\n' "$package" "$target" "$started_at" "$finished_at" "$((finished_epoch - started_epoch))" "$status" >>"$report_dir/summary.tsv"
	if ((status != 0)); then
		exit "$status"
	fi
done

if [[ -d "$cache_dir/fuzz" ]]; then
	cp -a "$cache_dir/fuzz" "$report_dir/corpus"
fi
printf 'finished_at=%s\ngit_commit=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$git_commit" >"$report_dir/COMPLETED"
finished=true

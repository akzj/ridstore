.PHONY: fmt test test-race test-fuzz-smoke test-fuzz-harness-smoke test-fuzz-long test-crash test-soak-smoke soak-72h vet check verify tool

FUZZ_TIME ?= 2s
FUZZ_PARALLEL ?= 4
FUZZ_LONG_TIME ?= 30m
SOAK_DURATION ?= 72h

fmt:
	gofmt -w $$(find . -name '*.go' -type f)

test:
	go test ./... -count=1

test-race:
	go test -race ./... -count=1

test-fuzz-smoke:
	@set -eu; \
	for item in \
		internal/recordcodec:FuzzDecodePut \
		internal/recordcodec:FuzzDecodeCommitGroup \
		internal/recordlog:FuzzDecodeRecord \
		internal/recordlog:FuzzDecodeSegmentHeader \
		internal/recordlog:FuzzDecodeSegmentFooter \
		internal/recordlog:FuzzDecodeRotationJournal \
		internal/mapstore:FuzzDecodeNode \
		internal/backuprestore:FuzzDecodeMetadata \
		internal/storecatalog:FuzzDecodeManifest; do \
		package=$${item%%:*}; target=$${item##*:}; \
		go test ./$$package -run '^$$' -fuzz "^$${target}$$" -fuzztime "$(FUZZ_TIME)" -parallel "$(FUZZ_PARALLEL)"; \
	done

test-fuzz-harness-smoke:
	@set -eu; \
	tmp_root="$$(mktemp -d)"; \
	trap 'rm -rf -- "$$tmp_root"' EXIT; \
	FUZZ_REPORT_DIR="$$tmp_root/report" FUZZ_TIME=1s FUZZ_PARALLEL=1 FUZZ_TARGETS='internal/recordcodec:FuzzDecodePut' ./scripts/run-long-fuzz.sh; \
	test -f "$$tmp_root/report/COMPLETED"; \
	test ! -e "$$tmp_root/report/FAILED"; \
	test -f "$$tmp_root/report/internal_recordcodec_FuzzDecodePut.log"; \
	test "$$(wc -l < "$$tmp_root/report/summary.tsv")" -eq 2; \
	if FUZZ_REPORT_DIR="$$tmp_root/report" FUZZ_TIME=1s FUZZ_TARGETS='internal/recordcodec:FuzzDecodePut' ./scripts/run-long-fuzz.sh >/dev/null 2>&1; then exit 1; fi; \
	if FUZZ_REPORT_DIR="$$tmp_root/failure" FUZZ_TIME=invalid FUZZ_TARGETS='internal/recordcodec:FuzzDecodePut' ./scripts/run-long-fuzz.sh >/dev/null 2>&1; then exit 1; fi; \
	test -f "$$tmp_root/failure/FAILED"; \
	test ! -e "$$tmp_root/failure/COMPLETED"; \
	test "$$(wc -l < "$$tmp_root/failure/summary.tsv")" -eq 2; \
	if FUZZ_REPORT_DIR="$$tmp_root/invalid" FUZZ_TARGETS='invalid:FuzzMissing' ./scripts/run-long-fuzz.sh >/dev/null 2>&1; then exit 1; fi; \
	test -f "$$tmp_root/invalid/FAILED"; \
	test ! -e "$$tmp_root/invalid/COMPLETED"

test-fuzz-long:
	@test -n "$(FUZZ_REPORT_DIR)" || (echo "FUZZ_REPORT_DIR is required" >&2; exit 2)
	FUZZ_REPORT_DIR="$(FUZZ_REPORT_DIR)" FUZZ_TIME="$(FUZZ_LONG_TIME)" FUZZ_PARALLEL="$(FUZZ_PARALLEL)" ./scripts/run-long-fuzz.sh

test-crash:
	go test . ./internal/recordlog ./internal/mapstore ./internal/engine ./internal/backuprestore -run 'RecoveryAcrossProcessExit' -count=1 -timeout=10m

test-soak-smoke:
	go test ./internal/soak -run 'TestShortRunValidatesHarnessWithoutClaimingLongSoak|TestCanceledRunClosesStoreAndLeavesExactEvidence' -count=1 -timeout=2m

soak-72h:
	@test -n "$(SOAK_DIR)" || (echo "SOAK_DIR is required" >&2; exit 2)
	@test -n "$(SOAK_REPORT)" || (echo "SOAK_REPORT is required" >&2; exit 2)
	go run ./cmd/ridstore-soak --dir "$(SOAK_DIR)" --report "$(SOAK_REPORT)" --duration "$(SOAK_DURATION)" --git-commit "$$(git rev-parse HEAD)" --git-dirty="$$(test -z "$$(git status --porcelain)" && echo false || echo true)"

vet:
	go vet ./...

check: test vet

verify: test test-race vet test-fuzz-smoke test-fuzz-harness-smoke test-crash test-soak-smoke

tool:
	mkdir -p .build
	go build -o .build/ridstore-tool ./cmd/ridstore-tool
	go build -o .build/ridstore-soak ./cmd/ridstore-soak

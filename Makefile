.PHONY: fmt test test-race test-fuzz-smoke test-crash vet check verify

FUZZ_TIME ?= 2s
FUZZ_PARALLEL ?= 4

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

test-crash:
	go test . ./internal/recordlog ./internal/mapstore ./internal/engine ./internal/backuprestore -run 'RecoveryAcrossProcessExit' -count=1 -timeout=10m

vet:
	go vet ./...

check: test vet

verify: test test-race vet test-fuzz-smoke test-crash

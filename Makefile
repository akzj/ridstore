.PHONY: fmt test test-race test-fuzz-smoke test-crash test-integration test-soak-smoke soak-72h bench vet check verify tool

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
	for target in \
		FuzzDecodeManifest \
		FuzzDecodeMutationEntries \
		FuzzDecodeDescriptorSeal \
		FuzzDecodeJournals \
		FuzzDecodeMappingNode \
		FuzzDecodeSegmentStructures \
		FuzzDecodeFrame \
		FuzzDecodeSystemPayloads; do \
		go test ./internal/format -run '^$$' -fuzz "^$${target}$$" -fuzztime "$(FUZZ_TIME)" -parallel "$(FUZZ_PARALLEL)"; \
	done

test-crash:
	go test ./... -run 'ProcessCrashMatrix' -count=1 -timeout=10m

test-integration:
	go test ./test/integration -count=1 -timeout=10m

test-soak-smoke:
	go test ./internal/soak -run TestShortRunValidatesHarnessWithoutClaimingLongSoak -count=1

soak-72h:
	@test -n "$(SOAK_DIR)" || (echo "SOAK_DIR is required" >&2; exit 2)
	@test -n "$(SOAK_REPORT)" || (echo "SOAK_REPORT is required" >&2; exit 2)
	go run ./cmd/ridstore-soak --dir "$(SOAK_DIR)" --report "$(SOAK_REPORT)" --duration 72h --git-commit "$$(git rev-parse HEAD)"

bench:
	go test . -run '^$$' -bench . -benchmem

vet:
	go vet ./...

check: test vet

verify: test test-race vet test-fuzz-smoke test-crash test-integration test-soak-smoke

tool:
	mkdir -p .build
	go build -o .build/ridstore-tool ./cmd/ridstore-tool
	go build -o .build/ridstore-soak ./cmd/ridstore-soak

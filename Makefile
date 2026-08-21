.PHONY: fmt test test-race test-fuzz-smoke test-crash test-integration bench vet check verify tool

FUZZ_TIME ?= 1s

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
		go test ./internal/format -run '^$$' -fuzz "^$${target}$$" -fuzztime "$(FUZZ_TIME)"; \
	done

test-crash:
	go test ./... -run 'ProcessCrashMatrix' -count=1 -timeout=10m

test-integration:
	go test ./test/integration -count=1 -timeout=10m

bench:
	go test . -run '^$$' -bench . -benchmem

vet:
	go vet ./...

check: test vet

verify: test test-race vet test-fuzz-smoke test-crash test-integration

tool:
	mkdir -p .build
	go build -o .build/ridstore-tool ./cmd/ridstore-tool

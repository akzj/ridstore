.PHONY: fmt test test-race test-crash vet check tool

fmt:
	gofmt -w $$(find . -name '*.go' -type f)

test:
	go test ./...

test-race:
	go test -race ./...

test-crash:
	go test ./... -run 'ProcessCrashMatrix' -count=1 -timeout=10m

vet:
	go vet ./...

check: test vet

tool:
	mkdir -p .build
	go build -o .build/ridstore-tool ./cmd/ridstore-tool

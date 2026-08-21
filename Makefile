.PHONY: fmt test test-race vet check tool

fmt:
	gofmt -w $$(find . -name '*.go' -type f)

test:
	go test ./...

test-race:
	go test -race ./...

vet:
	go vet ./...

check: test vet

tool:
	mkdir -p .build
	go build -o .build/ridstore-tool ./cmd/ridstore-tool

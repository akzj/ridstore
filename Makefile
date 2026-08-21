.PHONY: fmt test test-race vet check

fmt:
	gofmt -w $$(find . -name '*.go' -type f)

test:
	go test ./...

test-race:
	go test -race ./...

vet:
	go vet ./...

check: test vet

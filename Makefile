.PHONY: build test test-race vet fmt check cluster-3 cluster-5

build:
	go build ./...

test:
	go test ./...

test-race:
	go test -race ./...

vet:
	go vet ./...

fmt:
	gofmt -w $$(find . -name '*.go' -not -path './vendor/*')

check: build test-race vet

cluster-3:
	./scripts/run-local-cluster.sh 3

cluster-5:
	./scripts/run-local-cluster.sh 5

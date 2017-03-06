.PHONY: build ci test vet

build:
	go build -o build/kage ./cmd/kage

clean:
	rm -rf build/*

ci: build test vet

test: build
	go test $(shell go list ./... | grep -v /vendor/)

vet:
	go vet $(shell go list ./... | grep -v /vendor/)
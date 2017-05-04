.PHONY: build ci test vet docker

build:
	go build -o ./dist/kage ./cmd/kage

clean:
	rm -rf build/*

ci: build test vet

test: build
	go test $(shell go list ./... | grep -v /vendor/)

vet:
	go vet $(shell go list ./... | grep -v /vendor/)

bump:
	pip install --upgrade bumpversion
	bumpversion patch

docker:
	CGO_ENABLED=0 GOOS=linux go build -a -ldflags '-s' -o ./dist/kage_docker ./cmd/kage
	docker build -t kage .

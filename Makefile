include github.com/msales/make/golang

# Run all benchmarks
bench:
	@go test -bench=. $(shell go list ./... | grep -v /vendor/)
.PHONY: bench

# Build the docker image
docker:
	docker build -t kage .
.PHONY: docker

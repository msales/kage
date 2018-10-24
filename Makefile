include github.com/msales/make/golang

# Run all benchmarks
bench:
	@go test -bench=. $(shell go list ./... | grep -v /vendor/)
.PHONY: bench

# Build the docker image
docker:
	docker build -t kage .
.PHONY: docker

# Read data from Kafka
producer:
	for x in {1..100}; do echo PING; sleep 0.1; done | \
	docker-compose exec -T kafka \
		kafka-console-producer.sh \
        --broker-list localhost:9092 \
		--topic=test
.PHONY: producer

# Produce data to Kafka
consumer:
	@docker-compose exec -T kafka \
		kafka-console-consumer.sh \
		--bootstrap-server localhost:9092 \
		--topic=test
.PHONY: consumer
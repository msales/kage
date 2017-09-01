package kage

import (
	"github.com/msales/kage/kafka"
	"github.com/msales/kage/store"
)

// Store represents an offset store.
type Store interface {
	// SetState adds a state into the store.
	SetState(interface{}) error

	// BrokerOffsets returns a snapshot of the current broker offsets.
	BrokerOffsets() store.BrokerOffsets

	// ConsumerOffsets returns a snapshot of the current consumer group offsets.
	ConsumerOffsets() store.ConsumerOffsets

	// BrokerMetadata returns a snapshot of the current broker metadata.
	BrokerMetadata() store.BrokerMetadata

	// Channel get the offset channel.
	Channel() chan interface{}

	// Close gracefully stops the Store.
	Close()
}

// Monitor represents a Monitor monitor.
type Monitor interface {
	// Brokers returns a list of Kafka brokers.
	Brokers() []kafka.Broker

	// Collect collects the state of Monitor.
	Collect()

	// IsHealthy checks the health of the Monitor.
	IsHealthy() bool

	// Close gracefully stops the Monitor client.
	Close()
}

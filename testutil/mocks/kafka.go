package mocks

import (
	"github.com/msales/kage"
	"github.com/stretchr/testify/mock"
)

// MockKafka represents a mock Kafka client.
type MockKafka struct {
	mock.Mock
}

// Brokers returns a list of Kafka brokers.
func (m MockKafka) Brokers() []kage.KafkaBroker {
	args := m.Called()
	return args.Get(0).([]kage.KafkaBroker)
}

// IsHealthy checks the health of the Kafka client.
func (m MockKafka) IsHealthy() bool {
	args := m.Called()
	return args.Bool(0)
}

// Close gracefully stops the Kafka client.
func (m MockKafka) Close() {
	m.Called()
}

// MockKafkaBroker represents a mock Kafka broker.
type MockKafkaBroker struct {
	mock.Mock
}

// ID returns the Broker id.
func (m MockKafkaBroker) ID() int32 {
	args := m.Called()
	return int32(args.Int(0))
}

// Connected returns the Broker connection status.
func (m MockKafkaBroker) Connected() bool {
	args := m.Called()
	return args.Bool(0)
}

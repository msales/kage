package mocks

import (
	"github.com/msales/kage/kafka"
	"github.com/stretchr/testify/mock"
)

// MockMonitor represents a mock Kafka client.
type MockMonitor struct {
	mock.Mock
}

// Brokers returns a list of Kafka brokers.
func (m *MockMonitor) Brokers() []kafka.Broker {
	args := m.Called()
	return args.Get(0).([]kafka.Broker)
}

// Collect collects the state of Monitor.
func (m *MockMonitor) Collect() {
	m.Called()
}

// IsHealthy checks the health of the Kafka client.
func (m *MockMonitor) IsHealthy() bool {
	args := m.Called()
	return args.Bool(0)
}

// Close gracefully stops the Kafka client.
func (m *MockMonitor) Close() {
	m.Called()
}

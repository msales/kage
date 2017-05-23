package mocks

import (
	"github.com/msales/kage"
	"github.com/stretchr/testify/mock"
)

// MockStore represents a mock offset store.
type MockStore struct {
	mock.Mock
}

// AddOffset adds an offset into the store.
func (m *MockStore) AddOffset(o *kage.PartitionOffset) {
	m.Called(o)
}

// BrokerOffsets returns a snapshot of the current broker offsets.
func (m *MockStore) BrokerOffsets() kage.BrokerOffsets {
	args := m.Called()
	return args.Get(0).(kage.BrokerOffsets)
}

// ConsumerOffsets returns a snapshot of the current consumer group offsets.
func (m *MockStore) ConsumerOffsets() kage.ConsumerOffsets {
	args := m.Called()
	return args.Get(0).(kage.ConsumerOffsets)
}

// Channel get the offset channel.
func (m *MockStore) Channel() chan *kage.PartitionOffset {
	args := m.Called()
	return args.Get(0).(chan *kage.PartitionOffset)
}

// Close gracefully stops the Store.
func (m *MockStore) Close() {
	m.Called()
}

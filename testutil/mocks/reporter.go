package mocks

import (
	"github.com/msales/kage"
	"github.com/stretchr/testify/mock"
)

// MockReporter represents a mock Reporter.
type MockReporter struct {
	mock.Mock
}

// ReportBrokerOffsets reports a snapshot of the broker offsets.
func (m *MockReporter) ReportBrokerOffsets(o *kage.BrokerOffsets) {
	m.Called(o)
}

// ReportConsumerOffsets reports a snapshot of the consumer group offsets.
func (m *MockReporter) ReportConsumerOffsets(o *kage.ConsumerOffsets) {
	m.Called(o)
}

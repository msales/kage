package mocks

import "github.com/msales/kage/kage"

// MockReporter represents a mock Reporter.
type MockReporter struct {
	BrokerOffsets   *kage.BrokerOffsets
	ConsumerOffsets *kage.ConsumerOffsets
}

// ReportBrokerOffsets reports a snapshot of the broker offsets.
func (r *MockReporter) ReportBrokerOffsets(o *kage.BrokerOffsets) {
	r.BrokerOffsets = o
}

// ReportConsumerOffsets reports a snapshot of the consumer group offsets.
func (r *MockReporter) ReportConsumerOffsets(o *kage.ConsumerOffsets) {
	r.ConsumerOffsets = o
}

// IsHealthy checks the health of the reporter.
func (r *MockReporter) IsHealthy() bool {
	return true
}

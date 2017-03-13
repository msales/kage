package mocks

import "github.com/msales/kage/kage"

type MockReporter struct {
	BrokerOffsets   *kage.BrokerOffsets
	ConsumerOffsets *kage.ConsumerOffsets
}

func (r *MockReporter) ReportBrokerOffsets(o *kage.BrokerOffsets) {
	r.BrokerOffsets = o
}

func (r *MockReporter) ReportConsumerOffsets(o *kage.ConsumerOffsets) {
	r.ConsumerOffsets = o
}

func (r *MockReporter) IsHealthy() bool {
	return true
}

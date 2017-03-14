package reporter

import "github.com/msales/kage/kage"

// Reporters represents a set of reporters.
type Reporters map[string]Reporter

// Add adds a Reporter to the set.
func (rs *Reporters) Add(key string, r Reporter) {
	(*rs)[key] = r
}

// ReportBrokerOffsets reports a snapshot of the broker offsets on all reporters.
func (rs *Reporters) ReportBrokerOffsets(o *kage.BrokerOffsets) {
	for _, r := range *rs {
		r.ReportBrokerOffsets(o)
	}
}

// ReportConsumerOffsets reports a snapshot of the consumer group offsets on all reporters.
func (rs *Reporters) ReportConsumerOffsets(o *kage.ConsumerOffsets) {
	for _, r := range *rs {
		r.ReportConsumerOffsets(o)
	}
}

// Reporter represents a offset reporter.
type Reporter interface {
	// ReportBrokerOffsets reports a snapshot of the broker offsets.
	ReportBrokerOffsets(o *kage.BrokerOffsets)

	// ReportConsumerOffsets reports a snapshot of the consumer group offsets.
	ReportConsumerOffsets(o *kage.ConsumerOffsets)

	// IsHealthy checks the health of the reporter.
	IsHealthy() bool
}

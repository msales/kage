package kage

// Reporters represents a set of reporters.
type Reporters map[string]Reporter

// Add adds a Reporter to the set.
func (rs *Reporters) Add(key string, r Reporter) {
	(*rs)[key] = r
}

// ReportBrokerOffsets reports a snapshot of the broker offsets on all reporters.
func (rs *Reporters) ReportBrokerOffsets(o *BrokerOffsets) {
	for _, r := range *rs {
		r.ReportBrokerOffsets(o)
	}
}

// ReportConsumerOffsets reports a snapshot of the consumer group offsets on all reporters.
func (rs *Reporters) ReportConsumerOffsets(o *ConsumerOffsets) {
	for _, r := range *rs {
		r.ReportConsumerOffsets(o)
	}
}

// IsHealthy checks the health of the reporters.
func (rs *Reporters) IsHealthy() bool {
	for _, r := range *rs {
		if !r.IsHealthy() {
			return false
		}
	}

	return true
}

// Reporter represents a offset reporter.
type Reporter interface {
	// ReportBrokerOffsets reports a snapshot of the broker offsets.
	ReportBrokerOffsets(o *BrokerOffsets)

	// ReportConsumerOffsets reports a snapshot of the consumer group offsets.
	ReportConsumerOffsets(o *ConsumerOffsets)

	// IsHealthy checks the health of the reporter.
	IsHealthy() bool
}

// Store represents an offset store.
type Store interface {
	// AddOffset adds an offset into the store.
	AddOffset(o *PartitionOffset)

	// BrokerOffsets returns a snapshot of the current broker offsets.
	BrokerOffsets() BrokerOffsets

	// ConsumerOffsets returns a snapshot of the current consumer group offsets.
	ConsumerOffsets() ConsumerOffsets

	// Channel get the offset channel.
	Channel() chan *PartitionOffset

	// Close gracefully stops the Store.
	Close()
}

// Kafka represents a Kafka client.
type Kafka interface {
	// IsHealthy checks the health of the Kafka client.
	IsHealthy() bool

	// Close gracefully stops the Kafka client.
	Close()
}

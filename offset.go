package kage

// PartitionOffset represents a partition/group offset.
type PartitionOffset struct {
	Topic               string
	Partition           int32
	Position            int64
	Offset              int64
	Timestamp           int64
	Group               string
	TopicPartitionCount int
}

// BrokerOffsets represents a set of topic offsets.
type BrokerOffsets map[string][]*BrokerOffset

// BrokerOffset represents a topic partition offset.
type BrokerOffset struct {
	OldestOffset int64
	NewestOffset int64
	Timestamp    int64
}

// ConsumerOffsets represents a set of consumer group offsets.
type ConsumerOffsets map[string]map[string][]*ConsumerOffset

// ConsumerOffset represents a consumer group topic partition offset.
type ConsumerOffset struct {
	Offset    int64
	Timestamp int64
	Lag       int64
}

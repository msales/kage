package store

// BrokerPartitionMetadata represents a brokers partition metadata.
type BrokerPartitionMetadata struct {
	Topic               string
	Partition           int32
	TopicPartitionCount int
	Leader              int32
	Replicas            []int32
	Isr                 []int32
	Timestamp           int64
}

// BrokerMetadata represents a set of topic metadata.
type BrokerMetadata map[string][]*Metadata

// Metadata represents a topic partition metadata.
type Metadata struct {
	Leader    int32
	Replicas  []int32
	Isr       []int32
	Timestamp int64
}

// BrokerPartitionOffset represents a brokers partition offset.
type BrokerPartitionOffset struct {
	Topic               string
	Partition           int32
	Oldest              bool
	Offset              int64
	Timestamp           int64
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

// ConsumerPartitionOffset represents a consumers partition offset.
type ConsumerPartitionOffset struct {
	Group     string
	Topic     string
	Partition int32
	Oldest    bool
	Offset    int64
	Timestamp int64
}

// ConsumerOffsets represents a set of consumer group offsets.
type ConsumerOffsets map[string]map[string][]*ConsumerOffset

// ConsumerOffset represents a consumer group topic partition offset.
type ConsumerOffset struct {
	Offset    int64
	Timestamp int64
	Lag       int64
}

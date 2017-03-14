package kage

type PartitionOffset struct {
	Topic               string
	Partition           int32
	Position            int64
	Offset              int64
	Timestamp           int64
	Group               string
	TopicPartitionCount int
}

type BrokerOffsets map[string][]*BrokerOffset;

type BrokerOffset struct {
	OldestOffset int64
	NewestOffset int64
	Timestamp    int64
}

type ConsumerOffsets map[string]map[string][]*ConsumerOffset

type ConsumerOffset struct {
	Offset    int64
	Timestamp int64
	Lag       int64
}

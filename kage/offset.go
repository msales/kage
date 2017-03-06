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

type ConsumerOffset struct {
	Offset    int64
	Timestamp int64
	Lag       int64
}

package store

import (
	"sync"

	"github.com/Shopify/sarama"
	"github.com/msales/kage/kage"
)

type MemoryStore struct {
	offsets  *ClusterOffsets
	shutdown chan struct{}

	OffsetCh chan *kage.PartitionOffset
}

type ClusterOffsets struct {
	broker     map[string][]*BrokerOffset
	brokerLock sync.RWMutex

	consumer     map[string]map[string][]*kage.ConsumerOffset
	consumerLock sync.RWMutex
}

type BrokerOffset struct {
	OldestOffset int64
	NewestOffset int64
	Timestamp    int64
}

func New() (*MemoryStore, error) {
	m := &MemoryStore{
		shutdown: make(chan struct{}),
		OffsetCh: make(chan *kage.PartitionOffset, 10000),
	}

	// Initialise the cluster offsets
	m.offsets = &ClusterOffsets{
		broker:   make(map[string][]*BrokerOffset),
		consumer: make(map[string]map[string][]*kage.ConsumerOffset),
	}

	// Start the offset reader
	go func() {
		for {
			select {
			case o := <-m.OffsetCh:
				if o.Group == "" {
					go m.addBrokerOffset(o)
				} else {
					go m.addConsumerOffset(o)
				}

			case <-m.shutdown:
				return
			}
		}
	}()

	return m, nil
}

func (m *MemoryStore) AddOffset(o *kage.PartitionOffset) {
	if o.Group == "" {
		m.addBrokerOffset(o)
	} else {
		m.addConsumerOffset(o)
	}
}

func (m *MemoryStore) BrokerOffsets() map[string][]BrokerOffset {
	m.offsets.brokerLock.RLock()
	defer m.offsets.brokerLock.RUnlock()

	snapshot := make(map[string][]BrokerOffset)
	for topic, partitions := range m.offsets.broker {
		snapshot[topic] = make([]BrokerOffset, len(partitions))

		for partition, offset := range partitions {
			snapshot[topic][partition] = BrokerOffset{
				OldestOffset: offset.OldestOffset,
				NewestOffset: offset.NewestOffset,
				Timestamp:    offset.Timestamp,
			}
		}
	}

	return snapshot
}

func (m *MemoryStore) ConsumerOffsets() map[string]map[string][]kage.ConsumerOffset {
	m.offsets.consumerLock.RLock()
	defer m.offsets.consumerLock.RUnlock()

	snapshot := make(map[string]map[string][]kage.ConsumerOffset)
	for group, topics := range m.offsets.consumer {
		snapshot[group] = make(map[string][]kage.ConsumerOffset)

		for topic, partitions := range topics {
			snapshot[group][topic] = make([]kage.ConsumerOffset, len(partitions))

			for partition, offset := range partitions {
				snapshot[group][topic][partition] = kage.ConsumerOffset{
					Offset:    offset.Offset,
					Lag:       offset.Lag,
					Timestamp: offset.Timestamp,
				}
			}
		}
	}

	return snapshot
}

func (m *MemoryStore) Shutdown() {
	close(m.shutdown)
}

func (m *MemoryStore) addBrokerOffset(o *kage.PartitionOffset) {
	m.offsets.brokerLock.Lock()
	defer m.offsets.brokerLock.Unlock()

	topic, ok := m.offsets.broker[o.Topic]
	if !ok {
		topic = make([]*BrokerOffset, o.TopicPartitionCount)
		m.offsets.broker[o.Topic] = topic
	}

	if o.TopicPartitionCount > len(topic) {
		for i := len(topic); i < o.TopicPartitionCount; i++ {
			topic = append(topic, nil)
		}
	}

	partition := topic[o.Partition]
	if partition == nil {
		partition = &BrokerOffset{}
		topic[o.Partition] = partition
	}

	partition.Timestamp = o.Timestamp
	switch o.Position {
	case sarama.OffsetOldest:
		partition.OldestOffset = o.Offset
		break

	case sarama.OffsetNewest:
		partition.NewestOffset = o.Offset
		break
	}
}

func (m *MemoryStore) addConsumerOffset(o *kage.PartitionOffset) {
	brokerOffset, partitionCount := m.getBrokerOffset(o)
	if brokerOffset == -1 {
		return
	}

	m.offsets.consumerLock.Lock()
	defer m.offsets.consumerLock.Unlock()

	group, ok := m.offsets.consumer[o.Group]
	if !ok {
		group = make(map[string][]*kage.ConsumerOffset)
		m.offsets.consumer[o.Group] = group
	}

	topic, ok := group[o.Topic]
	if !ok {
		topic = make([]*kage.ConsumerOffset, partitionCount)
		group[o.Topic] = topic
	}

	if partitionCount > len(topic) {
		for i := len(topic); i < partitionCount; i++ {
			topic = append(topic, nil)
		}
	}

	offset := topic[o.Partition]
	if offset == nil {
		offset = &kage.ConsumerOffset{}
		topic[o.Partition] = offset
	}

	lag := brokerOffset - o.Offset
	if lag < 0 {
		lag = 0
	}

	offset.Offset = o.Offset
	offset.Timestamp = o.Timestamp
	offset.Lag = lag
}

func (m *MemoryStore) getBrokerOffset(o *kage.PartitionOffset) (int64, int) {
	m.offsets.brokerLock.RLock()
	defer m.offsets.brokerLock.RUnlock()

	topic, ok := m.offsets.broker[o.Topic]
	if !ok {
		return -1, -1
	}

	if o.Partition < 0 || o.Partition > int32(len(topic)) {
		return -1, -1
	}

	if topic[o.Partition] == nil {
		return -1, -1
	}

	return topic[o.Partition].NewestOffset, len(topic)
}

package store

import (
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/msales/kage"
)

// ConsumerOffsets represents the broker and consumer offset manager
type ClusterOffsets struct {
	broker     kage.BrokerOffsets
	brokerLock sync.RWMutex

	consumer     kage.ConsumerOffsets
	consumerLock sync.RWMutex
}

// MemoryStore represents an in memory offset store.
type MemoryStore struct {
	offsets       *ClusterOffsets
	cleanupTicker *time.Ticker
	shutdown      chan struct{}

	offsetCh chan *kage.PartitionOffset
}

// New creates and returns a new MemoryStore.
func New() (*MemoryStore, error) {
	m := &MemoryStore{
		shutdown: make(chan struct{}),
		offsetCh: make(chan *kage.PartitionOffset, 10000),
	}

	// Initialise the cluster offsets
	m.offsets = &ClusterOffsets{
		broker:   make(map[string][]*kage.BrokerOffset),
		consumer: make(map[string]map[string][]*kage.ConsumerOffset),
	}

	// Start the offset reader
	go func() {
		for {
			select {
			case o := <-m.offsetCh:
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

	// Start cleanup task
	m.cleanupTicker = time.NewTicker(1 * time.Hour)
	go func() {
		for range m.cleanupTicker.C {
			m.CleanConsumerOffsets()
		}
	}()

	return m, nil
}

// AddOffset adds an offset into the store.
func (m *MemoryStore) AddOffset(o *kage.PartitionOffset) {
	if o.Group == "" {
		m.addBrokerOffset(o)
	} else {
		m.addConsumerOffset(o)
	}
}

// BrokerOffsets returns a snapshot of the current broker offsets.
func (m *MemoryStore) BrokerOffsets() kage.BrokerOffsets {
	m.offsets.brokerLock.RLock()
	defer m.offsets.brokerLock.RUnlock()

	snapshot := make(kage.BrokerOffsets)
	for topic, partitions := range m.offsets.broker {
		snapshot[topic] = make([]*kage.BrokerOffset, len(partitions))

		for partition, offset := range partitions {
			if offset == nil {
				continue
			}

			snapshot[topic][partition] = &kage.BrokerOffset{
				OldestOffset: offset.OldestOffset,
				NewestOffset: offset.NewestOffset,
				Timestamp:    offset.Timestamp,
			}
		}
	}

	return snapshot
}

// ConsumerOffsets returns a snapshot of the current consumer group offsets.
func (m *MemoryStore) ConsumerOffsets() kage.ConsumerOffsets {
	m.offsets.consumerLock.RLock()
	defer m.offsets.consumerLock.RUnlock()

	snapshot := make(kage.ConsumerOffsets)
	for group, topics := range m.offsets.consumer {
		snapshot[group] = make(map[string][]*kage.ConsumerOffset)

		for topic, partitions := range topics {
			snapshot[group][topic] = make([]*kage.ConsumerOffset, len(partitions))

			for partition, offset := range partitions {
				if offset == nil {
					continue
				}

				snapshot[group][topic][partition] = &kage.ConsumerOffset{
					Offset:    offset.Offset,
					Lag:       offset.Lag,
					Timestamp: offset.Timestamp,
				}
			}
		}
	}

	return snapshot
}

// CleanConsumerOffsets cleans old offsets from the MemoryStore.
func (m *MemoryStore) CleanConsumerOffsets() {
	m.offsets.consumerLock.Lock()
	defer m.offsets.consumerLock.Unlock()

	ts := time.Now().Unix() * 1000
	for group, topics := range m.offsets.consumer {
		for topic, partitions := range topics {
			maxDuration := int64(0)

			for _, offset := range partitions {
				duration := ts - offset.Timestamp
				if duration > maxDuration {
					maxDuration = duration
				}
			}

			if maxDuration > (24 * int64(time.Hour.Seconds()) * 1000) {
				delete(m.offsets.consumer[group], topic)
			}
		}

		if len(m.offsets.consumer[group]) == 0 {
			delete(m.offsets.consumer, group)
		}
	}
}

// Channel get the offset channel.
func (m *MemoryStore) Channel() chan *kage.PartitionOffset {
	return m.offsetCh
}

// Close gracefully stops the MemoryStore.
func (m *MemoryStore) Close() {
	m.cleanupTicker.Stop()
	close(m.shutdown)
}

func (m *MemoryStore) addBrokerOffset(o *kage.PartitionOffset) {
	m.offsets.brokerLock.Lock()
	defer m.offsets.brokerLock.Unlock()

	topic, ok := m.offsets.broker[o.Topic]
	if !ok {
		topic = make([]*kage.BrokerOffset, o.TopicPartitionCount)
		m.offsets.broker[o.Topic] = topic
	}

	if o.TopicPartitionCount > len(topic) {
		for i := len(topic); i < o.TopicPartitionCount; i++ {
			topic = append(topic, nil)
		}
	}

	partition := topic[o.Partition]
	if partition == nil {
		partition = &kage.BrokerOffset{}
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
	if lag < 0 || o.Offset == 0 {
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

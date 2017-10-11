package store

import (
	"errors"
	"sync"
	"time"
)

// State represents the state of the store.
type State struct {
	broker     BrokerOffsets
	brokerLock sync.RWMutex

	consumer     ConsumerOffsets
	consumerLock sync.RWMutex

	metadata     BrokerMetadata
	metadataLock sync.RWMutex
}

// MemoryStore represents an in memory data store.
type MemoryStore struct {
	state         *State
	cleanupTicker *time.Ticker
	shutdown      chan struct{}

	stateCh chan interface{}
}

// New creates and returns a new MemoryStore.
func New() (*MemoryStore, error) {
	m := &MemoryStore{
		shutdown: make(chan struct{}),
		stateCh:  make(chan interface{}, 10000),
	}

	// Initialise the cluster offsets
	m.state = &State{
		broker:   make(BrokerOffsets),
		consumer: make(ConsumerOffsets),
		metadata: make(BrokerMetadata),
	}

	// Start the offset reader
	go func() {
		for {
			select {
			case v := <-m.stateCh:
				go m.SetState(v)

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

// SetState adds a state into the store.
func (m *MemoryStore) SetState(v interface{}) error {
	switch v.(type) {
	case *BrokerPartitionOffset:
		m.addBrokerOffset(v.(*BrokerPartitionOffset))

	case *ConsumerPartitionOffset:
		m.addConsumerOffset(v.(*ConsumerPartitionOffset))

	case *BrokerPartitionMetadata:
		m.addMetadata(v.(*BrokerPartitionMetadata))

	default:
		return errors.New("store: unknown state object")
	}

	return nil
}

// BrokerOffsets returns a snapshot of the current broker offsets.
func (m *MemoryStore) BrokerOffsets() BrokerOffsets {
	m.state.brokerLock.RLock()
	defer m.state.brokerLock.RUnlock()

	snapshot := make(BrokerOffsets)
	for topic, partitions := range m.state.broker {
		snapshot[topic] = make([]*BrokerOffset, len(partitions))

		for partition, offset := range partitions {
			if offset == nil {
				continue
			}

			snapshot[topic][partition] = &BrokerOffset{
				OldestOffset: offset.OldestOffset,
				NewestOffset: offset.NewestOffset,
				Timestamp:    offset.Timestamp,
			}
		}
	}

	return snapshot
}

// ConsumerOffsets returns a snapshot of the current consumer group offsets.
func (m *MemoryStore) ConsumerOffsets() ConsumerOffsets {
	m.state.consumerLock.RLock()
	defer m.state.consumerLock.RUnlock()

	snapshot := make(ConsumerOffsets)
	for group, topics := range m.state.consumer {
		snapshot[group] = make(map[string][]*ConsumerOffset)

		for topic, partitions := range topics {
			snapshot[group][topic] = make([]*ConsumerOffset, len(partitions))

			for partition, offset := range partitions {
				if offset == nil {
					continue
				}

				snapshot[group][topic][partition] = &ConsumerOffset{
					Offset:    offset.Offset,
					Lag:       offset.Lag,
					Timestamp: offset.Timestamp,
				}
			}
		}
	}

	return snapshot
}

// BrokerMetadata returns a snapshot of the current broker metadata.
func (m *MemoryStore) BrokerMetadata() BrokerMetadata {
	m.state.metadataLock.RLock()
	defer m.state.metadataLock.RUnlock()

	snapshot := make(BrokerMetadata)
	for topic, partitions := range m.state.metadata {
		snapshot[topic] = make([]*Metadata, len(partitions))

		for partition, metadata := range partitions {
			if metadata == nil {
				continue
			}

			snapshot[topic][partition] = &Metadata{
				Leader:   metadata.Leader,
				Replicas: make([]int32, len(metadata.Replicas)),
				Isr:      make([]int32, len(metadata.Isr)),
			}
			copy(snapshot[topic][partition].Replicas, metadata.Replicas)
			copy(snapshot[topic][partition].Isr, metadata.Isr)
		}
	}

	return snapshot
}

// CleanConsumerOffsets cleans old offsets from the MemoryStore.
func (m *MemoryStore) CleanConsumerOffsets() {
	m.state.consumerLock.Lock()
	defer m.state.consumerLock.Unlock()

	ts := time.Now().Unix() * 1000
	for group, topics := range m.state.consumer {
		for topic, partitions := range topics {
			maxDuration := int64(0)

			for _, offset := range partitions {
				if offset == nil {
					continue
				}

				duration := ts - offset.Timestamp
				if duration > maxDuration {
					maxDuration = duration
				}
			}

			if maxDuration > (24 * int64(time.Hour.Seconds()) * 1000) {
				delete(m.state.consumer[group], topic)
			}
		}

		if len(m.state.consumer[group]) == 0 {
			delete(m.state.consumer, group)
		}
	}
}

// Channel get the offset channel.
func (m *MemoryStore) Channel() chan interface{} {
	return m.stateCh
}

// Close gracefully stops the MemoryStore.
func (m *MemoryStore) Close() {
	m.cleanupTicker.Stop()
	close(m.shutdown)
}

func (m *MemoryStore) addBrokerOffset(o *BrokerPartitionOffset) {
	m.state.brokerLock.Lock()
	defer m.state.brokerLock.Unlock()

	topic, ok := m.state.broker[o.Topic]
	if !ok {
		topic = make([]*BrokerOffset, o.TopicPartitionCount)
		m.state.broker[o.Topic] = topic
	}

	if o.TopicPartitionCount > len(topic) {
		for i := len(topic); i < o.TopicPartitionCount; i++ {
			topic = append(topic, nil)
		}
		m.state.broker[o.Topic] = topic
	}

	partition := topic[o.Partition]
	if partition == nil {
		partition = &BrokerOffset{}
		topic[o.Partition] = partition
	}

	partition.Timestamp = o.Timestamp
	if o.Oldest {
		partition.OldestOffset = o.Offset
	} else {
		partition.NewestOffset = o.Offset
	}
}

func (m *MemoryStore) addConsumerOffset(o *ConsumerPartitionOffset) {
	brokerOffset, partitionCount := m.getBrokerOffset(o.Topic, o.Partition)
	if brokerOffset == -1 {
		return
	}

	m.state.consumerLock.Lock()
	defer m.state.consumerLock.Unlock()

	group, ok := m.state.consumer[o.Group]
	if !ok {
		group = make(map[string][]*ConsumerOffset)
		m.state.consumer[o.Group] = group
	}

	topic, ok := group[o.Topic]
	if !ok {
		topic = make([]*ConsumerOffset, partitionCount)
		group[o.Topic] = topic
	}

	if partitionCount > len(topic) {
		for i := len(topic); i < partitionCount; i++ {
			topic = append(topic, nil)
		}
		group[o.Topic] = topic
	}

	offset := topic[o.Partition]
	if offset == nil {
		offset = &ConsumerOffset{}
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

func (m *MemoryStore) getBrokerOffset(topic string, partition int32) (int64, int) {
	m.state.brokerLock.RLock()
	defer m.state.brokerLock.RUnlock()

	brokerTopic, ok := m.state.broker[topic]
	if !ok {
		return -1, -1
	}

	if partition < 0 || partition > int32(len(brokerTopic)-1) {
		return -1, -1
	}

	if brokerTopic[partition] == nil {
		return -1, -1
	}

	return brokerTopic[partition].NewestOffset, len(brokerTopic)
}

func (m *MemoryStore) addMetadata(v *BrokerPartitionMetadata) {
	m.state.metadataLock.Lock()
	defer m.state.metadataLock.Unlock()

	topic, ok := m.state.metadata[v.Topic]
	if !ok {
		topic = make([]*Metadata, v.TopicPartitionCount)
		m.state.metadata[v.Topic] = topic
	}

	if v.TopicPartitionCount > len(topic) {
		for i := len(topic); i < v.TopicPartitionCount; i++ {
			topic = append(topic, nil)
		}
	}

	partition := topic[v.Partition]
	if partition == nil {
		partition = &Metadata{}
		topic[v.Partition] = partition
	}

	partition.Leader = v.Leader
	partition.Replicas = v.Replicas
	partition.Isr = v.Isr
	partition.Timestamp = v.Timestamp
}

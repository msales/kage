package kafka

import (
	"fmt"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/msales/kage/store"
	"github.com/ryanuber/go-glob"
	"gopkg.in/inconshreveable/log15.v2"
)

// Broker represents a Kafka Broker.
type Broker struct {
	ID        int32
	Connected bool
}

// Monitor represents a Kafka cluster connection.
type Monitor struct {
	brokers []string

	client        sarama.Client
	refreshTicker *time.Ticker
	stateCh       chan interface{}

	ignoreTopics []string
	ignoreGroups []string

	log log15.Logger
}

// New creates and returns a new Monitor for a Kafka cluster.
func New(opts ...MonitorFunc) (*Monitor, error) {
	monitor := &Monitor{}

	for _, o := range opts {
		o(monitor)
	}

	config := sarama.NewConfig()
	config.Version = sarama.V0_10_1_0

	kafka, err := sarama.NewClient(monitor.brokers, config)
	if err != nil {
		return nil, err
	}
	monitor.client = kafka

	monitor.refreshTicker = time.NewTicker(2 * time.Minute)
	go func() {
		for range monitor.refreshTicker.C {
			monitor.refreshMetadata()
		}
	}()

	// Collect initial information
	go monitor.Collect()

	return monitor, nil
}

// Brokers returns a list of Kafka brokers.
func (m *Monitor) Brokers() []Broker {
	brokers := []Broker{}
	for _, b := range m.client.Brokers() {
		connected, _ := b.Connected()
		brokers = append(brokers, Broker{
			ID:        b.ID(),
			Connected: connected,
		})
	}
	return brokers
}

// Collect collects the state of Kafka.
func (m *Monitor) Collect() {
	m.getBrokerOffsets()
	m.getBrokerMetadata()
	m.getConsumerOffsets()
}

// IsHealthy checks the health of the Kafka cluster.
func (m *Monitor) IsHealthy() bool {
	for _, b := range m.client.Brokers() {
		if ok, _ := b.Connected(); ok {
			return true
		}
	}

	return false
}

// Close gracefully stops the Monitor.
func (m *Monitor) Close() {
	// Stop the offset ticker
	m.refreshTicker.Stop()
}

// getTopics gets the topics for the Kafka cluster.
func (m *Monitor) getTopics() map[string]int {
	// If auto create topics is on, trying to fetch metadata for a missing
	// topic will recreate it. To get around this we refresh the metadata
	// before getting topics and partitions.
	m.client.RefreshMetadata()

	topics, _ := m.client.Topics()

	topicMap := make(map[string]int)
	for _, topic := range topics {
		partitions, _ := m.client.Partitions(topic)

		topicMap[topic] = len(partitions)
	}

	return topicMap
}

// refreshMetadata refreshes the broker metadata
func (m *Monitor) refreshMetadata(topics ...string) {
	if err := m.client.RefreshMetadata(topics...); err != nil {
		m.log.Error(fmt.Sprintf("could not refresh topic metadata: %v", err))
	}
}

// getBrokerOffsets gets all broker topic offsets and sends them to the store.
func (m *Monitor) getBrokerOffsets() {
	topicMap := m.getTopics()

	requests := make(map[int32]map[int64]*sarama.OffsetRequest)
	brokers := make(map[int32]*sarama.Broker)

	for topic, partitions := range topicMap {
		if containsString(m.ignoreTopics, topic) {
			continue
		}

		for i := 0; i < partitions; i++ {
			broker, err := m.client.Leader(topic, int32(i))
			if err != nil {
				m.log.Error(fmt.Sprintf("topic leader error on %s:%v: %v", topic, int32(i), err))
				return
			}

			if _, ok := requests[broker.ID()]; !ok {
				brokers[broker.ID()] = broker
				requests[broker.ID()] = make(map[int64]*sarama.OffsetRequest)
				requests[broker.ID()][sarama.OffsetOldest] = &sarama.OffsetRequest{}
				requests[broker.ID()][sarama.OffsetNewest] = &sarama.OffsetRequest{}
			}

			requests[broker.ID()][sarama.OffsetOldest].AddBlock(topic, int32(i), sarama.OffsetOldest, 1)
			requests[broker.ID()][sarama.OffsetNewest].AddBlock(topic, int32(i), sarama.OffsetNewest, 1)
		}
	}

	var wg sync.WaitGroup
	getBrokerOffsets := func(brokerID int32, position int64, request *sarama.OffsetRequest) {
		defer wg.Done()

		response, err := brokers[brokerID].GetAvailableOffsets(request)
		if err != nil {
			m.log.Error(fmt.Sprintf("cannot fetch offsets from broker %v: %v", brokerID, err))

			brokers[brokerID].Close()

			return
		}

		ts := time.Now().Unix() * 1000
		for topic, partitions := range response.Blocks {
			for partition, offsetResp := range partitions {
				if offsetResp.Err != sarama.ErrNoError {
					if offsetResp.Err == sarama.ErrUnknownTopicOrPartition ||
						offsetResp.Err == sarama.ErrNotLeaderForPartition {
						// If we get this, the metadata is likely off, force a refresh for this topic
						m.refreshMetadata(topic)
						m.log.Info(fmt.Sprintf("metadata for topic %s refreshed due to OffsetResponse error", topic))
						continue
					}

					m.log.Warn(fmt.Sprintf("error in OffsetResponse for %s:%v from broker %v: %s", topic, partition, brokerID, offsetResp.Err.Error()))
					continue
				}

				offset := &store.BrokerPartitionOffset{
					Topic:               topic,
					Partition:           partition,
					Oldest:              position == sarama.OffsetOldest,
					Offset:              offsetResp.Offsets[0],
					Timestamp:           ts,
					TopicPartitionCount: topicMap[topic],
				}

				m.stateCh <- offset
			}
		}
	}

	for brokerID, requests := range requests {
		for position, request := range requests {
			wg.Add(1)

			go getBrokerOffsets(brokerID, position, request)
		}
	}

	wg.Wait()
}

// getBrokerMetadata gets all broker topic metadata and sends them to the store.
func (m *Monitor) getBrokerMetadata() {
	var broker *sarama.Broker
	brokers := m.client.Brokers()
	for _, b := range brokers {
		if ok, _ := b.Connected(); ok {
			broker = b
			break
		}
	}

	if broker == nil {
		m.log.Error("monitor: no connected brokers found to collect metadata")
		return
	}

	response, err := broker.GetMetadata(&sarama.MetadataRequest{})
	if err != nil {
		m.log.Error(fmt.Sprintf("monitor: cannot get metadata: %v", err))
		return
	}

	ts := time.Now().Unix() * 1000
	for _, topic := range response.Topics {
		if containsString(m.ignoreTopics, topic.Name) {
			continue
		}
		if topic.Err != sarama.ErrNoError {
			m.log.Error(fmt.Sprintf("monitor: cannot get topic metadata %s: %v", topic.Name, topic.Err.Error()))
			continue
		}

		partitionCount := len(topic.Partitions)
		for _, partition := range topic.Partitions {
			if partition.Err != sarama.ErrNoError {
				m.log.Error(fmt.Sprintf("monitor: cannot get topic partition metadata %s %d: %v", topic.Name, partition.ID, partition.Err.Error()))
				continue
			}

			m.stateCh <- &store.BrokerPartitionMetadata{
				Topic:               topic.Name,
				Partition:           partition.ID,
				TopicPartitionCount: partitionCount,
				Leader:              partition.Leader,
				Replicas:            partition.Replicas,
				Isr:                 partition.Isr,
				Timestamp:           ts,
			}
		}
	}
}

// getConsumerOffsets gets all the consumer offsets and send them to the store.
func (m *Monitor) getConsumerOffsets() {
	topicMap := m.getTopics()
	requests := make(map[int32]map[string]*sarama.OffsetFetchRequest)
	coordinators := make(map[int32]*sarama.Broker)

	brokers := m.client.Brokers()
	for _, broker := range brokers {
		if ok, err := broker.Connected(); !ok {
			if err != nil {
				m.log.Error(fmt.Sprintf("monitor: failed to connect to broker broker %v: %v", broker.ID(), err))
				continue
			}

			if err := broker.Open(m.client.Config()); err != nil {
				m.log.Error(fmt.Sprintf("monitor: failed to connect to broker broker %v: %v", broker.ID(), err))
				continue
			}
		}

		groups, err := broker.ListGroups(&sarama.ListGroupsRequest{})
		if err != nil {
			m.log.Error(fmt.Sprintf("monitor: cannot fetch consumer groups on broker %v: %v", broker.ID(), err))
			continue
		}

		for group := range groups.Groups {
			if containsString(m.ignoreGroups, group) {
				continue
			}

			coordinator, err := m.client.Coordinator(group)
			if err != nil {
				m.log.Error(fmt.Sprintf("monitor: cannot fetch co-ordinator for group %s: %v", group, err))
				continue
			}

			if _, ok := requests[coordinator.ID()]; !ok {
				coordinators[coordinator.ID()] = coordinator
				requests[coordinator.ID()] = make(map[string]*sarama.OffsetFetchRequest)
			}

			if _, ok := requests[coordinator.ID()][group]; !ok {
				requests[coordinator.ID()][group] = &sarama.OffsetFetchRequest{ConsumerGroup: group, Version: 1}
			}

			for topic, partitions := range topicMap {
				for i := 0; i < partitions; i++ {
					requests[coordinator.ID()][group].AddPartition(topic, int32(i))
				}
			}
		}
	}

	var wg sync.WaitGroup
	getConsumerOffsets := func(brokerID int32, group string, request *sarama.OffsetFetchRequest) {
		defer wg.Done()

		coordinator := coordinators[brokerID]

		offsets, err := coordinator.FetchOffset(request)
		if err != nil {
			m.log.Error(fmt.Sprintf("monitor: cannot get group topic offsets %v: %v", brokerID, err))

			return
		}

		ts := time.Now().Unix() * 1000
		for topic, partitions := range offsets.Blocks {
			for partition, block := range partitions {
				if block.Err != sarama.ErrNoError {
					m.log.Error(fmt.Sprintf("monitor: cannot get group topic offsets %v: %v", brokerID, block.Err.Error()))
					continue
				}

				if block.Offset == -1 {
					// We don't have an offset for this topic partition, ignore.
					continue
				}

				offset := &store.ConsumerPartitionOffset{
					Group:     group,
					Topic:     topic,
					Partition: partition,
					Offset:    block.Offset,
					Timestamp: ts,
				}

				m.stateCh <- offset
			}
		}
	}

	for brokerID, groups := range requests {
		for group, request := range groups {
			wg.Add(1)

			go getConsumerOffsets(brokerID, group, request)
		}
	}

	wg.Wait()
}

// containsString determines if the string matches any of the provided patterns.
func containsString(patterns []string, subject string) bool {
	for _, pattern := range patterns {
		if glob.Glob(pattern, subject) {
			return true
		}
	}

	return false
}

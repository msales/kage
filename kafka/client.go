package kafka

import (
	"fmt"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/inconshreveable/log15"
	"github.com/msales/kage/kage"
	"github.com/ryanuber/go-glob"
)

type Client struct {
	brokers []string

	client             sarama.Client
	masterConsumer     sarama.Consumer
	partitionConsumers []sarama.PartitionConsumer
	messageCh          chan *sarama.ConsumerMessage
	errorCh            chan *sarama.ConsumerError
	fanInWG            sync.WaitGroup
	processorWG        sync.WaitGroup

	ignoreTopics []string
	ignoreGroups []string

	brokerOffsetTicker   *time.Ticker
	consumerOffsetTicker *time.Ticker
	offsetCh             chan *kage.PartitionOffset

	log log15.Logger
}

func New(opts ...ClientFunc) (*Client, error) {
	client := &Client{
		messageCh:   make(chan *sarama.ConsumerMessage),
		errorCh:     make(chan *sarama.ConsumerError),
		fanInWG:     sync.WaitGroup{},
		processorWG: sync.WaitGroup{},
	}

	for _, o := range opts {
		o(client)
	}

	config := sarama.NewConfig()
	config.Version = sarama.V0_10_1_0

	kafka, err := sarama.NewClient(client.brokers, config)
	if err != nil {
		return nil, err
	}
	client.client = kafka

	// Create sarama master consumer
	consumer, err := sarama.NewConsumerFromClient(kafka)
	if err != nil {
		kafka.Close()

		return nil, err
	}
	client.masterConsumer = consumer

	// Get the offsets for all topics
	client.getOffsets()
	client.brokerOffsetTicker = time.NewTicker(30 * time.Second)
	go func() {
		for range client.brokerOffsetTicker.C {
			client.getOffsets()
		}
	}()

	// Get the offsets for all topics
	client.getConsumerOffsets()
	client.consumerOffsetTicker = time.NewTicker(30 * time.Second)
	go func() {
		for range client.consumerOffsetTicker.C {
			client.getConsumerOffsets()
		}
	}()

	return client, nil
}

func (c *Client) Shutdown() {
	for _, consumer := range c.partitionConsumers {
		consumer.AsyncClose()
	}

	// Stop the partition consumer
	c.fanInWG.Wait()
	close(c.errorCh)
	close(c.messageCh)
	c.processorWG.Wait()

	// Stop the offset ticker
	c.brokerOffsetTicker.Stop()
	c.consumerOffsetTicker.Stop()
}

func (c *Client) getTopics() map[string]int {
	topics, _ := c.client.Topics()

	topicMap := make(map[string]int)
	for _, topic := range topics {
		partitions, _ := c.client.Partitions(topic)

		topicMap[topic] = len(partitions)
	}

	return topicMap
}

func (c *Client) getOffsets() error {
	topicMap := c.getTopics()

	requests := make(map[int32]map[int64]*sarama.OffsetRequest)
	brokers := make(map[int32]*sarama.Broker)

	for topic, partitions := range topicMap {
		if containsString(c.ignoreTopics, topic) {
			continue
		}

		for i := 0; i < partitions; i++ {
			broker, err := c.client.Leader(topic, int32(i))
			if err != nil {
				c.log.Error(fmt.Sprintf("Topic leader error on %s:%v: %v", topic, int32(i), err))

				return err
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
			c.log.Error(fmt.Sprintf("Cannot fetch offsets from broker %v: %v", brokerID, err))

			brokers[brokerID].Close()

			return
		}

		ts := time.Now().Unix() * 1000
		for topic, partitions := range response.Blocks {
			for partition, offsetResp := range partitions {
				if offsetResp.Err != sarama.ErrNoError {
					c.log.Warn(fmt.Sprintf("Error in OffsetResponse for %s:%v from broker %v: %s", topic, partition, brokerID, offsetResp.Err.Error()))

					continue
				}

				offset := &kage.PartitionOffset{
					Topic:               topic,
					Partition:           partition,
					Position:            position,
					Offset:              offsetResp.Offsets[0],
					Timestamp:           ts,
					TopicPartitionCount: topicMap[topic],
				}

				c.offsetCh <- offset
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

	return nil
}

func (c *Client) getConsumerOffsets() error {
	requests := make(map[int32]*sarama.DescribeGroupsRequest)
	coordinators := make(map[int32]*sarama.Broker)

	brokers := c.client.Brokers()
	for _, broker := range brokers {
		groups, err := broker.ListGroups(&sarama.ListGroupsRequest{})
		if err != nil {
			c.log.Error(fmt.Sprintf("Cannot fetch consumer groups on broker %v: %v", broker.ID(), err))
			continue
		}

		for group := range groups.Groups {
			if containsString(c.ignoreGroups, group) {
				continue
			}

			coordinator, err := c.client.Coordinator(group)
			if err != nil {
				c.log.Error(fmt.Sprintf("Cannot fetch co-ordinator for group %s: %v", group, err))
				continue
			}

			if _, ok := requests[coordinator.ID()]; !ok {
				coordinators[coordinator.ID()] = coordinator
				requests[coordinator.ID()] = &sarama.DescribeGroupsRequest{}
			}

			requests[coordinator.ID()].AddGroup(group)
		}
	}

	var wg sync.WaitGroup
	getConsumerOffsets := func(brokerID int32, request *sarama.DescribeGroupsRequest) {
		defer wg.Done()

		coordinator := coordinators[brokerID]

		response, err := coordinator.DescribeGroups(request)
		if err != nil {
			c.log.Error(fmt.Sprintf("Cannot describe consumer offsets from broker %v: %v", brokerID, err))

			return
		}

		for _, groupDesc := range response.Groups {
			offsetRequest := &sarama.OffsetFetchRequest{ConsumerGroup: groupDesc.GroupId, Version: 1}

			for _, groupMemDesc := range groupDesc.Members {
				meta, err := groupMemDesc.GetMemberAssignment()
				if err != nil {
					c.log.Error(fmt.Sprintf("Cannot get group member metadata %v: %v", brokerID, err))

					return
				}

				for topic, partitions := range meta.Topics {

					for _, partition := range partitions {
						offsetRequest.AddPartition(topic, partition)
					}
				}
			}

			offsets, err := coordinator.FetchOffset(offsetRequest)
			if err != nil {
				c.log.Error(fmt.Sprintf("Cannot get group topic offsets %v: %v", brokerID, err))

				return
			}

			ts := time.Now().Unix() * 1000
			for topic, partitions := range offsets.Blocks {
				for partition, block := range partitions {
					if block.Err != sarama.ErrNoError {
						c.log.Error(fmt.Sprintf("Cannot get group %v topic offsets %v: %v", groupDesc.GroupId, topic, block.Err))

						continue
					}

					offset := &kage.PartitionOffset{
						Topic:     topic,
						Partition: partition,
						Group:     groupDesc.GroupId,
						Offset:    block.Offset,
						Timestamp: ts,
					}

					c.offsetCh <- offset
				}
			}
		}
	}

	for brokerID, request := range requests {
		wg.Add(1)

		go getConsumerOffsets(brokerID, request)
	}

	wg.Wait()

	return nil
}

func containsString(patterns []string, subject string) bool {
	for _, pattern := range patterns {
		if glob.Glob(pattern, subject) {
			return true
		}
	}

	return false
}

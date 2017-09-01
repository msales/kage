package kafka

import (
	"testing"

	"github.com/Shopify/sarama"
	"github.com/msales/kage/testutil"
	"github.com/stretchr/testify/assert"
)

func TestMonitor_Brokers(t *testing.T) {
	seedBroker := sarama.NewMockBroker(t, 1)
	leader := sarama.NewMockBroker(t, 2)

	metadata := new(sarama.MetadataResponse)
	metadata.AddTopicPartition("foo", 0, leader.BrokerID(), nil, nil, sarama.ErrNoError)
	metadata.AddBroker(seedBroker.Addr(), seedBroker.BrokerID())
	metadata.AddBroker(leader.Addr(), leader.BrokerID())
	seedBroker.Returns(metadata)

	kafka, err := sarama.NewClient([]string{seedBroker.Addr()}, sarama.NewConfig())
	if err != nil {
		t.Fatal(err)
	}

	c := &Monitor{client: kafka}

	assert.Len(t, c.Brokers(), 2)

	seedBroker.Close()
	leader.Close()
}

func TestMonitor_IsHealthy(t *testing.T) {
	seedBroker := sarama.NewMockBroker(t, 1)
	leader := sarama.NewMockBroker(t, 2)

	metadata := new(sarama.MetadataResponse)
	metadata.AddTopicPartition("foo", 0, leader.BrokerID(), nil, nil, sarama.ErrNoError)
	metadata.AddBroker(seedBroker.Addr(), seedBroker.BrokerID())
	metadata.AddBroker(leader.Addr(), leader.BrokerID())
	seedBroker.Returns(metadata)

	kafka, err := sarama.NewClient([]string{seedBroker.Addr()}, sarama.NewConfig())
	assert.NoError(t, err)
	// Open all broker connections
	for _, b := range kafka.Brokers() {
		b.Open(kafka.Config())
	}

	c := &Monitor{client: kafka}

	assert.True(t, c.IsHealthy())

	// Close all broker connections
	for _, b := range kafka.Brokers() {
		b.Close()
	}

	assert.False(t, c.IsHealthy())

	seedBroker.Close()
	leader.Close()
}

func TestMonitor_getBrokerOffsets(t *testing.T) {
	seedBroker := sarama.NewMockBroker(t, 1)
	leader := sarama.NewMockBroker(t, 2)

	metadata := new(sarama.MetadataResponse)
	metadata.AddTopicPartition("foo", 0, leader.BrokerID(), nil, nil, sarama.ErrNoError)
	metadata.AddBroker(leader.Addr(), leader.BrokerID())
	seedBroker.Returns(metadata)

	oldestOffsetResponse := new(sarama.OffsetResponse)
	oldestOffsetResponse.AddTopicPartition("foo", 0, 0)
	leader.Returns(oldestOffsetResponse)

	newestOffsetResponse := new(sarama.OffsetResponse)
	newestOffsetResponse.AddTopicPartition("foo", 0, 123)
	leader.Returns(newestOffsetResponse)

	kafka, err := sarama.NewClient([]string{seedBroker.Addr()}, nil)
	assert.NoError(t, err)

	c := &Monitor{
		client:  kafka,
		stateCh: make(chan interface{}, 100),
		log:     testutil.Logger,
	}

	c.getBrokerOffsets()

	assert.Len(t, c.stateCh, 2)

	seedBroker.Close()
	leader.Close()
}

func TestMonitor_getBrokerMetadata(t *testing.T) {
	seedBroker := sarama.NewMockBroker(t, 1)
	leader := sarama.NewMockBroker(t, 2)

	metadata := new(sarama.MetadataResponse)
	metadata.AddTopicPartition("foo", 0, leader.BrokerID(), nil, nil, sarama.ErrNoError)
	metadata.AddBroker(leader.Addr(), leader.BrokerID())
	seedBroker.Returns(metadata)

	metadata = new(sarama.MetadataResponse)
	metadata.AddTopicPartition("foo", 0, leader.BrokerID(), []int32{leader.BrokerID()}, []int32{leader.BrokerID()}, sarama.ErrNoError)
	metadata.AddBroker(leader.Addr(), leader.BrokerID())
	leader.Returns(metadata)

	kafka, err := sarama.NewClient([]string{seedBroker.Addr()}, sarama.NewConfig())
	assert.NoError(t, err)
	// Open all broker connections
	for _, b := range kafka.Brokers() {
		b.Open(kafka.Config())
	}

	c := &Monitor{
		client:  kafka,
		stateCh: make(chan interface{}, 100),
		log:     testutil.Logger,
	}

	c.getBrokerMetadata()

	assert.Len(t, c.stateCh, 1)

	seedBroker.Close()
	leader.Close()
}

func TestMonitor_getConsumerOffsets(t *testing.T) {
	seedBroker := sarama.NewMockBroker(t, 1)
	leader := sarama.NewMockBroker(t, 2)

	metadata := new(sarama.MetadataResponse)
	metadata.AddTopicPartition("foo", 0, leader.BrokerID(), nil, nil, sarama.ErrNoError)
	metadata.AddBroker(leader.Addr(), leader.BrokerID())
	seedBroker.Returns(metadata)

	leader.Returns(&sarama.ListGroupsResponse{
		Err:    sarama.ErrNoError,
		Groups: map[string]string{"test": "test"},
	})

	seedBroker.Returns(&sarama.ConsumerMetadataResponse{
		Err:             sarama.ErrNoError,
		CoordinatorID:   leader.BrokerID(),
		CoordinatorHost: "127.0.0.1",
		CoordinatorPort: leader.Port(),
	})

	offset := new(sarama.OffsetFetchResponse)
	offset.AddBlock("test", 0, &sarama.OffsetFetchResponseBlock{
		Err:    sarama.ErrNoError,
		Offset: 123,
	})
	leader.Returns(offset)

	conf := sarama.NewConfig()
	conf.Version = sarama.V0_10_1_0
	kafka, err := sarama.NewClient([]string{seedBroker.Addr()}, conf)
	assert.NoError(t, err)

	c := &Monitor{
		client:  kafka,
		stateCh: make(chan interface{}, 100),
		log:     testutil.Logger,
	}

	c.getConsumerOffsets()

	assert.Len(t, c.stateCh, 1)

	seedBroker.Close()
	leader.Close()
}

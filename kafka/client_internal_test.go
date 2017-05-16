package kafka

import (
	"testing"

	"github.com/Shopify/sarama"
	"github.com/msales/kage"
	"github.com/msales/kage/testutil"
	"github.com/stretchr/testify/assert"
)

func TestClient_IsHealthy(t *testing.T) {
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

	c := &Client{client: kafka}

	assert.True(t, c.IsHealthy())

	seedBroker.Close()
	leader.Close()
}

func TestClient_getOffsets(t *testing.T) {
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

	c := &Client{
		client:   kafka,
		offsetCh: make(chan *kage.PartitionOffset, 100),
		log:      testutil.Logger,
	}

	c.getOffsets()

	assert.Len(t, c.offsetCh, 2)

	seedBroker.Close()
	leader.Close()
}

func TestClient_getConsumerOffsets(t *testing.T) {
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

	c := &Client{
		client:   kafka,
		offsetCh: make(chan *kage.PartitionOffset, 100),
		log:      testutil.Logger,
	}

	c.getConsumerOffsets()

	assert.Len(t, c.offsetCh, 1)

	seedBroker.Close()
	leader.Close()
}

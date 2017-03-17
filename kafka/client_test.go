package kafka

import (
	"testing"

	"github.com/Shopify/sarama"
	"github.com/msales/kage/kage"
	"gopkg.in/inconshreveable/log15.v2"
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

	if !c.IsHealthy() {
		t.Fatal("Expected health to pass")
	}

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
	if err != nil {
		t.Fatal(err)
	}

	c := &Client{
		client:   kafka,
		offsetCh: make(chan *kage.PartitionOffset, 100),
		log:      log15.New(),
	}

	c.getOffsets()

	if len(c.offsetCh) != 2 {
		t.Fatal("not enough offsets in offset channel")
	}

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
	if err != nil {
		t.Fatal(err)
	}

	c := &Client{
		client:   kafka,
		offsetCh: make(chan *kage.PartitionOffset, 100),
		log:      log15.New(),
	}

	c.getConsumerOffsets()

	if len(c.offsetCh) != 1 {
		t.Fatal("not enough offsets in offset channel")
	}

	seedBroker.Close()
	leader.Close()
}

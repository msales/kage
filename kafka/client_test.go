package kafka

import (
	"testing"

	"github.com/Shopify/sarama"
	"github.com/msales/kage/kage"
	"github.com/inconshreveable/log15"
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
		client: kafka,
		offsetCh: make(chan *kage.PartitionOffset, 100),
		log: log15.New(),
	}

	c.getOffsets()

	if len(c.offsetCh) != 2 {
		t.Fatal("not enough offsets in offset channel")
	}

	seedBroker.Close()
	leader.Close()
}

package store

import (
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/msales/kage/kage"
)

func TestMemoryStore_ConsumerOffsets(t *testing.T) {
	memStore, err := New()
	if err != nil {
		t.Error(err)
	}
	defer memStore.Shutdown()

	memStore.OffsetCh <- &kage.PartitionOffset{
		Topic:               "test",
		Partition:           0,
		Position:            sarama.OffsetNewest,
		Offset:              1000,
		Timestamp:           time.Now().Unix(),
		TopicPartitionCount: 1,
	}

	memStore.OffsetCh <- &kage.PartitionOffset{
		Topic:     "test",
		Partition: 0,
		Group:     "foo",
		Offset:    500,
		Timestamp: time.Now().Unix(),
	}

	// Wait for processing
	time.Sleep(1 * time.Second)

	offsets := memStore.ConsumerOffsets()

	if _, ok := offsets["foo"]; !ok {
		t.Error("expected group not found")
		return
	}

	if _, ok := offsets["foo"]["test"]; !ok {
		t.Error("expected topic not found")
		return
	}

	if len(offsets["foo"]["test"]) != 1 {
		t.Error("expected partition not found")
		return
	}

	offset := offsets["foo"]["test"][0]

	if offset.Lag != 500 {
		t.Errorf("excpected lag %d; got %d", 500, offset.Lag)
	}
}

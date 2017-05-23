package store_test

import (
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/msales/kage"
	"github.com/msales/kage/store"
	"github.com/stretchr/testify/assert"
)

func TestMemoryStore_BrokerOffsets(t *testing.T) {
	memStore, err := store.New()
	assert.NoError(t, err)

	defer memStore.Close()

	memStore.AddOffset(&kage.PartitionOffset{
		Topic:               "test",
		Partition:           0,
		Position:            sarama.OffsetOldest,
		Offset:              0,
		Timestamp:           time.Now().Unix(),
		TopicPartitionCount: 1,
	})
	memStore.AddOffset(&kage.PartitionOffset{
		Topic:               "test",
		Partition:           0,
		Position:            sarama.OffsetNewest,
		Offset:              1000,
		Timestamp:           time.Now().Unix(),
		TopicPartitionCount: 1,
	})

	offsets := memStore.BrokerOffsets()

	if _, ok := offsets["test"]; !ok {
		t.Error("expected topic not found")
		return
	}

	if len(offsets["test"]) != 1 {
		t.Error("expected partition not found")
		return
	}

	offset := offsets["test"][0]

	if offset.OldestOffset != 0 {
		t.Errorf("excpected oldest offset %d; got %d", 0, offset.OldestOffset)
		return
	}

	if offset.NewestOffset != 1000 {
		t.Errorf("excpected newest offset %d; got %d", 1000, offset.NewestOffset)
		return
	}
}

func TestMemoryStore_ConsumerOffsets(t *testing.T) {
	memStore, err := store.New()
	assert.NoError(t, err)

	defer memStore.Close()

	memStore.AddOffset(&kage.PartitionOffset{
		Topic:               "test",
		Partition:           0,
		Position:            sarama.OffsetNewest,
		Offset:              1000,
		Timestamp:           time.Now().Unix(),
		TopicPartitionCount: 1,
	})
	memStore.AddOffset(&kage.PartitionOffset{
		Topic:     "test",
		Partition: 0,
		Group:     "foo",
		Offset:    500,
		Timestamp: time.Now().Unix(),
	})

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

func TestMemoryStore_ConsumerOffsetsZeroOffset(t *testing.T) {
	memStore, err := store.New()
	assert.NoError(t, err)

	defer memStore.Close()

	memStore.AddOffset(&kage.PartitionOffset{
		Topic:               "test",
		Partition:           0,
		Position:            sarama.OffsetNewest,
		Offset:              1000,
		Timestamp:           time.Now().Unix(),
		TopicPartitionCount: 1,
	})
	memStore.AddOffset(&kage.PartitionOffset{
		Topic:     "test",
		Partition: 0,
		Group:     "foo",
		Offset:    0,
		Timestamp: time.Now().Unix(),
	})

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

	if offset.Lag != 0 {
		t.Errorf("excpected lag %d; got %d", 0, offset.Lag)
	}
}

func TestMemoryStore_CleanConsumerOffsets(t *testing.T) {
	memStore, err := store.New()
	assert.NoError(t, err)

	defer memStore.Close()

	memStore.AddOffset(&kage.PartitionOffset{
		Topic:               "test",
		Partition:           0,
		Position:            sarama.OffsetNewest,
		Offset:              1000,
		Timestamp:           time.Now().Unix(),
		TopicPartitionCount: 1,
	})
	memStore.AddOffset(&kage.PartitionOffset{
		Topic:     "test",
		Partition: 0,
		Group:     "foo",
		Offset:    500,
		Timestamp: time.Now().Unix()*1000 - (25 * int64(time.Hour.Seconds()) * 1000),
	})

	memStore.CleanConsumerOffsets()

	if len(memStore.ConsumerOffsets()) > 0 {
		t.Fatal("expected group to be cleaned; still exists")
	}
}

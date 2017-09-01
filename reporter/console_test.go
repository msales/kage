package reporter_test

import (
	"bytes"
	"testing"
	"time"

	"github.com/msales/kage/reporter"
	"github.com/msales/kage/store"
	"github.com/stretchr/testify/assert"
)

func TestConsoleReporter_ReportBrokerOffsets(t *testing.T) {
	buf := bytes.NewBuffer([]byte{})
	r := reporter.NewConsoleReporter(buf)

	offsets := &store.BrokerOffsets{
		"test": []*store.BrokerOffset{
			{
				OldestOffset: 0,
				NewestOffset: 1000,
				Timestamp:    time.Now().Unix() * 1000,
			},
		},
	}
	r.ReportBrokerOffsets(offsets)

	assert.Equal(t, "test:0 oldest:0 newest:1000 available:1000 \n", buf.String())
}

func TestConsoleReporter_ReportBrokerMetadata(t *testing.T) {
	buf := bytes.NewBuffer([]byte{})
	r := reporter.NewConsoleReporter(buf)

	metadata := &store.BrokerMetadata{
		"test": []*store.Metadata{
			{
				Leader:    1,
				Replicas:  []int32{1, 2},
				Isr:       []int32{1, 2},
				Timestamp: time.Now().Unix() * 1000,
			},
		},
	}
	r.ReportBrokerMetadata(metadata)

	assert.Equal(t, "test:0 leader:1 replicas:1,2 isr:1,2 \n", buf.String())
}

func TestConsoleReporter_ReportConsumerOffsets(t *testing.T) {
	buf := bytes.NewBuffer([]byte{})
	r := reporter.NewConsoleReporter(buf)

	offsets := &store.ConsumerOffsets{
		"foo": map[string][]*store.ConsumerOffset{
			"test": {
				{
					Offset:    1000,
					Lag:       100,
					Timestamp: time.Now().Unix() * 1000,
				},
			},
		},
	}
	r.ReportConsumerOffsets(offsets)

	assert.Equal(t, "foo test:0 offset:1000 lag:100 \n", buf.String())
}

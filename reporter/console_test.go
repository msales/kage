package reporter_test

import (
	"bytes"
	"testing"
	"time"

	"github.com/msales/kage"
	"github.com/msales/kage/reporter"
	"github.com/stretchr/testify/assert"
)

func TestConsoleReporter_ReportBrokerOffsets(t *testing.T) {
	buf := bytes.NewBuffer([]byte{})
	r := reporter.NewConsoleReporter(buf)

	offsets := &kage.BrokerOffsets{
		"test": []*kage.BrokerOffset{
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

func TestConsoleReporter_ReportConsumerOffsets(t *testing.T) {
	buf := bytes.NewBuffer([]byte{})
	r := reporter.NewConsoleReporter(buf)

	offsets := &kage.ConsumerOffsets{
		"foo": map[string][]*kage.ConsumerOffset{
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

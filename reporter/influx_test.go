package reporter_test

import (
	"testing"
	"time"

	"github.com/influxdata/influxdb/client/v2"
	"github.com/msales/kage/reporter"
	"github.com/msales/kage/store"
	"github.com/msales/kage/testutil"
	"github.com/msales/kage/testutil/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestInfluxReporter_ReportBrokerOffsets(t *testing.T) {
	c := new(mocks.MockInfluxClient)
	c.On("Write", mock.AnythingOfType("*client.batchpoints")).Return(nil).Run(func(args mock.Arguments) {
		bp := args.Get(0).(client.BatchPoints)
		assert.Len(t, bp.Points(), 1)
	})

	r := reporter.NewInfluxReporter(c,
		reporter.Tags(map[string]string{"test": "test"}),
		reporter.Log(testutil.Logger),
	)

	offsets := &store.BrokerOffsets{
		"test": []*store.BrokerOffset{
			{
				OldestOffset: 0,
				NewestOffset: 1000,
				Timestamp:    time.Now().Unix() * 1000,
			},
		},
		"nil": []*store.BrokerOffset{nil},
	}
	r.ReportBrokerOffsets(offsets)

}

func TestInfluxReporter_ReportBrokerMetadata(t *testing.T) {
	c := new(mocks.MockInfluxClient)
	c.On("Write", mock.AnythingOfType("*client.batchpoints")).Return(nil).Run(func(args mock.Arguments) {
		bp := args.Get(0).(client.BatchPoints)
		assert.Len(t, bp.Points(), 1)
	})

	r := reporter.NewInfluxReporter(c,
		reporter.Tags(map[string]string{"test": "test"}),
		reporter.Log(testutil.Logger),
	)

	metadata := &store.BrokerMetadata{
		"test": []*store.Metadata{
			{
				Leader:    1,
				Replicas:  []int32{1, 2},
				Isr:       []int32{1, 2},
				Timestamp: time.Now().Unix() * 1000,
			},
		},
		"nil": []*store.Metadata{nil},
	}
	r.ReportBrokerMetadata(metadata)

}

func TestInfluxReporter_ReportConsumerOffsets(t *testing.T) {
	c := new(mocks.MockInfluxClient)
	c.On("Write", mock.AnythingOfType("*client.batchpoints")).Return(nil).Run(func(args mock.Arguments) {
		bp := args.Get(0).(client.BatchPoints)
		assert.Len(t, bp.Points(), 1)
	})

	r := reporter.NewInfluxReporter(c,
		reporter.Tags(map[string]string{"test": "test"}),
		reporter.Log(testutil.Logger),
	)

	offsets := &store.ConsumerOffsets{
		"foo": map[string][]*store.ConsumerOffset{
			"test": {
				{
					Offset:    1000,
					Lag:       100,
					Timestamp: time.Now().Unix() * 1000,
				},
			},
			"nil": {nil},
		},
	}
	r.ReportConsumerOffsets(offsets)
}

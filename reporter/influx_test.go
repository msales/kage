package reporter_test

import (
	"errors"
	"testing"
	"time"

	"github.com/influxdata/influxdb/client/v2"
	"github.com/msales/kage"
	"github.com/msales/kage/reporter"
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
		reporter.Log(testutil.Logger),
	)

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

}

func TestInfluxReporter_ReportConsumerOffsets(t *testing.T) {
	c := new(mocks.MockInfluxClient)
	c.On("Write", mock.AnythingOfType("*client.batchpoints")).Return(nil).Run(func(args mock.Arguments) {
		bp := args.Get(0).(client.BatchPoints)
		assert.Len(t, bp.Points(), 1)
	})

	r := reporter.NewInfluxReporter(c,
		reporter.Log(testutil.Logger),
	)

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
}

func TestInfluxReporter_IsHealthy(t *testing.T) {
	c := new(mocks.MockInfluxClient)
	c.On("Ping", mock.Anything).Return(time.Millisecond, "", nil).Once()
	c.On("Ping", mock.Anything).Return(time.Millisecond, "", errors.New("test error")).Once()

	r := reporter.NewInfluxReporter(c,
		reporter.Log(testutil.Logger),
	)

	assert.True(t, r.IsHealthy())

	c.Close()

	assert.False(t, r.IsHealthy())
}

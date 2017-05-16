package reporter_test

import (
	"testing"
	"time"

	"github.com/msales/kage"
	"github.com/msales/kage/reporter"
	"github.com/msales/kage/testutil"
	"github.com/msales/kage/testutil/mocks"
	"github.com/stretchr/testify/assert"
)

func TestInfluxReporter_ReportBrokerOffsets(t *testing.T) {
	c := &mocks.MockInfluxClient{Connected: true}
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

	assert.Len(t, c.BatchPoints.Points(), 1)
}

func TestInfluxReporter_ReportConsumerOffsets(t *testing.T) {
	c := &mocks.MockInfluxClient{Connected: true}
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

	assert.Len(t, c.BatchPoints.Points(), 1)
}

func TestInfluxReporter_IsHealthy(t *testing.T) {
	c := &mocks.MockInfluxClient{Connected: true}
	r := reporter.NewInfluxReporter(c,
		reporter.Log(testutil.Logger),
	)

	assert.True(t, r.IsHealthy())

	c.Close()

	assert.False(t, r.IsHealthy())
}

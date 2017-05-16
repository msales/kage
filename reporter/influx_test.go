package reporter_test

import (
	"testing"
	"time"

	"github.com/msales/kage"
	"github.com/msales/kage/reporter"
	"github.com/msales/kage/testutil"
	"github.com/msales/kage/testutil/mocks"
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

	if len(c.BatchPoints.Points()) != 1 {
		t.Fatalf("expected %d point(s); got %d", 1, len(c.BatchPoints.Points()))
	}
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

	if len(c.BatchPoints.Points()) != 1 {
		t.Fatalf("expected %d point(s); got %d", 1, len(c.BatchPoints.Points()))
	}
}

func TestInfluxReporter_IsHealthy(t *testing.T) {
	c := &mocks.MockInfluxClient{Connected: true}
	r := reporter.NewInfluxReporter(c,
		reporter.Log(testutil.Logger),
	)

	if !r.IsHealthy() {
		t.Fatal("expected health to pass")
	}

	c.Close()

	if r.IsHealthy() {
		t.Fatal("expected health to fail")
	}
}

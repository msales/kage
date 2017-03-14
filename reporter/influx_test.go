package reporter

import (
	"testing"
	"time"

	"github.com/inconshreveable/log15"
	"github.com/msales/kage/kage"
	"github.com/msales/kage/testutil/mocks"
)

func TestInfluxReporter_ReportBrokerOffsets(t *testing.T) {
	log := log15.New()
	log.SetHandler(log15.DiscardHandler())

	c := &mocks.MockInfluxClient{Connected: true}
	r := &InfluxReporter{
		client: c,
		log:    log,
	}

	offsets := &kage.BrokerOffsets{
		"test": []*kage.BrokerOffset{
			&kage.BrokerOffset{
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
	log := log15.New()
	log.SetHandler(log15.DiscardHandler())

	c := &mocks.MockInfluxClient{Connected: true}
	r := &InfluxReporter{
		client: c,
		log:    log,
	}

	offsets := &kage.ConsumerOffsets{
		"foo": map[string][]*kage.ConsumerOffset{
			"test": []*kage.ConsumerOffset{
				&kage.ConsumerOffset{
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
	log := log15.New()
	log.SetHandler(log15.DiscardHandler())

	c := &mocks.MockInfluxClient{Connected: true}
	r := &InfluxReporter{
		client: c,
		log:    log,
	}

	if !r.IsHealthy() {
		t.Fatal("expected health to pass")
	}

	c.Close()

	if r.IsHealthy() {
		t.Fatal("expected health to fail")
	}
}

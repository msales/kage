package reporter

import (
	"net/url"
	"testing"
	"time"

	"github.com/msales/kage"
	"github.com/msales/kage/testutil/mocks"
	"gopkg.in/inconshreveable/log15.v2"
	"github.com/msales/kage/testutil"
)

func TestDSN(t *testing.T) {
	r := &InfluxReporter{}

	u, _ := url.Parse("https://username:password@addr:1234/db")
	DSN(u)(r)

	if r.addr != "https://addr:1234" {
		t.Fatalf("expected address %s; got %s", "addr", r.addr)
	}

	if r.username != "username" {
		t.Fatalf("expected username %s; got %s", "username", r.username)
	}

	if r.password != "password" {
		t.Fatalf("expected password %s; got %s", "password", r.password)
	}

	if r.database != "db" {
		t.Fatalf("expected database %s; got %s", "db", r.database)
	}
}

func TestDSN_NoUserInfo(t *testing.T) {
	r := &InfluxReporter{}

	u, _ := url.Parse("https://addr:1234/db")
	DSN(u)(r)

	if r.addr != "https://addr:1234" {
		t.Fatalf("expected address %s; got %s", "addr", r.addr)
	}

	if r.username != "" {
		t.Fatalf("expected username %s; got %s", "username", r.username)
	}

	if r.password != "" {
		t.Fatalf("expected password %s; got %s", "password", r.password)
	}

	if r.database != "db" {
		t.Fatalf("expected database %s; got %s", "db", r.database)
	}
}

func TestMetric(t *testing.T) {
	r := &InfluxReporter{}

	Metric("kafka")(r)

	if r.metric != "kafka" {
		t.Fatalf("expected metric %s; got %s", "kafka", r.metric)
	}
}

func TestPolicy(t *testing.T) {
	r := &InfluxReporter{}

	Policy("foobar")(r)

	if r.policy != "foobar" {
		t.Fatalf("expected metric %s; got %s", "foobar", r.policy)
	}
}

func TestTags(t *testing.T) {
	r := &InfluxReporter{}

	Tags(map[string]string{"foo": "bar"})(r)

	if r.tags["foo"] != "bar" {
		t.Fatal("expected tags not found")
	}
}

func TestLog(t *testing.T) {
	log := log15.New()
	r := &InfluxReporter{}

	Log(log)(r)

	if r.log != log {
		t.Fatal("expected log not found")
	}
}

func TestInfluxReporter_ReportBrokerOffsets(t *testing.T) {
	c := &mocks.MockInfluxClient{Connected: true}
	r := &InfluxReporter{
		client: c,
		log:    testutil.Logger,
	}

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
	log := log15.New()
	log.SetHandler(log15.DiscardHandler())

	c := &mocks.MockInfluxClient{Connected: true}
	r := &InfluxReporter{
		client: c,
		log:    log,
	}

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

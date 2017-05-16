package reporter

import (
	"testing"

	"gopkg.in/inconshreveable/log15.v2"
)

func TestDatabase(t *testing.T) {
	r := &InfluxReporter{}

	Database("test")(r)

	if r.database != "test" {
		t.Fatalf("expected database %s; got %s", "test", r.database)
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

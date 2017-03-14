package reporter

import (
	"testing"

	"github.com/msales/kage/kage"
	"github.com/msales/kage/testutil/mocks"
)

func TestReporters_Add(t *testing.T) {
	rs := Reporters{}

	rs.Add("test1", &mocks.MockReporter{})
	rs.Add("test2", &mocks.MockReporter{})

	want := 2
	got := len(rs)
	if got != want {
		t.Fatalf("expected reporter count %d; got %d", want, got)
	}
}

func TestReporters_ReportBrokerOffsets(t *testing.T) {
	rs := Reporters{}
	rs.Add("test1", &mocks.MockReporter{})
	rs.Add("test2", &mocks.MockReporter{})

	offsets := &kage.BrokerOffsets{}
	rs.ReportBrokerOffsets(offsets)

	for _, reporter := range rs {
		got := (reporter.(*mocks.MockReporter)).BrokerOffsets
		if got != offsets {
			t.Fatalf("expected offsets %v; got %v", offsets, got)
		}
	}
}

func TestReporters_ReportConsumerOffsets(t *testing.T) {
	rs := Reporters{}
	rs.Add("test1", &mocks.MockReporter{})
	rs.Add("test2", &mocks.MockReporter{})

	offsets := &kage.ConsumerOffsets{}
	rs.ReportConsumerOffsets(offsets)

	for _, reporter := range rs {
		got := (reporter.(*mocks.MockReporter)).ConsumerOffsets
		if got != offsets {
			t.Fatalf("expected offsets %v; got %v", offsets, got)
		}
	}
}

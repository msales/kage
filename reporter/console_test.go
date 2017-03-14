package reporter

import (
	"bytes"
	"testing"
	"time"

	"github.com/msales/kage/kage"
)

func TestConsoleReporter_ReportBrokerOffsets(t *testing.T) {
	buf := bytes.NewBuffer([]byte{})
	r := &ConsoleReporter{
		w: buf,
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

	want := "test:0 oldest:0 newest:1000 available:1000 \n"
	got := buf.String()
	if want != got {
		t.Fatalf("expected %s; got %s", want, got)
	}
}

func TestConsoleReporter_ReportConsumerOffsets(t *testing.T) {
	buf := bytes.NewBuffer([]byte{})
	r := &ConsoleReporter{
		w: buf,
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

	want := "foo test:0 offset:1000 lag:100 \n"
	got := buf.String()
	if want != got {
		t.Fatalf("expected %s; got %s", want, got)
	}
}

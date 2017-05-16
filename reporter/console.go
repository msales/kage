package reporter

import (
	"fmt"
	"io"

	"github.com/msales/kage"
)

// ConsoleReporter represents a console reporter.
type ConsoleReporter struct {
	w io.Writer
}

// NewConsoleReporter creates and returns a new ConsoleReporter.
func NewConsoleReporter(w io.Writer) *ConsoleReporter {
	return &ConsoleReporter{
		w: w,
	}
}

// ReportBrokerOffsets reports a snapshot of the broker offsets.
func (r ConsoleReporter) ReportBrokerOffsets(o *kage.BrokerOffsets) {
	for topic, partitions := range *o {
		for partition, offset := range partitions {
			if offset == nil {
				continue
			}

			io.WriteString(
				r.w,
				fmt.Sprintf(
					"%s:%d oldest:%d newest:%d available:%d \n",
					topic,
					partition,
					offset.OldestOffset,
					offset.NewestOffset,
					offset.NewestOffset-offset.OldestOffset,
				),
			)
		}
	}
}

// ReportConsumerOffsets reports a snapshot of the consumer group offsets.
func (r ConsoleReporter) ReportConsumerOffsets(o *kage.ConsumerOffsets) {
	for group, topics := range *o {
		for topic, partitions := range topics {
			for partition, offset := range partitions {
				if offset == nil {
					continue
				}

				io.WriteString(
					r.w,
					fmt.Sprintf(
						"%s %s:%d offset:%d lag:%d \n",
						group,
						topic,
						partition,
						offset.Offset,
						offset.Lag,
					),
				)
			}
		}
	}
}

// IsHealthy checks the health of the console reporter.
func (r ConsoleReporter) IsHealthy() bool {
	return true
}

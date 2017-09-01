package reporter

import (
	"fmt"
	"io"
	"strings"

	"github.com/msales/kage/store"
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
func (r ConsoleReporter) ReportBrokerOffsets(o *store.BrokerOffsets) {
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

// ReportBrokerMetadata reports a snapshot of the broker metadata.
func (r ConsoleReporter) ReportBrokerMetadata(m *store.BrokerMetadata) {
	for topic, partitions := range *m {
		for partition, metadata := range partitions {
			if metadata == nil {
				continue
			}

			io.WriteString(
				r.w,
				fmt.Sprintf(
					"%s:%d leader:%d replicas:%s isr:%s \n",
					topic,
					partition,
					metadata.Leader,
					strings.Replace(strings.Trim(fmt.Sprint(metadata.Replicas), "[]"), " ", ",", -1),
					strings.Replace(strings.Trim(fmt.Sprint(metadata.Isr), "[]"), " ", ",", -1),
				),
			)
		}
	}
}

// ReportConsumerOffsets reports a snapshot of the consumer group offsets.
func (r ConsoleReporter) ReportConsumerOffsets(o *store.ConsumerOffsets) {
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

package reporter

import (
	"fmt"
	"io"
	"os"

	"github.com/msales/kage/kage"
)

type ConsoleReporter struct {
	w io.Writer
}

func NewConsoleReporter() (*ConsoleReporter, error) {
	return &ConsoleReporter{
		w: os.Stdout,
	}, nil
}

func (r ConsoleReporter) ReportBrokerOffsets(o *kage.BrokerOffsets) {
	for topic, partitions := range *o {
		for partition, offset := range partitions {
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

func (r ConsoleReporter) ReportConsumerOffsets(o *kage.ConsumerOffsets) {
	for group, topics := range *o {
		for topic, partitions := range topics {
			for partition, offset := range partitions {
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

func (r ConsoleReporter) IsHealthy() bool {
	return true
}

package reporter

import (
	"fmt"

	"github.com/msales/kage/kage"
)

type ConsoleReporter struct {}

func NewConsoleReporter() (*ConsoleReporter, error) {
	return &ConsoleReporter{}, nil
}

func (r ConsoleReporter) ReportBrokerOffsets(o *kage.BrokerOffsets) {
	for topic, partitions := range *o {
		for partition, offset := range partitions {
			fmt.Printf(
				"%s:%d oldest:%d newest:%d available:%d \n",
				topic,
				partition,
				offset.OldestOffset,
				offset.NewestOffset,
				offset.NewestOffset - offset.OldestOffset,
			)
		}
	}
}

func (r ConsoleReporter) ReportConsumerOffsets(o *kage.ConsumerOffsets) {
	for group, topics := range *o {
		for topic, partitions := range topics {
			for partition, offset := range partitions {
				fmt.Printf(
					"%s %s:%d offset:%d lag:%d \n",
					group,
					topic,
					partition,
					offset.Offset,
					offset.Lag,
				)
			}
		}
	}
}

func (r ConsoleReporter) IsHealthy() bool {
	return true
}
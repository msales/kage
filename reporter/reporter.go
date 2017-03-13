package reporter

import "github.com/msales/kage/kage"

type Reporters map[string]Reporter

func (rs *Reporters) Add(key string, r Reporter) {
	(*rs)[key] = r
}

func (rs *Reporters) ReportBrokerOffsets(o *kage.BrokerOffsets) {
	for _, r := range *rs {
		r.ReportBrokerOffsets(o)
	}
}

func (rs *Reporters) ReportConsumerOffsets(o *kage.ConsumerOffsets) {
	for _, r := range *rs {
		r.ReportConsumerOffsets(o)
	}
}

type Reporter interface {
	ReportBrokerOffsets(o *kage.BrokerOffsets)

	ReportConsumerOffsets(o *kage.ConsumerOffsets)

	IsHealthy() bool
}

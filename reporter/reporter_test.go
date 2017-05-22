package reporter_test

import (
	"testing"

	"github.com/msales/kage"
	"github.com/msales/kage/reporter"
	"github.com/msales/kage/testutil/mocks"
	"github.com/stretchr/testify/assert"
)

func TestReporters_Add(t *testing.T) {
	rs := reporter.Reporters{}

	rs.Add("test1", &mocks.MockReporter{})
	rs.Add("test2", &mocks.MockReporter{})

	assert.Len(t, rs, 2)
}

func TestReporters_ReportBrokerOffsets(t *testing.T) {
	rs := reporter.Reporters{}
	rs.Add("test1", &mocks.MockReporter{})
	rs.Add("test2", &mocks.MockReporter{})

	offsets := &kage.BrokerOffsets{}
	rs.ReportBrokerOffsets(offsets)

	for _, reporter := range rs {
		assert.Equal(t, offsets, (reporter.(*mocks.MockReporter)).BrokerOffsets)
	}
}

func TestReporters_ReportConsumerOffsets(t *testing.T) {
	rs := reporter.Reporters{}
	rs.Add("test1", &mocks.MockReporter{})
	rs.Add("test2", &mocks.MockReporter{})

	offsets := &kage.ConsumerOffsets{}
	rs.ReportConsumerOffsets(offsets)

	for _, reporter := range rs {
		assert.Equal(t, offsets, (reporter.(*mocks.MockReporter)).ConsumerOffsets)
	}
}

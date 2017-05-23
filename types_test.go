package kage_test

import (
	"testing"

	"github.com/msales/kage"
	"github.com/msales/kage/testutil/mocks"
	"github.com/stretchr/testify/assert"
)

func TestReporters_Add(t *testing.T) {
	rs := kage.Reporters{}

	rs.Add("test1", &mocks.MockReporter{})
	rs.Add("test2", &mocks.MockReporter{})

	assert.Len(t, rs, 2)
}

func TestReporters_ReportBrokerOffsets(t *testing.T) {
	rs := kage.Reporters{}
	rs.Add("test1", &mocks.MockReporter{})
	rs.Add("test2", &mocks.MockReporter{})

	offsets := &kage.BrokerOffsets{}
	rs.ReportBrokerOffsets(offsets)

	for _, r := range rs {
		assert.Equal(t, offsets, (r.(*mocks.MockReporter)).BrokerOffsets)
	}
}

func TestReporters_ReportConsumerOffsets(t *testing.T) {
	rs := kage.Reporters{}
	rs.Add("test1", &mocks.MockReporter{})
	rs.Add("test2", &mocks.MockReporter{})

	offsets := &kage.ConsumerOffsets{}
	rs.ReportConsumerOffsets(offsets)

	for _, r := range rs {
		assert.Equal(t, offsets, (r.(*mocks.MockReporter)).ConsumerOffsets)
	}
}

func TestReporters_IsHealthy(t *testing.T) {
	rs := kage.Reporters{}
	rs.Add("test1", &mocks.MockReporter{})
	rs.Add("test2", &mocks.MockReporter{})

	assert.True(t, rs.IsHealthy())
}

package kage_test

import (
	"testing"

	"github.com/msales/kage"
	"github.com/msales/kage/store"
	"github.com/msales/kage/testutil/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestReporters_Add(t *testing.T) {
	rs := kage.Reporters{}

	rs.Add("test1", new(mocks.MockReporter))
	rs.Add("test2", new(mocks.MockReporter))

	assert.Len(t, rs, 2)
}

func TestReporters_ReportBrokerOffsets(t *testing.T) {
	rs := kage.Reporters{}
	offsets := &store.BrokerOffsets{}

	m1 := new(mocks.MockReporter)
	m1.On("ReportBrokerOffsets", mock.AnythingOfType("*store.BrokerOffsets")).Run(func(args mock.Arguments) {
		assert.Equal(t, offsets, args.Get(0))
	})
	rs.Add("test1", m1)

	m2 := new(mocks.MockReporter)
	m2.On("ReportBrokerOffsets", mock.AnythingOfType("*store.BrokerOffsets")).Run(func(args mock.Arguments) {
		assert.Equal(t, offsets, args.Get(0))
	})
	rs.Add("test2", m2)

	rs.ReportBrokerOffsets(offsets)

	m1.AssertExpectations(t)
}

func TestReporters_ReportConsumerOffsets(t *testing.T) {
	rs := kage.Reporters{}
	offsets := &store.ConsumerOffsets{}

	m1 := new(mocks.MockReporter)
	m1.On("ReportConsumerOffsets", mock.AnythingOfType("*store.ConsumerOffsets")).Run(func(args mock.Arguments) {
		assert.Equal(t, offsets, args.Get(0))
	})
	rs.Add("test1", m1)

	m2 := new(mocks.MockReporter)
	m2.On("ReportConsumerOffsets", mock.AnythingOfType("*store.ConsumerOffsets")).Run(func(args mock.Arguments) {
		assert.Equal(t, offsets, args.Get(0))
	})
	rs.Add("test2", m2)

	rs.ReportConsumerOffsets(offsets)

	m1.AssertExpectations(t)
}

func TestReporters_ReportBrokerMetadata(t *testing.T) {
	rs := kage.Reporters{}
	metadata := &store.BrokerMetadata{}

	m1 := new(mocks.MockReporter)
	m1.On("ReportBrokerMetadata", mock.AnythingOfType("*store.BrokerMetadata")).Run(func(args mock.Arguments) {
		assert.Equal(t, metadata, args.Get(0))
	})
	rs.Add("test1", m1)

	m2 := new(mocks.MockReporter)
	m2.On("ReportBrokerMetadata", mock.AnythingOfType("*store.BrokerMetadata")).Run(func(args mock.Arguments) {
		assert.Equal(t, metadata, args.Get(0))
	})
	rs.Add("test2", m2)

	rs.ReportBrokerMetadata(metadata)

	m1.AssertExpectations(t)
}

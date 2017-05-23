package kage_test

import (
	"testing"

	"github.com/msales/kage"
	"github.com/msales/kage/testutil/mocks"
	"github.com/stretchr/testify/assert"
)

func TestNewApplication(t *testing.T) {
	app := kage.NewApplication()

	assert.IsType(t, &kage.Application{}, app)
}

func TestApplication_Close(t *testing.T) {
	store := new(mocks.MockStore)
	store.On("Close").Return()

	kafka := new(mocks.MockKafka)
	kafka.On("Close").Return()

	app := &kage.Application{
		Store: store,
		Kafka: kafka,
	}

	app.Close()

	store.AssertExpectations(t)
	kafka.AssertExpectations(t)
}

func TestApplication_IsHealthy(t *testing.T) {
	reporters := &kage.Reporters{}

	reporter := new(mocks.MockReporter)
	reporter.On("IsHealthy").Return(true).Once()
	reporter.On("IsHealthy").Return(false).Once()
	reporters.Add("test", reporter)

	kafka := new(mocks.MockKafka)
	kafka.On("IsHealthy").Return(true).Twice()
	kafka.On("IsHealthy").Return(false).Once()

	app := &kage.Application{
		Reporters: reporters,
		Kafka:     kafka,
	}

	assert.True(t, app.IsHealthy())
	assert.False(t, app.IsHealthy())
	assert.False(t, app.IsHealthy())

	reporter.AssertExpectations(t)
	kafka.AssertExpectations(t)
}

func TestApplication_IsHealthyNoServices(t *testing.T) {
	app := &kage.Application{}

	assert.False(t, app.IsHealthy())
}

func TestApplication_Report(t *testing.T) {
	bo := kage.BrokerOffsets{}
	co := kage.ConsumerOffsets{}

	store := new(mocks.MockStore)
	store.On("BrokerOffsets").Return(bo)
	store.On("ConsumerOffsets").Return(co)

	reporters := &kage.Reporters{}

	reporter := new(mocks.MockReporter)
	reporter.On("ReportBrokerOffsets", &bo).Return()
	reporter.On("ReportConsumerOffsets", &co).Return()
	reporters.Add("test", reporter)

	app := &kage.Application{
		Store:     store,
		Reporters: reporters,
	}

	app.Report()

	store.AssertExpectations(t)
	reporter.AssertExpectations(t)
}

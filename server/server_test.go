package server_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/msales/kage"
	"github.com/msales/kage/server"
	"github.com/msales/kage/testutil"
	"github.com/msales/kage/testutil/mocks"
	"github.com/stretchr/testify/assert"
)

func TestServer_BrokersHandler(t *testing.T) {
	req, err := http.NewRequest("GET", "/brokers", nil)
	if err != nil {
		t.Fatal(err)
	}

	rr := httptest.NewRecorder()

	broker := new(mocks.MockKafkaBroker)
	broker.On("ID").Return(0)
	broker.On("Connected").Return(false)

	kafka := new(mocks.MockKafka)
	kafka.On("Brokers").Return([]kage.KafkaBroker{broker})

	app := &kage.Application{Kafka: kafka}

	srv := server.New(app)
	srv.ServeHTTP(rr, req)

	want := "[{\"id\":0,\"connected\":false}]"
	assert.Equal(t, http.StatusOK, rr.Code)
	assert.Equal(t, want, rr.Body.String())
}

func TestServer_BrokersHealthHandler(t *testing.T) {
	req, err := http.NewRequest("GET", "/brokers/health", nil)
	if err != nil {
		t.Fatal(err)
	}

	rr := httptest.NewRecorder()

	broker := new(mocks.MockKafkaBroker)
	broker.On("ID").Return(0)
	broker.On("Connected").Return(true).Once()
	broker.On("Connected").Return(false).Once()

	kafka := new(mocks.MockKafka)
	kafka.On("Brokers").Return([]kage.KafkaBroker{broker})

	app := &kage.Application{Kafka: kafka}

	srv := server.New(app)
	srv.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)

	srv.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusInternalServerError, rr.Code)
}

func TestServer_TopicsHandler(t *testing.T) {
	req, err := http.NewRequest("GET", "/topics", nil)
	if err != nil {
		t.Fatal(err)
	}

	rr := httptest.NewRecorder()

	bo := kage.BrokerOffsets{
		"test": []*kage.BrokerOffset{{OldestOffset: 0, NewestOffset: 100, Timestamp: 0}},
	}

	store := new(mocks.MockStore)
	store.On("BrokerOffsets").Return(bo)

	app := &kage.Application{Store: store}

	srv := server.New(app)
	srv.ServeHTTP(rr, req)

	want := "[{\"topic\":\"test\",\"total_available\":100,\"partitions\":[{\"partition\":0,\"oldest\":0,\"newest\":100,\"available\":100}]}]"
	assert.Equal(t, http.StatusOK, rr.Code)
	assert.Equal(t, want, rr.Body.String())
}

func TestServer_ConsumerGroupsHandler(t *testing.T) {
	req, err := http.NewRequest("GET", "/consumers", nil)
	if err != nil {
		t.Fatal(err)
	}

	rr := httptest.NewRecorder()

	co := kage.ConsumerOffsets{
		"test": map[string][]*kage.ConsumerOffset{
			"test": {{Offset: 0, Lag: 100, Timestamp: 0}},
		},
	}

	store := new(mocks.MockStore)
	store.On("ConsumerOffsets").Return(co)

	app := &kage.Application{Store: store}

	srv := server.New(app)
	srv.ServeHTTP(rr, req)

	want := "[{\"group\":\"test\",\"topic\":\"test\",\"total_lag\":100,\"partitions\":[{\"partition\":0,\"offset\":0,\"lag\":100}]}]"
	assert.Equal(t, http.StatusOK, rr.Code)
	assert.Equal(t, want, rr.Body.String())
}

func TestServer_ConsumerGroupHandler(t *testing.T) {
	req, err := http.NewRequest("GET", "/consumers/test", nil)
	if err != nil {
		t.Fatal(err)
	}

	rr := httptest.NewRecorder()

	co := kage.ConsumerOffsets{
		"test": map[string][]*kage.ConsumerOffset{
			"test": {{Offset: 0, Lag: 100, Timestamp: 0}},
		},
	}

	store := new(mocks.MockStore)
	store.On("ConsumerOffsets").Return(co)

	app := &kage.Application{Store: store}

	srv := server.New(app)
	srv.ServeHTTP(rr, req)

	want := "[{\"group\":\"test\",\"topic\":\"test\",\"total_lag\":100,\"partitions\":[{\"partition\":0,\"offset\":0,\"lag\":100}]}]"
	assert.Equal(t, http.StatusOK, rr.Code)
	assert.Equal(t, want, rr.Body.String())
}

func TestServer_ConsumerGroupHandler_NotFound(t *testing.T) {
	req, err := http.NewRequest("GET", "/consumers/none", nil)
	if err != nil {
		t.Fatal(err)
	}

	rr := httptest.NewRecorder()

	co := kage.ConsumerOffsets{
		"test": map[string][]*kage.ConsumerOffset{
			"test": {{Offset: 0, Lag: 100, Timestamp: 0}},
		},
	}

	store := new(mocks.MockStore)
	store.On("ConsumerOffsets").Return(co)

	app := &kage.Application{Store: store}

	srv := server.New(app)
	srv.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusNotFound, rr.Code)
}

func TestServer_HealthPass(t *testing.T) {
	req, err := http.NewRequest("GET", "/health", nil)
	if err != nil {
		t.Fatal(err)
	}

	rr := httptest.NewRecorder()

	reporters := &kage.Reporters{}
	reporter := new(mocks.MockReporter)
	reporter.On("IsHealthy").Return(true)
	reporters.Add("test", reporter)

	kafka := new(mocks.MockKafka)
	kafka.On("IsHealthy").Return(true)

	app := &kage.Application{
		Reporters: reporters,
		Kafka:     kafka,
	}

	srv := server.New(app)
	srv.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)
}

func TestServer_HealthFail(t *testing.T) {
	req, err := http.NewRequest("GET", "/health", nil)
	if err != nil {
		t.Fatal(err)
	}

	rr := httptest.NewRecorder()

	srv := server.New(&kage.Application{Logger: testutil.Logger})
	srv.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusInternalServerError, rr.Code)
}

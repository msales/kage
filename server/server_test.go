package server_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/msales/kage"
	"github.com/msales/kage/kafka"
	"github.com/msales/kage/server"
	"github.com/msales/kage/testutil"
	"github.com/msales/kage/testutil/mocks"
	"github.com/stretchr/testify/assert"
)

func TestBrokersHandler(t *testing.T) {
	req, err := http.NewRequest("GET", "/brokers", nil)
	if err != nil {
		t.Fatal(err)
	}

	rr := httptest.NewRecorder()

	monitor := new(mocks.MockMonitor)
	monitor.On("Brokers").Return([]kafka.Broker{{ID: 0, Connected: false}})

	app := &kage.Application{Monitor: monitor}

	srv := server.New(app)
	srv.ServeHTTP(rr, req)

	want := "[{\"id\":0,\"connected\":false}]"
	assert.Equal(t, http.StatusOK, rr.Code)
	assert.Equal(t, want, rr.Body.String())
}

func TestBrokersHealthHandler(t *testing.T) {
	req, err := http.NewRequest("GET", "/brokers/health", nil)
	if err != nil {
		t.Fatal(err)
	}

	rr := httptest.NewRecorder()

	monitor := new(mocks.MockMonitor)
	monitor.On("Brokers").Return([]kafka.Broker{{ID: 0, Connected: true}}).Once()
	monitor.On("Brokers").Return([]kafka.Broker{{ID: 0, Connected: false}}).Once()

	app := &kage.Application{Monitor: monitor}

	srv := server.New(app)
	srv.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)

	srv.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusInternalServerError, rr.Code)
}

func TestHealthPass(t *testing.T) {
	req, err := http.NewRequest("GET", "/health", nil)
	if err != nil {
		t.Fatal(err)
	}

	rr := httptest.NewRecorder()

	reporters := &kage.Reporters{}
	reporter := new(mocks.MockReporter)
	reporter.On("IsHealthy").Return(true)
	reporters.Add("test", reporter)

	monitor := new(mocks.MockMonitor)
	monitor.On("IsHealthy").Return(true)

	app := &kage.Application{
		Reporters: reporters,
		Monitor:   monitor,
	}

	srv := server.New(app)
	srv.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)
}

func TestHealthFail(t *testing.T) {
	req, err := http.NewRequest("GET", "/health", nil)
	if err != nil {
		t.Fatal(err)
	}

	rr := httptest.NewRecorder()

	srv := server.New(&kage.Application{Logger: testutil.Logger})
	srv.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusInternalServerError, rr.Code)
}

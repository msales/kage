package server_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/msales/kage"
	"github.com/msales/kage/server"
	"github.com/msales/kage/store"
	"github.com/msales/kage/testutil/mocks"
	"github.com/stretchr/testify/assert"
)

func TestConsumerGroupsHandler(t *testing.T) {
	req, err := http.NewRequest("GET", "/consumers", nil)
	if err != nil {
		t.Fatal(err)
	}

	rr := httptest.NewRecorder()

	co := store.ConsumerOffsets{
		"test": map[string][]*store.ConsumerOffset{
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

func TestConsumerGroupHandler(t *testing.T) {
	req, err := http.NewRequest("GET", "/consumers/test", nil)
	if err != nil {
		t.Fatal(err)
	}

	rr := httptest.NewRecorder()

	co := store.ConsumerOffsets{
		"test": map[string][]*store.ConsumerOffset{
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

func TestConsumerGroupHandler_NotFound(t *testing.T) {
	req, err := http.NewRequest("GET", "/consumers/none", nil)
	if err != nil {
		t.Fatal(err)
	}

	rr := httptest.NewRecorder()

	co := store.ConsumerOffsets{
		"test": map[string][]*store.ConsumerOffset{
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

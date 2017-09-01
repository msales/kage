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

func TestTopicsHandler(t *testing.T) {
	req, err := http.NewRequest("GET", "/topics", nil)
	if err != nil {
		t.Fatal(err)
	}

	rr := httptest.NewRecorder()

	bo := store.BrokerOffsets{
		"test": []*store.BrokerOffset{{OldestOffset: 0, NewestOffset: 100, Timestamp: 0}},
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

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

func TestMetadataHandler(t *testing.T) {
	req, err := http.NewRequest("GET", "/metadata", nil)
	if err != nil {
		t.Fatal(err)
	}

	rr := httptest.NewRecorder()

	bo := store.BrokerMetadata{
		"test": []*store.Metadata{{Leader: 1, Replicas: []int32{1, 2}, Isr: []int32{1, 2}, Timestamp: 0}},
	}

	store := new(mocks.MockStore)
	store.On("BrokerMetadata").Return(bo)

	app := &kage.Application{Store: store}

	srv := server.New(app)
	srv.ServeHTTP(rr, req)

	want := "[{\"topic\":\"test\",\"partitions\":[{\"partition\":0,\"leader\":1,\"replicas\":[1,2],\"isr\":[1,2]}]}]"
	assert.Equal(t, http.StatusOK, rr.Code)
	assert.Equal(t, want, rr.Body.String())
}

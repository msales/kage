package server_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/msales/kage"
	"github.com/msales/kage/server"
	"github.com/msales/kage/testutil"
	"github.com/stretchr/testify/assert"
)

func TestServer_HealthFail(t *testing.T) {
	req, err := http.NewRequest("GET", "/health", nil)
	if err != nil {
		t.Fatal(err)
	}

	rr := httptest.NewRecorder()

	srv := server.New(&kage.Application{Logger: testutil.Logger})
	srv.ServeHTTP(rr, req)

	assert.Equal(t, rr.Code, http.StatusInternalServerError)
}

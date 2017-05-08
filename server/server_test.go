package server_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/msales/kage/server"
	"github.com/msales/kage/testutil"
	"github.com/msales/kage/testutil/mocks"
)

func TestServer_HealthPass(t *testing.T) {
	svc := &mocks.MockService{Health: true}

	req, err := http.NewRequest("GET", "/health", nil)
	if err != nil {
		t.Fatal(err)
	}

	rr := httptest.NewRecorder()

	srv := server.New([]server.Service{svc}, testutil.Logger)
	srv.ServeHTTP(rr, req)

	if status := rr.Code; status != http.StatusOK {
		t.Errorf("handler returned wrong status code: got %v want %v", status, http.StatusOK)
	}
}

func TestServer_HealthFail(t *testing.T) {
	svc := &mocks.MockService{Health: false}

	req, err := http.NewRequest("GET", "/health", nil)
	if err != nil {
		t.Fatal(err)
	}

	rr := httptest.NewRecorder()

	srv := server.New([]server.Service{svc}, testutil.Logger)
	srv.ServeHTTP(rr, req)

	if status := rr.Code; status != http.StatusInternalServerError {
		t.Errorf("handler returned wrong status code: got %v want %v", status, http.StatusInternalServerError)
	}
}

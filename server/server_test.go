package server

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/msales/kage/testutil/mocks"
	"github.com/tendermint/log15"
)

func TestServer_HealthPass(t *testing.T) {
	svc := &mocks.MockService{Health: true}

	req, err := http.NewRequest("GET", "/health", nil)
	if err != nil {
		t.Fatal(err)
	}

	rr := httptest.NewRecorder()

	srv := New("", []Service{svc}, log15.New())
	srv.handleHealth(rr, req)

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

	srv := New("", []Service{svc}, log15.New())
	srv.handleHealth(rr, req)

	if status := rr.Code; status != http.StatusInternalServerError {
		t.Errorf("handler returned wrong status code: got %v want %v", status, http.StatusInternalServerError)
	}
}

func TestServer(t *testing.T) {
	svc := &mocks.MockService{Health: true}

	srv := New("127.0.0.1:0", []Service{svc}, log15.New())
	srv.Start()
	defer srv.Shutdown()

	resp, err := http.Get("http://" + srv.ln.Addr().String() + "/health")
	if err != nil {
		t.Fatal(err)
	}

	if status := resp.StatusCode; status != http.StatusOK {
		t.Errorf("handler returned wrong status code: got %v want %v", status, http.StatusOK)
	}
}

package server

import (
	"net/http"

	"github.com/msales/kage"
)

// Server represents an http server.
type Server struct {
	*kage.Application

	mux *http.ServeMux
}

// New creates a new instance of Server.
func New(app *kage.Application) *Server {
	s := &Server{
		Application: app,
		mux:         http.NewServeMux(),
	}

	//s.mux.H

	s.mux.HandleFunc("/health", http.HandlerFunc(s.HandleHealth))

	return s
}

// ServeHTTP dispatches the request to the handler whose
// pattern most closely matches the request URL.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mux.ServeHTTP(w, r)
}

// HandleHealth handles health requests.
func (s *Server) HandleHealth(w http.ResponseWriter, r *http.Request) {
	if !s.IsHealthy() {
		w.WriteHeader(500)
	}

	w.WriteHeader(200)
}

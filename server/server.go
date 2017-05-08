package server

import (
	"net/http"

	"gopkg.in/inconshreveable/log15.v2"
)

// Service represents a service with health checks.
type Service interface {
	// IsHealthy checks the health of the service.
	IsHealthy() bool
}

// Server represents an http server.
type Server struct {
	mux *http.ServeMux

	services []Service

	logger log15.Logger
}

// New create and returns a new Server.
func New(services []Service, logger log15.Logger) *Server {
	s := &Server{
		mux:      http.NewServeMux(),
		services: services,
		logger:   logger,
	}

	s.mux.HandleFunc("/health", http.HandlerFunc(s.HandleHealth))

	return s
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mux.ServeHTTP(w, r)
}

func (s *Server) HandleHealth(w http.ResponseWriter, r *http.Request) {
	code := 200

	for _, service := range s.services {
		if !service.IsHealthy() {
			code = 500

			break
		}
	}

	w.WriteHeader(code)
}

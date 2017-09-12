package server

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/go-zoo/bone"
	"github.com/msales/kage"
)

// Server represents an http server.
type Server struct {
	*kage.Application

	mux *bone.Mux
}

// New creates a new instance of Server.
func New(app *kage.Application) *Server {
	s := &Server{
		Application: app,
		mux:         bone.New(),
	}

	s.mux.GetFunc("/brokers", s.BrokersHandler)
	s.mux.GetFunc("/brokers/health", s.BrokersHealthHandler)
	s.mux.GetFunc("/metadata", s.MetadataHandler)
	s.mux.GetFunc("/topics", s.TopicsHandler)
	s.mux.GetFunc("/consumers", s.ConsumerGroupsHandler)
	s.mux.GetFunc("/consumers/:group", s.ConsumerGroupHandler)

	s.mux.GetFunc("/health", s.HealthHandler)

	return s
}

// ServeHTTP dispatches the request to the handler whose
// pattern most closely matches the request URL.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mux.ServeHTTP(w, r)
}

type brokerStatus struct {
	ID        int32 `json:"id"`
	Connected bool  `json:"connected"`
}

// BrokersHandler handles requests for brokers status.
func (s *Server) BrokersHandler(w http.ResponseWriter, r *http.Request) {
	brokers := []brokerStatus{}
	for _, b := range s.Monitor.Brokers() {
		brokers = append(brokers, brokerStatus{
			ID:        b.ID,
			Connected: b.Connected,
		})
	}

	s.writeJSON(w, brokers)
}

// BrokersHealthHandler handles requests for brokers health.
func (s *Server) BrokersHealthHandler(w http.ResponseWriter, r *http.Request) {
	for _, b := range s.Monitor.Brokers() {
		if !b.Connected {
			w.WriteHeader(500)
			return
		}
	}
}

// HealthHandler handles health requests.
func (s *Server) HealthHandler(w http.ResponseWriter, r *http.Request) {
	if !s.IsHealthy() {
		w.WriteHeader(500)
		return
	}

	w.WriteHeader(200)
}

func (s *Server) writeJSON(w http.ResponseWriter, v interface{}) {
	data, err := json.Marshal(v)
	if err != nil {
		w.WriteHeader(500)
		s.Logger.Error(fmt.Sprintf("server: error writing json: %s", err))
		return
	}

	w.Write(data)
}

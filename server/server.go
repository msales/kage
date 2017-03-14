package server

import (
	"net"
	"net/http"

	"github.com/inconshreveable/log15"
)

type Service interface {
	IsHealthy() bool
}

type Server struct {
	addr       string
	ln         *net.TCPListener
	shutdownCh chan struct{}

	services []Service

	logger log15.Logger
}

func New(addr string, services []Service, logger log15.Logger) *Server {
	return &Server{
		addr:     addr,
		shutdownCh: make(chan struct{}),
		services: services,
		logger:   logger,
	}
}

// Start starts the service.
func (s *Server) Start() error {
	addr, err := net.ResolveTCPAddr("tcp", s.addr)
	if err != nil {
		panic(err)
	}

	ln, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return err
	}
	s.ln = ln

	http.HandleFunc("/health", http.HandlerFunc(s.handleHealth))
	go func() {
		if err := http.Serve(s.ln, http.DefaultServeMux); err != nil {
			s.logger.Crit(err.Error())
		}
	}()

	return nil
}

// Close closes the service.
func (s *Server) Shutdown() {
	close(s.shutdownCh)
	s.ln.Close()
	return
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	code := 200

	for _, service := range s.services {
		if !service.IsHealthy() {
			code = 500

			break
		}
	}

	w.WriteHeader(code)
}

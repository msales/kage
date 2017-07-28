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
	Connected bool `json:"connected"`
}

// BrokersHandler handles requests for brokers status.
func (s *Server) BrokersHandler(w http.ResponseWriter, r *http.Request) {
	brokers := []brokerStatus{}
	for _, b := range s.Kafka.Brokers() {
		brokers = append(brokers, brokerStatus{
			ID:        b.ID(),
			Connected: b.Connected(),
		})
	}

	s.writeJson(w, brokers)
}

// BrokersHealthHandler handles requests for brokers health.
func (s *Server) BrokersHealthHandler(w http.ResponseWriter, r *http.Request) {
	for _, b := range s.Kafka.Brokers() {
		if !b.Connected() {
			w.WriteHeader(500)
			return
		}
	}
}

type brokerTopics struct {
	Topic          string            `json:"topic"`
	TotalAvailable int64             `json:"total_available"`
	Partitions     []brokerPartition `json:"partitions"`
}

type brokerPartition struct {
	Partition int   `json:"partition"`
	Oldest    int64 `json:"oldest"`
	Newest    int64 `json:"newest"`
	Available int64 `json:"available"`
}

// TopicsHandler handles requests for topic offsets.
func (s *Server) TopicsHandler(w http.ResponseWriter, r *http.Request) {
	offsets := s.Store.BrokerOffsets()

	topics := []brokerTopics{}
	for topic, partitions := range offsets {
		bt := brokerTopics{
			Topic:      topic,
			Partitions: make([]brokerPartition, len(partitions)),
		}

		for i, partition := range partitions {
			if partition == nil {
				continue
			}

			bp := brokerPartition{
				Partition: i,
				Oldest:    partition.OldestOffset,
				Newest:    partition.NewestOffset,
				Available: partition.NewestOffset - partition.OldestOffset,
			}

			bt.TotalAvailable += bp.Available
			bt.Partitions[i] = bp
		}

		topics = append(topics, bt)
	}

	s.writeJson(w, topics)
}

type consumerGroup struct {
	Group      string              `json:"group"`
	Topic      string              `json:"topic"`
	TotalLag   int64               `json:"total_lag"`
	Partitions []consumerPartition `json:"partitions"`
}

type consumerPartition struct {
	Partition int   `json:"partition"`
	Offset    int64 `json:"offset"`
	Lag       int64 `json:"lag"`
}

// ConsumerGroupsHandler handles requests for consumer groups offsets.
func (s *Server) ConsumerGroupsHandler(w http.ResponseWriter, r *http.Request) {
	offsets := s.Store.ConsumerOffsets()

	groups := []consumerGroup{}
	for group, topics := range offsets {
		groups = append(groups, createConsumerGroup(group, topics)...)
	}

	s.writeJson(w, groups)
}

// ConsumerGroupHandler handles requests for a consumer group offsets.
func (s *Server) ConsumerGroupHandler(w http.ResponseWriter, r *http.Request) {
	offsets := s.Store.ConsumerOffsets()

	group := bone.GetValue(r, "group")
	topics, ok := offsets[group]
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	groups := createConsumerGroup(group, topics)

	s.writeJson(w, groups)
}

func createConsumerGroup(group string, topics map[string][]*kage.ConsumerOffset) []consumerGroup {
	groups := []consumerGroup{}
	for topic, partitions := range topics {
		bt := consumerGroup{
			Group:      group,
			Topic:      topic,
			Partitions: make([]consumerPartition, len(partitions)),
		}

		for i, partition := range partitions {
			if partition == nil {
				continue
			}

			bp := consumerPartition{
				Partition: i,
				Offset:    partition.Offset,
				Lag:       partition.Lag,
			}

			bt.TotalLag += bp.Lag
			bt.Partitions[i] = bp
		}

		groups = append(groups, bt)
	}

	return groups
}

// HealthHandler handles health requests.
func (s *Server) HealthHandler(w http.ResponseWriter, r *http.Request) {
	if !s.IsHealthy() {
		w.WriteHeader(500)
		return
	}

	w.WriteHeader(200)
}

func (s *Server) writeJson(w http.ResponseWriter, v interface{}) {
	data, err := json.Marshal(v)
	if err != nil {
		w.WriteHeader(500)
		s.Logger.Error(fmt.Sprintf("server: error writing json: %s", err))
		return
	}

	w.Write(data)
}

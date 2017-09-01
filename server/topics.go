package server

import (
	"net/http"
)

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

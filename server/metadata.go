package server

import (
	"net/http"
)

type topicMetadata struct {
	Topic      string              `json:"topic"`
	Partitions []partitionMetadata `json:"partitions"`
}

type partitionMetadata struct {
	Partition int     `json:"partition"`
	Leader    int32   `json:"leader"`
	Replicas  []int32 `json:"replicas"`
	Isr       []int32 `json:"isr"`
}

// MetadataHandler handles requests for topic metadata.
func (s *Server) MetadataHandler(w http.ResponseWriter, r *http.Request) {
	metadata := s.Store.BrokerMetadata()

	topics := []topicMetadata{}
	for topic, partitions := range metadata {
		bt := topicMetadata{
			Topic:      topic,
			Partitions: make([]partitionMetadata, len(partitions)),
		}

		for i, partition := range partitions {
			if partition == nil {
				continue
			}

			pm := partitionMetadata{
				Partition: i,
				Leader:    partition.Leader,
				Replicas:  partition.Replicas,
				Isr:       partition.Isr,
			}
			bt.Partitions[i] = pm
		}

		topics = append(topics, bt)
	}

	s.writeJSON(w, topics)
}

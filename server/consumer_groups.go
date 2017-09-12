package server

import (
	"net/http"

	"github.com/go-zoo/bone"
	"github.com/msales/kage/store"
)

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

	s.writeJSON(w, groups)
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

	s.writeJSON(w, groups)
}

func createConsumerGroup(group string, topics map[string][]*store.ConsumerOffset) []consumerGroup {
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

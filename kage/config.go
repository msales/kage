package kage

import "encoding/json"

type Config struct {
	Log        LogConfig `json:"log"`
	LogMetrics bool      `json:"log-metrics"`

	Kafka     KafkaConfig  `json:"kafka"`
	Reporters Reporters `json:"reporters"`
}

type LogConfig struct {
	Path  string `json:"path"`
	Level string `json:"level"`
}

type KafkaConfig struct {
	Brokers []string          `json:"brokers"`
	Ignore  KafkaIgnoreConfig `json:"ignore"`
}

type KafkaIgnoreConfig struct {
	Topics []string `json:"topics"`
	Groups []string `json:"groups"`
}

type Reporters map[string]json.RawMessage

type InfluxReporterConfig struct {
	Address  string            `json:"address"`
	Username string            `json:"username"`
	Password string            `json:"password"`
	Database string            `json:"database"`
	Metric   string            `json:"metric"`
	Tags     map[string]string `json:"tags"`
}

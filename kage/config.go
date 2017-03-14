package kage

import "encoding/json"

// Config represents the application configuration.
type Config struct {
	Log        LogConfig `json:"log"`
	LogMetrics bool      `json:"log-metrics"`

	Kafka     KafkaConfig  `json:"kafka"`
	Reporters Reporters    `json:"reporters"`
	Server    ServerConfig `json:"server"`
}

// LogConfig represents the configuration for logging.
type LogConfig struct {
	Path  string `json:"path"`
	Level string `json:"level"`
}

// KafkaConfig represents the configuration for Kafka.
type KafkaConfig struct {
	Brokers []string          `json:"brokers"`
	Ignore  KafkaIgnoreConfig `json:"ignore"`
}

// KafkaIgnoreConfig represents the configuration for ignored topics and groups.
type KafkaIgnoreConfig struct {
	Topics []string `json:"topics"`
	Groups []string `json:"groups"`
}

// ServerConfig represents the configuration for the server.
type ServerConfig struct {
	Address string `json:"address"`
}

// Reporters represents the configuration for the reporters.
type Reporters map[string]json.RawMessage

// InfluxReporterConfig represents the configuration for the influx reporter.
type InfluxReporterConfig struct {
	Address  string            `json:"address"`
	Username string            `json:"username"`
	Password string            `json:"password"`
	Database string            `json:"database"`
	Metric   string            `json:"metric"`
	Tags     map[string]string `json:"tags"`
}

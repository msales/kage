package kage

// Config represents the application configuration.
type Config struct {
	Log      string `json:"log"`
	LogFile  string `json:"log-file"`
	LogLevel string `json:"log-level"`

	Kafka KafkaConfig  `json:"kafka"`

	Reporters []string     `json:"reporters"`
	Influx    InfluxConfig `json:"influx,omitempty"`

	Server ServerConfig `json:"server,omitempty"`
}

// KafkaConfig represents the configuration for Kafka.
type KafkaConfig struct {
	Brokers []string          `json:"brokers"`
	Ignore  KafkaIgnoreConfig `json:"ignore,omitempty"`
}

// KafkaIgnoreConfig represents the configuration for ignored topics and groups.
type KafkaIgnoreConfig struct {
	Topics []string `json:"topics,omitempty"`
	Groups []string `json:"groups,omitempty"`
}

// InfluxConfig represents the configuration for the influx reporter.
type InfluxConfig struct {
	DSN    string            `json:"dsn"`
	Metric string            `json:"metric"`
	Tags   map[string]string `json:"tags"`
}

// ServerConfig represents the configuration for the server.
type ServerConfig struct {
	Address string `json:"address,omitempty"`
}

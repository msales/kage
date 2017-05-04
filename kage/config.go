package kage

// Config represents the application configuration.
type Config struct {
	Log      string `json:"log"`
	LogFile  string `json:"log-file"`
	LogLevel string `json:"log-level"`

	Kafka KafkaConfig `json:"kafka"`

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
	Policy string            `json:"policy"`
	Tags   map[string]string `json:"tags"`
}

// ServerConfig represents the configuration for the server.
type ServerConfig struct {
	Address string `json:"address,omitempty"`
}

// MergeConfig  merges two configurations together to make a single new
// configuration.
func MergeConfig(a, b *Config) *Config {
	var result Config = *a

	// Log
	if b.Log != "" {
		result.Log = b.Log
	}

	if b.LogFile != "" {
		result.LogFile = b.LogFile
	}

	if b.LogLevel != "" {
		result.LogLevel = b.LogLevel
	}

	// Kafka
	if len(b.Kafka.Brokers) > 0 {
		result.Kafka.Brokers = b.Kafka.Brokers
	}

	if len(b.Kafka.Ignore.Topics) > 0 {
		result.Kafka.Ignore.Topics = b.Kafka.Ignore.Topics
	}

	if len(b.Kafka.Ignore.Groups) > 0 {
		result.Kafka.Ignore.Groups = b.Kafka.Ignore.Groups
	}

	// Reporters
	if len(b.Reporters) > 0 {
		result.Reporters = b.Reporters
	}

	if b.Influx.DSN != "" {
		result.Influx.DSN = b.Influx.DSN
	}

	if b.Influx.Metric != "" {
		result.Influx.Metric = b.Influx.Metric
	}

	if len(b.Influx.Tags) > 0 {
		result.Influx.Tags = b.Influx.Tags
	}

	// Server
	if b.Server.Address != "" {
		result.Server.Address = b.Server.Address
	}

	return &result
}

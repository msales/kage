package kage

type Config struct {
	Log        LogConfig `json:"log"`
	LogMetrics bool      `json:"log-metrics"`

	Kafka  KafkaConfig  `json:"kafka"`
	Influx InfluxConfig `json:"influx"`
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

type InfluxConfig struct {
	Address  string            `json:"address"`
	Username string            `json:"username"`
	Password string            `json:"password"`
	Database string            `json:"database"`
	Metric   string            `json:"metric"`
	Tags     map[string]string `json:"tags"`
}

package kage

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMergeConfig(t *testing.T) {
	a := &Config{
		Log:      "stdout",
		LogFile:  "foo",
		LogLevel: "info",
		Kafka: KafkaConfig{
			Brokers: []string{"foo"},
			Ignore: KafkaIgnoreConfig{
				Topics: []string{"foo"},
				Groups: []string{"foo"},
			},
		},
		Reporters: []string{"foo"},
		Influx: InfluxConfig{
			DSN:    "foo",
			Metric: "foo",
			Tags:   map[string]string{"foo": "foo"},
		},
		Server: ServerConfig{
			Address: "foo",
		},
	}

	b := &Config{
		Log:      "file",
		LogFile:  "bar",
		LogLevel: "debug",
		Kafka: KafkaConfig{
			Brokers: []string{"bar"},
			Ignore: KafkaIgnoreConfig{
				Topics: []string{"bar"},
				Groups: []string{"bar"},
			},
		},
		Reporters: []string{"bar"},
		Influx: InfluxConfig{
			DSN:    "bar",
			Metric: "bar",
			Tags:   map[string]string{"bar": "bar"},
		},
		Server: ServerConfig{
			Address: "bar",
		},
	}

	c := MergeConfig(a, b)

	assert.Equal(t, b, c)

}

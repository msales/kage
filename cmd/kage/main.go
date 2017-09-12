package main

import (
	"os"

	"gopkg.in/urfave/cli.v1"
)

import _ "github.com/joho/godotenv/autoload"

// Flag constants declared for CLI use.
const (
	FlagLog      = "log"
	FlagLogFile  = "log.file"
	FlagLogLevel = "log.level"

	FlagKafkaBrokers      = "kafka.brokers"
	FlagKafkaIgnoreTopics = "kafka.ignore-topics"
	FlagKafkaIgnoreGroups = "kafka.ignore-groups"

	FlagReporters = "reporters"

	FlagInflux       = "influx"
	FlagInfluxMetric = "influx.metric"
	FlagInfluxPolicy = "influx.policy"
	FlagInfluxTags   = "influx.tags"

	FlagServer = "server"
	FlagPort   = "port"
)

var commonFlags = []cli.Flag{
	cli.StringFlag{
		Name:   FlagLog,
		Value:  "stdout",
		Usage:  "The type of log to use (options: \"stdout\", \"file\")",
		EnvVar: "KAGE_LOG",
	},
	cli.StringFlag{
		Name:   FlagLogFile,
		Usage:  "The path to the file",
		EnvVar: "KAGE_LOG_FILE",
	},
	cli.StringFlag{
		Name:   FlagLogLevel,
		Value:  "info",
		Usage:  "Specify the log level (options: \"debug\", \"info\", \"warn\", \"error\")",
		EnvVar: "KAGE_LOG_LEVEL",
	},
}

var commands = []cli.Command{
	{
		Name:  "agent",
		Usage: "Run the kage agent",
		Flags: append([]cli.Flag{
			cli.StringSliceFlag{
				Name:   FlagKafkaBrokers,
				Usage:  "Specify the Kafka seed brokers",
				EnvVar: "KAGE_KAFKA_BROKERS",
			},
			cli.StringSliceFlag{
				Name:   FlagKafkaIgnoreTopics,
				Usage:  "Specify the Kafka topic patterns to ignore (may contain wildcards)",
				EnvVar: "KAGE_KAFKA_IGNORE_TOPICS",
			},
			cli.StringSliceFlag{
				Name:   FlagKafkaIgnoreGroups,
				Usage:  "Specify the Kafka group patterns to ignore (may contain wildcards)",
				EnvVar: "KAGE_KAFKA_IGNORE_GROUPS",
			},

			cli.StringSliceFlag{
				Name:   FlagReporters,
				Value:  &cli.StringSlice{"stdout"},
				Usage:  "Specify the reporters to use (options: \"influx\", \"stdout\")",
				EnvVar: "KAGE_REPORTERS",
			},

			cli.StringFlag{
				Name:   FlagInflux,
				Usage:  "Specify the InfluxDB DSN (e.g. \"http://user:pass@ip:port/database\")",
				EnvVar: "KAGE_INFLUX",
			},
			cli.StringFlag{
				Name:   FlagInfluxMetric,
				Value:  "kafka",
				Usage:  "Specify the InfluxDB metric name",
				EnvVar: "KAGE_INFLUX_METRIC",
			},
			cli.StringFlag{
				Name:   FlagInfluxPolicy,
				Usage:  "Specify the InfluxDB metric policy",
				EnvVar: "KAGE_INFLUX_POLICY",
			},
			cli.StringSliceFlag{
				Name:   FlagInfluxTags,
				Usage:  "Specify additions tags to add to all metrics (e.g. \"tag1=value\")",
				EnvVar: "KAGE_INFLUX_TAGS",
			},

			cli.BoolFlag{
				Name:   FlagServer,
				Usage:  "Start the http server",
				EnvVar: "KAGE_SERVER",
			},
			cli.StringFlag{
				Name:   FlagPort,
				Value:  "80",
				Usage:  "Specify the port to run the server on",
				EnvVar: "KAGE_PORT",
			},
		}, commonFlags...),
		Action: runServer,
	},
}

func main() {
	app := cli.NewApp()
	app.Name = "kage"
	app.Usage = "A Kafka monitoring agent"
	app.Version = Version
	app.Commands = commands

	app.Run(os.Args)
}

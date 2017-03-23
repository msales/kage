package main

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/msales/kage/kafka"
	"github.com/msales/kage/kage"
	"github.com/msales/kage/reporter"
	"github.com/msales/kage/server"
	"github.com/msales/kage/store"
	"gopkg.in/alecthomas/kingpin.v2"
	"gopkg.in/inconshreveable/log15.v2"
)

var (
	configPath = kingpin.Flag("config", "The path to the JSON configuration file. This will be overriden by any command line arguemnts").String()

	log = kingpin.Flag("log", "The type of log to use. Options: 'stdout', 'file'").Default("stdout").Enum("stdout", "file")
	logFile = kingpin.Flag("log-file", "The path to the file to log to. Only works when --log is set to file").Default("./kage.log").String()
	logLevel = kingpin.Flag("log-level", "The log level to use. Options: 'debug', 'info', 'warn', 'error'").Default("info").Enum("debug", "info", "warn", "error")

	brokers = kingpin.Flag("brokers", "The kafka seed brokers connect to, Format: 'ip:port'").Strings()
	ignoreTopics = kingpin.Flag("ignore-topics", "The kafka topic patterns to ignore. This may contian wildcards").Strings()
	ignoreGroups = kingpin.Flag("ignore-groups", "The kafka consumer group patterns to ignore. This may contian wildcards").Strings()

	reporters = kingpin.Flag("reporters", "The reporters to use. Options: 'influx', 'stdout'").Default("stdout").Enums("influx", "stdout")

	influx = kingpin.Flag("influx", "The DSN of the InfluxDB server to report to. Format: http://user:pass@ip:port/database'").URL()
	influxMetric = kingpin.Flag("influx-metric", "The measurement name to report statistics under").Default("kafka").String()
	influxTags = kingpin.Flag("influx-tags", "Additional tags to add to the statistics.").PlaceHolder("KEY:VALUE").StringMap()

	addr = kingpin.Flag("addr", "The address to bind to for the http server").String()
)

func main() {
	kingpin.Version(Version)
	kingpin.CommandLine.HelpFlag.Short('h')
	kingpin.CommandLine.VersionFlag.Short('v')
	kingpin.CommandLine.DefaultEnvars()
	kingpin.Parse()

	// Config
	config, err := readConfig(*configPath)
	if err != nil {
		panic(err)
	}

	lvl, err := log15.LvlFromString(config.Log.Level)
	if err != nil {
		panic(err)
	}

	// Logger
	log := log15.New()
	log.SetHandler(log15.StreamHandler(os.Stderr, log15.LogfmtFormat()))
	log.SetHandler(log15.LvlFilterHandler(
		lvl,
		log15.Must.FileHandler(config.Log.Path, log15.LogfmtFormat()),
	))

	// Store
	memStore, err := store.New()
	if err != nil {
		panic(err)
	}
	defer memStore.Shutdown()

	// Kafka
	kafkaClient, err := kafka.New(
		kafka.Log(log),
		kafka.Brokers(config.Kafka.Brokers),
		kafka.IgnoreTopics(config.Kafka.Ignore.Topics),
		kafka.IgnoreGroups(config.Kafka.Ignore.Groups),
		kafka.OffsetChannel(memStore.OffsetCh),
	)
	if err != nil {
		panic(err)
	}
	defer kafkaClient.Shutdown()

	// Reporters
	reporters, err := createReporters(config.Reporters, log)
	if err != nil {
		panic(err)
	}
	reportTicker := time.NewTicker(60 * time.Second)
	defer reportTicker.Stop()
	go func() {
		for range reportTicker.C {
			brokerOffsets := memStore.BrokerOffsets()
			reporters.ReportBrokerOffsets(&brokerOffsets)

			consumerOffsets := memStore.ConsumerOffsets()
			reporters.ReportConsumerOffsets(&consumerOffsets)
		}
	}()

	// Server
	services := []server.Service{kafkaClient}
	for _, r := range *reporters {
		services = append(services, r)
	}
	srv := server.New(
		config.Server.Address,
		services,
		log,
	)
	if err := srv.Start(); err != nil {
		panic(err)
	}
	defer srv.Shutdown()

	// Wait for quit
	quit := listenForSignals()
	<-quit
}

// readConfig reads the configuration from the given path
func readConfig(path string) (*kage.Config, error) {
	file, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}

	config := &kage.Config{}
	json.Unmarshal(file, config)

	return config, nil
}

// createReporters creates reporters from the config
func createReporters(reporters kage.Reporters, logger log15.Logger) (*reporter.Reporters, error) {
	rs := &reporter.Reporters{}

	for t, config := range reporters {
		switch t {
		case "influx":
			influxConfig := kage.InfluxReporterConfig{}
			json.Unmarshal(config, &influxConfig)

			r, err := reporter.NewInfluxReporter(
				reporter.Credentials(influxConfig.Address, influxConfig.Username, influxConfig.Password),
				reporter.Database(influxConfig.Database),
				reporter.Metric(influxConfig.Metric),
				reporter.Tags(influxConfig.Tags),
				reporter.Log(logger),
			)
			if err != nil {
				return nil, err
			}

			rs.Add(t, r)
			break

		case "console":
			r, err := reporter.NewConsoleReporter()
			if err != nil {
				return nil, err
			}

			rs.Add(t, r)
		}
	}

	return rs, nil
}

// Wait for SIGTERM to end the application.
func listenForSignals() chan bool {
	sigs := make(chan os.Signal, 1)
	done := make(chan bool, 1)

	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigs

		done <- true
	}()

	return done
}

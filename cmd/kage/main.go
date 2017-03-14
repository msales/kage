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
	"github.com/tendermint/log15"
	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	configPath = kingpin.Flag("config", "The path to the configuration file").Default("./kage.json").String()
)

func main() {
	kingpin.Version(Version)
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

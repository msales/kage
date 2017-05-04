package main

import (
	"net/url"
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

func main() {
	// Config
	config, err := readConfig(os.Args[1:])
	if err != nil {
		kingpin.Fatalf("Error reading configuration: %s", err.Error())
	}

	// Logger
	log := createLogger(config)

	// Store
	memStore, err := store.New()
	if err != nil {
		kingpin.Fatalf("Error starting store: %s", err.Error())
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
		kingpin.Fatalf("Error connecting to Kafka: %s", err.Error())
	}
	defer kafkaClient.Shutdown()

	// Reporters
	reporters, err := createReporters(config, log)
	if err != nil {
		kingpin.Fatalf("Error starting reporters: %s", err.Error())
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
	if config.Server.Address != "" {
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
			kingpin.Fatalf("Error starting server: %s", err.Error())
		}
		defer srv.Shutdown()
	}

	// Wait for quit
	quit := listenForSignals()
	<-quit
}

// createLogger creates a new logger from config
func createLogger(config *kage.Config) log15.Logger {
	lvl, err := log15.LvlFromString(config.LogLevel)
	if err != nil {
		kingpin.Fatalf("Error creating logger: %s", err.Error())
	}

	h := log15.StreamHandler(os.Stderr, log15.LogfmtFormat())
	if config.Log == "file" {
		h = log15.Must.FileHandler(config.LogFile, log15.LogfmtFormat())
	}

	log := log15.New()
	log.SetHandler(log15.LvlFilterHandler(
		lvl,
		h,
	))

	return log
}

// createReporters creates reporters from the config
func createReporters(config *kage.Config, logger log15.Logger) (*reporter.Reporters, error) {
	rs := &reporter.Reporters{}

	for _, name := range config.Reporters {
		switch name {
		case "influx":
			u, err := url.Parse(config.Influx.DSN)
			if err != nil {
				return nil, err
			}

			r, err := reporter.NewInfluxReporter(
				reporter.DSN(u),
				reporter.Metric(config.Influx.Metric),
				reporter.Policy(config.Influx.Policy),
				reporter.Tags(config.Influx.Tags),
				reporter.Log(logger),
			)
			if err != nil {
				return nil, err
			}

			rs.Add(name, r)
			break

		case "stdout":
			r, err := reporter.NewConsoleReporter()
			if err != nil {
				return nil, err
			}

			rs.Add(name, r)
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

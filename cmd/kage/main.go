package main

import (
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/influxdata/influxdb/client/v2"
	"github.com/msales/kage"
	"github.com/msales/kage/kafka"
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

		ln, err := runServer(config.Server.Address, services, log)
		if err != nil {
			kingpin.Fatalf("%s", err.Error())
		}
		defer ln.Close()
	}

	// Wait for quit
	quit := listenForSignals()
	<-quit
}

// createLogger creates a new logger from config.
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

// createReporters creates reporters from the config.
func createReporters(config *kage.Config, logger log15.Logger) (*reporter.Reporters, error) {
	rs := &reporter.Reporters{}

	for _, name := range config.Reporters {
		switch name {
		case "influx":
			dsn, err := url.Parse(config.Influx.DSN)
			if err != nil {
				return nil, err
			}

			c, err := createInfluxClient(dsn)
			if err != nil {
				return nil, err
			}
			db := strings.Trim(dsn.Path, "/")

			r := reporter.NewInfluxReporter(c,
				reporter.Database(db),
				reporter.Metric(config.Influx.Metric),
				reporter.Policy(config.Influx.Policy),
				reporter.Tags(config.Influx.Tags),
				reporter.Log(logger),
			)
			rs.Add(name, r)
			break

		case "stdout":
			r := reporter.NewConsoleReporter(os.Stdout)
			rs.Add(name, r)
		}
	}

	return rs, nil
}

// createInfluxClient creates an influx client from a DSN.
func createInfluxClient(dsn *url.URL) (client.Client, error) {
	if dsn.User == nil {
		dsn.User = &url.Userinfo{}
	}

	addr := dsn.Scheme + "://" + dsn.Host
	username := dsn.User.Username()
	password, _ := dsn.User.Password()

	return client.NewHTTPClient(client.HTTPConfig{
		Addr:     addr,
		Username: username,
		Password: password,
	})
}

// Create and start the http server.
func runServer(addr string, svcs []server.Service, logger log15.Logger) (*net.TCPListener, error) {
	bind, err := net.ResolveTCPAddr("tcp", addr)
	if err != nil {
		panic(err)
	}

	ln, err := net.ListenTCP("tcp", bind)
	if err != nil {
		return nil, err
	}

	srv := server.New(svcs, logger)

	go func() {
		if err := http.Serve(ln, srv); err != nil {
			logger.Crit(err.Error())
		}
	}()

	return ln, nil
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

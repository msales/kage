package main

import (
	"net/url"
	"os"
	"strings"

	"github.com/influxdata/influxdb/client/v2"
	"github.com/msales/kage"
	"github.com/msales/kage/kafka"
	"github.com/msales/kage/reporter"
	"github.com/msales/kage/store"
	"gopkg.in/inconshreveable/log15.v2"
)

// Application =============================

func newApplication(c *kage.Config) (*kage.Application, error) {
	logger, err := newLogger(c)
	if err != nil {
		return nil, err
	}

	memStore, err := store.New()
	if err != nil {
		return nil, err
	}

	reporters, err := newReporters(c, logger)
	if err != nil {
		return nil, err
	}

	monitor, err := kafka.New(
		kafka.Brokers(c.Kafka.Brokers),
		kafka.IgnoreTopics(c.Kafka.Ignore.Topics),
		kafka.IgnoreGroups(c.Kafka.Ignore.Groups),
		kafka.StateChannel(memStore.Channel()),
		kafka.Log(logger),
	)
	if err != nil {
		return nil, err
	}

	app := kage.NewApplication()
	app.Store = memStore
	app.Reporters = reporters
	app.Monitor = monitor
	app.Logger = logger

	return app, nil
}

// Reporters ===============================

// newReporters creates reporters from the config.
func newReporters(config *kage.Config, logger log15.Logger) (*kage.Reporters, error) {
	rs := &kage.Reporters{}

	for _, name := range config.Reporters {
		switch name {
		case "influx":
			r, err := newInfluxReporter(config.Influx, logger)
			if err != nil {
				return nil, err
			}
			rs.Add(name, r)
			break

		case "stdout":
			r := reporter.NewConsoleReporter(os.Stdout)
			rs.Add(name, r)
		}
	}

	return rs, nil
}

// newInfluxReporter create a new InfluxDB reporter.
func newInfluxReporter(config kage.InfluxConfig, logger log15.Logger) (kage.Reporter, error) {
	dsn, err := url.Parse(config.DSN)
	if err != nil {
		return nil, err
	}

	if dsn.User == nil {
		dsn.User = &url.Userinfo{}
	}

	addr := dsn.Scheme + "://" + dsn.Host
	username := dsn.User.Username()
	password, _ := dsn.User.Password()

	c, err := client.NewHTTPClient(client.HTTPConfig{
		Addr:     addr,
		Username: username,
		Password: password,
	})
	if err != nil {
		return nil, err
	}
	db := strings.Trim(dsn.Path, "/")

	return reporter.NewInfluxReporter(c,
		reporter.Database(db),
		reporter.Metric(config.Metric),
		reporter.Policy(config.Policy),
		reporter.Tags(config.Tags),
		reporter.Log(logger),
	), nil
}

// Logger ==================================

// newLogger creates a new logger from config.
func newLogger(config *kage.Config) (log15.Logger, error) {
	lvl, err := log15.LvlFromString(config.LogLevel)
	if err != nil {
		return nil, err
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

	return log, nil
}

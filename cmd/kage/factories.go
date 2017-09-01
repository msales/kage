package main

import (
	"fmt"
	"net/url"
	"os"
	"strings"

	"github.com/influxdata/influxdb/client/v2"
	"github.com/msales/kage"
	"github.com/msales/kage/kafka"
	"github.com/msales/kage/reporter"
	"github.com/msales/kage/store"
	"github.com/msales/kage/utils"
	"gopkg.in/inconshreveable/log15.v2"
	"gopkg.in/urfave/cli.v1"
)

// Application =============================

func newApplication(c *cli.Context) (*kage.Application, error) {
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
		kafka.Brokers(c.StringSlice(FlagKafkaBrokers)),
		kafka.IgnoreTopics(c.StringSlice(FlagKafkaIgnoreTopics)),
		kafka.IgnoreGroups(c.StringSlice(FlagKafkaIgnoreGroups)),
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
func newReporters(c *cli.Context, logger log15.Logger) (*kage.Reporters, error) {
	rs := &kage.Reporters{}

	for _, name := range c.StringSlice(FlagReporters) {
		switch name {
		case "influx":
			r, err := newInfluxReporter(c, logger)
			if err != nil {
				return nil, err
			}
			rs.Add(name, r)
			break

		case "stdout":
			r := reporter.NewConsoleReporter(os.Stdout)
			rs.Add(name, r)

		default:
			return nil, fmt.Errorf("unknown reporter \"%s\"", name)
		}
	}

	return rs, nil
}

// newInfluxReporter create a new InfluxDB reporter.
func newInfluxReporter(c *cli.Context, logger log15.Logger) (kage.Reporter, error) {
	dsn, err := url.Parse(c.String(FlagInflux))
	if err != nil {
		return nil, err
	}

	if dsn.User == nil {
		dsn.User = &url.Userinfo{}
	}

	addr := dsn.Scheme + "://" + dsn.Host
	username := dsn.User.Username()
	password, _ := dsn.User.Password()

	influx, err := client.NewHTTPClient(client.HTTPConfig{
		Addr:     addr,
		Username: username,
		Password: password,
	})
	if err != nil {
		return nil, err
	}
	db := strings.Trim(dsn.Path, "/")

	return reporter.NewInfluxReporter(influx,
		reporter.Database(db),
		reporter.Metric(c.String(FlagInfluxMetric)),
		reporter.Policy(c.String(FlagInfluxPolicy)),
		reporter.Tags(utils.SplitMap(c.StringSlice(FlagInfluxTags), "=")),
		reporter.Log(logger),
	), nil
}

// Logger ==================================

// newLogger creates a new logger from config.
func newLogger(c *cli.Context) (log15.Logger, error) {
	lvl, err := log15.LvlFromString(c.String(FlagLogLevel))
	if err != nil {
		return nil, err
	}

	h := log15.StreamHandler(os.Stderr, log15.LogfmtFormat())
	if c.String(FlagLog) == "file" {
		h = log15.Must.FileHandler(c.String(FlagLogFile), log15.LogfmtFormat())
	}

	log := log15.New()
	log.SetHandler(log15.LvlFilterHandler(
		lvl,
		h,
	))

	return log, nil
}

package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/inconshreveable/log15"
	"github.com/msales/kage/influx"
	"github.com/msales/kage/kafka"
	"github.com/msales/kage/kage"
	"github.com/msales/kage/store"
	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	configPath = kingpin.Flag("config", "The path to the configuration file").Default("./kage.json").String()
)

func main() {
	kingpin.Version(Version)
	kingpin.Parse()

	config, err := readConfig(*configPath)
	if err != nil {
		panic(err)
	}

	lvl, err := log15.LvlFromString(config.Log.Level)
	if err != nil {
		panic(err)
	}

	log := log15.New()
	log.SetHandler(log15.StreamHandler(os.Stderr, log15.LogfmtFormat()))
	log.SetHandler(log15.LvlFilterHandler(
		lvl,
		log15.Must.FileHandler(config.Log.Path, log15.LogfmtFormat()),
	))

	memStore, err := store.New()
	if err != nil {
		panic(err)
	}

	kafkaClient, err := kafka.New(
		kafka.Log(log),
		kafka.Brokers(config.Kafka.Brokers),
		kafka.OffsetChannel(memStore.OffsetCh),
	)
	if err != nil {
		panic(err)
	}

	influxDB, err := influx.New(
		influx.Credentials(config.Influx.Address, config.Influx.Username, config.Influx.Password),
		influx.Database(config.Influx.Database),
		influx.Tags(config.Influx.Tags),
	)
	if err != nil {
		panic(err)
	}

	reportTicker := writeToInflux(memStore, influxDB, log, config.LogMetrics)

	quit := listenForSignals()
	<-quit

	reportTicker.Stop()
	kafkaClient.Shutdown()
	memStore.Shutdown()
}

// Read the configuration from the given path
func readConfig(path string) (*kage.Config, error) {
	file, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}

	config := &kage.Config{}
	json.Unmarshal(file, config)

	return config, nil
}

// Write metrics from the MemoryStore to InfluxDB
func writeToInflux(memStore *store.MemoryStore, influxDB *influx.Client, log log15.Logger, logMetrics bool) *time.Ticker {
	reportTicker := time.NewTicker(60 * time.Second)
	go func() {
		for _ = range reportTicker.C {
			brokerOffsets := memStore.BrokerOffsets()
			consumerOffsets := memStore.ConsumerOffsets()

			pts := []*influx.Point{}
			for topic, partitions := range brokerOffsets {
				for partition, offset := range partitions {
					pt := &influx.Point{
						Measurement: "kafka",
						Tags: map[string]string{
							"type":      "BrokerOffset",
							"topic":     topic,
							"partition": fmt.Sprint(partition),
						},
						Fields: map[string]interface{}{
							"oldest":    offset.OldestOffset,
							"newest":    offset.NewestOffset,
							"available": offset.NewestOffset - offset.OldestOffset,
						},
						Time: time.Now(),
					}

					pts = append(pts, pt)

					if logMetrics {
						log.Info("broker",
							"topic", topic,
							"partition", partition,
							"oldest", offset.OldestOffset,
							"newest", offset.NewestOffset,
							"available", offset.NewestOffset-offset.OldestOffset,
						)
					}
				}
			}

			for group, topics := range consumerOffsets {
				for topic, partitions := range topics {
					for partition, offset := range partitions {
						pt := &influx.Point{
							Measurement: "kafka",
							Tags: map[string]string{
								"type":      "ConsumerOffset",
								"group":     group,
								"topic":     topic,
								"partition": fmt.Sprint(partition),
							},
							Fields: map[string]interface{}{
								"offset": offset.Offset,
								"lag":    offset.Lag,
							},
							Time: time.Now(),
						}

						pts = append(pts, pt)

						if logMetrics {
							log.Info("consumer",
								"group", group,
								"topic", topic,
								"partition", partition,
								"offset", offset.Offset,
								"lag", offset.Lag,
							)
						}
					}
				}
			}

			if err := influxDB.Write(pts); err != nil {
				log.Error(err.Error())
			}
		}
	}()

	return reportTicker
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

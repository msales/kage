package reporter

import (
	"fmt"
	"time"

	"github.com/influxdata/influxdb/client/v2"
	"github.com/msales/kage/kage"
	"gopkg.in/inconshreveable/log15.v2"
)

// InfluxReporterFunc represents a configuration function for InfluxReporter.
type InfluxReporterFunc func(c *InfluxReporter)

// Credentials configures the credentials on an InfluxReporter.
func Credentials(addr, username, password string) InfluxReporterFunc {
	return func(c *InfluxReporter) {
		c.addr = addr
		c.username = username
		c.password = password
	}
}

// Database configures the database on an InfluxReporter.
func Database(database string) InfluxReporterFunc {
	return func(c *InfluxReporter) {
		c.database = database
	}
}

// Log configures the logger on an InfluxReporter.
func Log(log log15.Logger) InfluxReporterFunc {
	return func(c *InfluxReporter) {
		c.log = log
	}
}

// Metric configures the metric name on an InfluxReporter.
func Metric(metric string) InfluxReporterFunc {
	return func(c *InfluxReporter) {
		c.metric = metric
	}
}

// Tags configures the additional tags on an InfluxReporter.
func Tags(tags map[string]string) InfluxReporterFunc {
	return func(c *InfluxReporter) {
		c.tags = tags
	}
}

// InfluxReporter represents an InfluxDB reporter.
type InfluxReporter struct {
	addr     string
	username string
	password string
	database string

	metric string
	tags   map[string]string

	client client.Client

	log log15.Logger
}

// NewInfluxReporter creates and returns a new NewInfluxReporter.
func NewInfluxReporter(opts ...InfluxReporterFunc) (*InfluxReporter, error) {
	r := &InfluxReporter{}

	for _, o := range opts {
		o(r)
	}

	c, err := client.NewHTTPClient(client.HTTPConfig{
		Addr:     r.addr,
		Username: r.username,
		Password: r.password,
	})
	if err != nil {
		return nil, err
	}
	r.client = c

	return r, nil
}

// ReportBrokerOffsets reports a snapshot of the broker offsets.
func (r InfluxReporter) ReportBrokerOffsets(o *kage.BrokerOffsets) {
	pts, err := client.NewBatchPoints(client.BatchPointsConfig{
		Database:  r.database,
		Precision: "s",
	})
	if err != nil {
		r.log.Error(err.Error())

		return
	}

	for topic, partitions := range *o {
		for partition, offset := range partitions {
			tags := map[string]string{
				"type":      "BrokerOffset",
				"topic":     topic,
				"partition": fmt.Sprint(partition),
			}

			for key, value := range r.tags {
				tags[key] = value
			}

			pt, err := client.NewPoint(
				r.metric,
				tags,
				map[string]interface{}{
					"oldest":    offset.OldestOffset,
					"newest":    offset.NewestOffset,
					"available": offset.NewestOffset - offset.OldestOffset,
				},
				time.Now(),
			)
			if err != nil {
				r.log.Error(err.Error())

				continue
			}

			pts.AddPoint(pt)
		}
	}

	if err := r.client.Write(pts); err != nil {
		r.log.Error(err.Error())
	}
}

// ReportConsumerOffsets reports a snapshot of the consumer group offsets.
func (r InfluxReporter) ReportConsumerOffsets(o *kage.ConsumerOffsets) {
	pts, err := client.NewBatchPoints(client.BatchPointsConfig{
		Database:  r.database,
		Precision: "s",
	})
	if err != nil {
		r.log.Error(err.Error())

		return
	}

	for group, topics := range *o {
		for topic, partitions := range topics {
			for partition, offset := range partitions {
				tags := map[string]string{
					"type":      "ConsumerOffset",
					"group":     group,
					"topic":     topic,
					"partition": fmt.Sprint(partition),
				}

				for key, value := range r.tags {
					tags[key] = value
				}

				pt, err := client.NewPoint(
					r.metric,
					tags,
					map[string]interface{}{
						"offset": offset.Offset,
						"lag":    offset.Lag,
					},
					time.Now(),
				)
				if err != nil {
					r.log.Error(err.Error())

					continue
				}

				pts.AddPoint(pt)
			}
		}
	}

	if err := r.client.Write(pts); err != nil {
		r.log.Error(err.Error())
	}
}

// IsHealthy checks the health of the InfluxReporter.
func (r InfluxReporter) IsHealthy() bool {
	_, _, err := r.client.Ping(100)

	if err != nil {
		r.log.Crit(err.Error())

		return false
	}

	return true
}

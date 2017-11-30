package reporter

import (
	"fmt"
	"math"
	"time"

	"github.com/influxdata/influxdb/client/v2"
	"github.com/msales/kage/store"
	"gopkg.in/inconshreveable/log15.v2"
)

// InfluxReporterFunc represents a configuration function for InfluxReporter.
type InfluxReporterFunc func(c *InfluxReporter)

// Database configures the database on an InfluxReporter.
func Database(db string) InfluxReporterFunc {
	return func(c *InfluxReporter) {
		c.database = db
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

// Policy configures the retention policy name on an InfluxReporter.
func Policy(policy string) InfluxReporterFunc {
	return func(c *InfluxReporter) {
		c.policy = policy
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
	database string

	metric string
	policy string
	tags   map[string]string

	client client.Client

	log log15.Logger
}

// NewInfluxReporter creates and returns a new NewInfluxReporter.
func NewInfluxReporter(client client.Client, opts ...InfluxReporterFunc) *InfluxReporter {
	r := &InfluxReporter{
		client: client,
	}

	for _, o := range opts {
		o(r)
	}

	return r
}

// ReportBrokerOffsets reports a snapshot of the broker offsets.
func (r InfluxReporter) ReportBrokerOffsets(o *store.BrokerOffsets) {
	pts, _ := client.NewBatchPoints(client.BatchPointsConfig{
		Database:        r.database,
		Precision:       "s",
		RetentionPolicy: r.policy,
	})

	for topic, partitions := range *o {
		for partition, offset := range partitions {
			if offset == nil {
				continue
			}

			tags := map[string]string{
				"type":      "BrokerOffset",
				"topic":     topic,
				"partition": fmt.Sprint(partition),
			}

			for key, value := range r.tags {
				tags[key] = value
			}

			pt, _ := client.NewPoint(
				r.metric,
				tags,
				map[string]interface{}{
					"oldest":    offset.OldestOffset,
					"newest":    offset.NewestOffset,
					"available": offset.NewestOffset - offset.OldestOffset,
				},
				time.Now(),
			)

			pts.AddPoint(pt)
		}
	}

	if err := r.client.Write(pts); err != nil {
		r.log.Error("influx: offsets:" + err.Error())
	}
}

// ReportBrokerMetadata reports a snapshot of the broker metadata.
func (r InfluxReporter) ReportBrokerMetadata(m *store.BrokerMetadata) {
	pts, _ := client.NewBatchPoints(client.BatchPointsConfig{
		Database:        r.database,
		Precision:       "s",
		RetentionPolicy: r.policy,
	})

	for topic, partitions := range *m {
		for partition, metadata := range partitions {
			if metadata == nil {
				continue
			}

			tags := map[string]string{
				"type":      "BrokerMetadata",
				"topic":     topic,
				"partition": fmt.Sprint(partition),
			}

			for key, value := range r.tags {
				tags[key] = value
			}

			leaders := 1
			if metadata.Leader < 0 {
				leaders = 0
			}
			pt, _ := client.NewPoint(
				r.metric,
				tags,
				map[string]interface{}{
					"leaders":  leaders,
					"replicas": len(metadata.Replicas),
					"isr":      len(metadata.Isr),
					"isr_diff": math.Abs(float64(len(metadata.Isr) - len(metadata.Replicas))),
				},
				time.Now(),
			)

			pts.AddPoint(pt)
		}
	}

	if err := r.client.Write(pts); err != nil {
		r.log.Error("influx: metadata:" + err.Error())
	}
}

// ReportConsumerOffsets reports a snapshot of the consumer group offsets.
func (r InfluxReporter) ReportConsumerOffsets(o *store.ConsumerOffsets) {
	pts, _ := client.NewBatchPoints(client.BatchPointsConfig{
		Database:        r.database,
		Precision:       "s",
		RetentionPolicy: r.policy,
	})

	for group, topics := range *o {
		for topic, partitions := range topics {
			for partition, offset := range partitions {
				if offset == nil {
					continue
				}

				tags := map[string]string{
					"type":      "ConsumerOffset",
					"group":     group,
					"topic":     topic,
					"partition": fmt.Sprint(partition),
				}

				for key, value := range r.tags {
					tags[key] = value
				}

				pt, _ := client.NewPoint(
					r.metric,
					tags,
					map[string]interface{}{
						"offset": offset.Offset,
						"lag":    offset.Lag,
					},
					time.Now(),
				)

				pts.AddPoint(pt)
			}
		}
	}

	if err := r.client.Write(pts); err != nil {
		r.log.Error("influx: consumer-offsets:" + err.Error())
	}
}

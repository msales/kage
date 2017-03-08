package influx

import (
	"time"

	"github.com/influxdata/influxdb/client/v2"
)

type Point struct {
	Measurement string
	Tags        map[string]string
	Fields      map[string]interface{}
	Time        time.Time
}

type Client struct {
	addr     string
	username string
	password string
	database string

	tags   map[string]string

	client client.Client
}

func New(opts ...ClientFunc) (*Client, error) {
	c := &Client{}

	for _, o := range opts {
		o(c)
	}

	influx, err := client.NewHTTPClient(client.HTTPConfig{
		Addr:     c.addr,
		Username: c.username,
		Password: c.password,
	})
	if err != nil {
		return nil, err
	}
	c.client = influx

	return c, nil
}

func (c Client) Write(pts []*Point) error {
	batch, err := client.NewBatchPoints(client.BatchPointsConfig{
		Database:  c.database,
		Precision: "s",
	})
	if err != nil {
		return err
	}

	for _, pt := range pts {
		if pt.Tags == nil {
			pt.Tags = make(map[string]string)
		}

		for key, value := range c.tags {
			pt.Tags[key] = value
		}

		point, err := client.NewPoint(pt.Measurement, pt.Tags, pt.Fields, pt.Time)
		if err != nil {
			return err
		}

		batch.AddPoint(point)
	}

	if err := c.client.Write(batch); err != nil {
		return err
	}

	return nil
}

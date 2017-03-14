package mocks

import (
	"errors"
	"time"
	"github.com/influxdata/influxdb/client/v2"
)

type MockInfluxClient struct {
	Connected bool

	BatchPoints client.BatchPoints
}

func (c *MockInfluxClient) Ping(timeout time.Duration) (time.Duration, string, error) {
	if !c.Connected {
		return time.Millisecond, "", errors.New("Mock client set to disconnected")
	}

	return time.Millisecond, "", nil
}

func (c *MockInfluxClient) Write(bp client.BatchPoints) error {
	c.BatchPoints = bp

	return nil
}

func (c *MockInfluxClient) Query(q client.Query) (*client.Response, error) {
	return &client.Response{
		Err: "",
		Results: []client.Result{},
	}, nil
}

func (c *MockInfluxClient) Close() error {
	c.Connected = false

	return nil
}

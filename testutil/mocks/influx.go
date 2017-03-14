package mocks

import (
	"errors"
	"github.com/influxdata/influxdb/client/v2"
	"time"
)

// MockInfluxClient represents a mock InfluxDB client.
type MockInfluxClient struct {
	Connected bool

	BatchPoints client.BatchPoints
}

// Ping checks that status of cluster, and will always return 0 time and no
// error for UDP clients.
func (c *MockInfluxClient) Ping(timeout time.Duration) (time.Duration, string, error) {
	if !c.Connected {
		return time.Millisecond, "", errors.New("Mock client set to disconnected")
	}

	return time.Millisecond, "", nil
}

// Write takes a BatchPoints object and writes all Points to InfluxDB.
func (c *MockInfluxClient) Write(bp client.BatchPoints) error {
	c.BatchPoints = bp

	return nil
}

// Query makes an InfluxDB Query on the database. This will fail if using
// the UDP client.
func (c *MockInfluxClient) Query(q client.Query) (*client.Response, error) {
	return &client.Response{
		Err:     "",
		Results: []client.Result{},
	}, nil
}

// Close releases any resources a Client may be using.
func (c *MockInfluxClient) Close() error {
	c.Connected = false

	return nil
}

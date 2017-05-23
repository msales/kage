package mocks

import (
	"time"

	"github.com/influxdata/influxdb/client/v2"
	"github.com/stretchr/testify/mock"
)

// MockInfluxClient represents a mock InfluxDB client.
type MockInfluxClient struct {
	mock.Mock
}

// Ping checks that status of cluster, and will always return 0 time and no
// error for UDP clients.
func (m *MockInfluxClient) Ping(timeout time.Duration) (time.Duration, string, error) {
	args := m.Called(timeout)
	return args.Get(0).(time.Duration), args.String(1), args.Error(2)
}

// Write takes a BatchPoints object and writes all Points to InfluxDB.
func (m *MockInfluxClient) Write(bp client.BatchPoints) error {
	args := m.Called(bp)
	return args.Error(0)
}

// Query makes an InfluxDB Query on the database. This will fail if using
// the UDP client.
func (m *MockInfluxClient) Query(q client.Query) (*client.Response, error) {
	args := m.Called(q)
	return args.Get(0).(*client.Response), args.Error(1)
}

// Close releases any resources a Client may be using.
func (c *MockInfluxClient) Close() error {
	return nil
}

package influx

import (
	"testing"
	"time"

	"github.com/influxdata/influxdb/client/v2"
)

func TestClient_WriteInvalidPoint(t *testing.T) {
	fake := &FakeInfluxClient{}
	c := &Client{client: fake}

	pts := make([]*Point, 1)
	pts[0] = &Point{}

	if err := c.Write(pts); err == nil {
		t.Error("expected point error, got none")
	}
}

func TestClient_Write(t *testing.T) {
	fake := &FakeInfluxClient{}
	c := &Client{client: fake, tags: map[string]string{"blah": "blah"}}

	pts := make([]*Point, 2)
	pts[0] = &Point{
		Measurement: "foo",
		Tags:        map[string]string{"test": "test"},
		Fields:      map[string]interface{}{"test": 1},
		Time:        time.Now(),
	}
	pts[1] = &Point{Measurement: "bar", Fields: map[string]interface{}{"value": 1}, Time: time.Now()}

	if err := c.Write(pts); err != nil {
		t.Error(err)
		return
	}

	if len(fake.BP.Points()) != 2 {
		t.Errorf("excpected points length %d; got %d", 2, len(fake.BP.Points()))
		return
	}

	if len(fake.BP.Points()[0].Tags()) != 2 {
		t.Errorf("excpected tags length %d; got %d", 2, len(fake.BP.Points()[0].Tags()))
		return
	}
}

type FakeInfluxClient struct {
	BP client.BatchPoints
}

func (f *FakeInfluxClient) Ping(timeout time.Duration) (time.Duration, string, error) {
	return time.Second, "", nil
}

func (f *FakeInfluxClient) Write(bp client.BatchPoints) error {
	f.BP = bp

	return nil
}

func (f *FakeInfluxClient) Query(q client.Query) (*client.Response, error) {
	return nil, nil
}

func (f *FakeInfluxClient) Close() error {
	return nil
}

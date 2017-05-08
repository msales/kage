package kafka

import (
	"reflect"
	"testing"

	"github.com/msales/kage"
	"gopkg.in/inconshreveable/log15.v2"
)

func TestBrokers(t *testing.T) {
	brokers := []string{"127.0.0.1"}
	c := &Client{}

	Brokers(brokers)(c)

	if !reflect.DeepEqual(c.brokers, brokers) {
		t.Fatal("expected brokers not found")
	}
}

func TestIgnoreGroups(t *testing.T) {
	i := []string{"test"}
	c := &Client{}

	IgnoreGroups(i)(c)

	if !reflect.DeepEqual(c.ignoreGroups, i) {
		t.Fatal("expected ignore groups not found")
	}
}

func TestIgnoreTopics(t *testing.T) {
	i := []string{"test"}
	c := &Client{}

	IgnoreTopics(i)(c)

	if !reflect.DeepEqual(c.ignoreTopics, i) {
		t.Fatal("expected ignore topics not found")
	}
}

func TestLog(t *testing.T) {
	log := log15.New()
	c := &Client{}

	Log(log)(c)

	if c.log != log {
		t.Fatal("expected log not found")
	}
}

func TestOffsetChannel(t *testing.T) {
	ch := make(chan *kage.PartitionOffset)
	c := &Client{}

	OffsetChannel(ch)(c)

	if c.offsetCh != ch {
		t.Fatal("expected offset channel not found")
	}
}

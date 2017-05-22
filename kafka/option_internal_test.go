package kafka

import (
	"testing"

	"github.com/msales/kage"
	"github.com/stretchr/testify/assert"
	"gopkg.in/inconshreveable/log15.v2"
)

func TestBrokers(t *testing.T) {
	brokers := []string{"127.0.0.1"}
	c := &Client{}

	Brokers(brokers)(c)

	assert.Equal(t, brokers, c.brokers)
}

func TestIgnoreGroups(t *testing.T) {
	i := []string{"test"}
	c := &Client{}

	IgnoreGroups(i)(c)

	assert.Equal(t, i, c.ignoreGroups)
}

func TestIgnoreTopics(t *testing.T) {
	i := []string{"test"}
	c := &Client{}

	IgnoreTopics(i)(c)

	assert.Equal(t, i, c.ignoreTopics)
}

func TestLog(t *testing.T) {
	log := log15.New()
	c := &Client{}

	Log(log)(c)

	assert.Equal(t, log, c.log)
}

func TestOffsetChannel(t *testing.T) {
	ch := make(chan *kage.PartitionOffset)
	c := &Client{}

	OffsetChannel(ch)(c)

	assert.Equal(t, ch, c.offsetCh)
}

package kafka

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"gopkg.in/inconshreveable/log15.v2"
)

func TestBrokers(t *testing.T) {
	brokers := []string{"127.0.0.1"}
	c := &Monitor{}

	Brokers(brokers)(c)

	assert.Equal(t, brokers, c.brokers)
}

func TestIgnoreGroups(t *testing.T) {
	i := []string{"test"}
	c := &Monitor{}

	IgnoreGroups(i)(c)

	assert.Equal(t, i, c.ignoreGroups)
}

func TestIgnoreTopics(t *testing.T) {
	i := []string{"test"}
	c := &Monitor{}

	IgnoreTopics(i)(c)

	assert.Equal(t, i, c.ignoreTopics)
}

func TestLog(t *testing.T) {
	log := log15.New()
	c := &Monitor{}

	Log(log)(c)

	assert.Equal(t, log, c.log)
}

func TestOffsetChannel(t *testing.T) {
	ch := make(chan interface{})
	c := &Monitor{}

	StateChannel(ch)(c)

	assert.Equal(t, ch, c.stateCh)
}

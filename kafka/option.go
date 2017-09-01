package kafka

import (
	"gopkg.in/inconshreveable/log15.v2"
)

// MonitorFunc represents a function that configures the Monitor.
type MonitorFunc func(c *Monitor)

// Log configures the logger on the Monitor.
func Log(log log15.Logger) MonitorFunc {
	return func(c *Monitor) {
		c.log = log
	}
}

// Brokers configures the brokers on the Monitor.
func Brokers(brokers []string) MonitorFunc {
	return func(c *Monitor) {
		c.brokers = brokers
	}
}

// IgnoreTopics configures the topic patterns to be ignored on the Monitor.
func IgnoreTopics(topics []string) MonitorFunc {
	return func(c *Monitor) {
		c.ignoreTopics = topics
	}
}

// IgnoreGroups configures the group patterns to be ignored on the Monitor.
func IgnoreGroups(groups []string) MonitorFunc {
	return func(c *Monitor) {
		c.ignoreGroups = groups
	}
}

// StateChannel configures the offset manager channel on the Monitor.
func StateChannel(ch chan interface{}) MonitorFunc {
	return func(c *Monitor) {
		c.stateCh = ch
	}
}

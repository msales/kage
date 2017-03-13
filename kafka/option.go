package kafka

import (
	"github.com/inconshreveable/log15"
	"github.com/msales/kage/kage"
)

type ClientFunc func(c *Client)

// Log sets the logger on the Client.
func Log(log log15.Logger) ClientFunc {
	return func(c *Client) {
		c.log = log
	}
}

// Brokers set the brokers on the Client.
func Brokers(brokers []string) ClientFunc {
	return func(c *Client) {
		c.brokers = brokers
	}
}

// IgnoreTopics sets the topic patterns to be ignored on the Client.
func IgnoreTopics(topics []string) ClientFunc {
	return func(c *Client) {
		c.ignoreTopics = topics
	}
}

// IgnoreGroups sets the group patterns to be ignored on the Client.
func IgnoreGroups(groups []string) ClientFunc {
	return func(c *Client) {
		c.ignoreGroups = groups
	}
}

// OffsetChannel set the offset manager channel on the Client.
func OffsetChannel(ch chan *kage.PartitionOffset) ClientFunc {
	return func(c *Client) {
		c.offsetCh = ch
	}
}

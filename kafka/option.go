package kafka

import (
	"github.com/inconshreveable/log15"
	"github.com/msales/kage/kage"
)

type ClientFunc func(c *Client)

func Log(log log15.Logger) ClientFunc {
	return func(c *Client) {
		c.log = log
	}
}

func Brokers(brokers []string) ClientFunc {
	return func(c *Client) {
		c.brokers = brokers
	}
}

func IgnoreTopics(topics []string) ClientFunc {
	return func(c *Client) {
		c.ignoreTopics = topics
	}
}

func IgnoreGroups(groups []string) ClientFunc {
	return func(c *Client) {
		c.ignoreGroups = groups
	}
}

func OffsetChannel(ch chan *kage.PartitionOffset) ClientFunc {
	return func(c *Client) {
		c.offsetCh = ch
	}
}

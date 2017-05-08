package testutil

import "gopkg.in/inconshreveable/log15.v2"

var Logger = log15.New()

func init() {
	Logger.SetHandler(log15.DiscardHandler())
}

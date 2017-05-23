package testutil

import "gopkg.in/inconshreveable/log15.v2"

// Logger is a common discard logger for testing.
var Logger = log15.New()

func init() {
	Logger.SetHandler(log15.DiscardHandler())
}

package kage

import (
	"gopkg.in/inconshreveable/log15.v2"
)

// Application represents the kage application.
type Application struct {
	Store     Store
	Reporters *Reporters
	Kafka     Kafka

	Logger log15.Logger
}

// NewApplication creates an instance of Application.
func NewApplication() *Application {
	return &Application{}
}

// Close gracefully shuts down the application
func (a *Application) Close() {
	if a.Store != nil {
		a.Store.Close()
	}

	if a.Kafka != nil {
		a.Kafka.Close()
	}
}

// Report reports the current state of the MemoryStore to the Reporters
func (a *Application) Report() {
	bo := a.Store.BrokerOffsets()
	a.Reporters.ReportBrokerOffsets(&bo)

	co := a.Store.ConsumerOffsets()
	a.Reporters.ReportConsumerOffsets(&co)
}

// IsHealthy checks the health of the Application.
func (a *Application) IsHealthy() bool {
	if a.Kafka == nil {
		return false
	}

	return a.Kafka.IsHealthy()
}

package main

import (
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/msales/kage"
	"github.com/msales/kage/server"
	"gopkg.in/alecthomas/kingpin.v2"
)

func main() {
	// Config
	config, err := readConfig(os.Args[1:])
	if err != nil {
		kingpin.Fatalf("Error reading configuration: %s", err.Error())
	}

	// Application
	app, err := newApplication(config)
	if err != nil {
		kingpin.Fatalf(err.Error())
	}
	defer app.Close()

	// Monitor state
	monitorTicker := time.NewTicker(30 * time.Second)
	defer monitorTicker.Stop()
	go func() {
		for range monitorTicker.C {
			app.Collect()
		}
	}()

	// Report state
	reportTicker := time.NewTicker(60 * time.Second)
	defer reportTicker.Stop()
	go func() {
		for range reportTicker.C {
			app.Report()
		}
	}()

	// Server
	if config.Server.Address != "" {
		ln, err := runServer(app, config.Server.Address)
		if err != nil {
			kingpin.Fatalf(err.Error())
		}
		defer ln.Close()
	}

	// Wait for quit
	quit := listenForSignals()
	<-quit
}

// runServer creates and starts the http server.
func runServer(app *kage.Application, addr string) (*net.TCPListener, error) {
	bind, err := net.ResolveTCPAddr("tcp", addr)
	if err != nil {
		panic(err)
	}

	ln, err := net.ListenTCP("tcp", bind)
	if err != nil {
		return nil, err
	}

	srv := server.New(app)

	go func() {
		if err := http.Serve(ln, srv); err != nil {
			app.Logger.Crit(err.Error())
		}
	}()

	return ln, nil
}

// Wait for SIGTERM to end the application.
func listenForSignals() chan bool {
	sigs := make(chan os.Signal, 1)
	done := make(chan bool, 1)

	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigs

		done <- true
	}()

	return done
}

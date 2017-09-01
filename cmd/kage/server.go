package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/msales/kage"
	"github.com/msales/kage/server"
	"gopkg.in/urfave/cli.v1"
)

func runServer(c *cli.Context) {
	app, err := newApplication(c)
	if err != nil {
		log.Fatal(err)
	}
	defer app.Close()

	monitorTicker := time.NewTicker(30 * time.Second)
	defer monitorTicker.Stop()
	go func() {
		for range monitorTicker.C {
			app.Collect()
		}
	}()

	reportTicker := time.NewTicker(60 * time.Second)
	defer reportTicker.Stop()
	go func() {
		for range reportTicker.C {
			app.Report()
		}
	}()

	if c.Bool(FlagServer) {
		port := c.String(FlagPort)
		srv := newServer(app)
		h := http.Server{Addr: ":" + port, Handler: srv}
		defer func() {
			h.Shutdown(context.Background())
		}()
		go func() {
			log.Printf("Starting on port %s.\n", port)
			if err := h.ListenAndServe(); err != nil {
				if err != http.ErrServerClosed {
					log.Fatal(err)
				}
			}
		}()
	}

	<-catchOsSignals()
}

func newServer(app *kage.Application) http.Handler {
	return server.New(app)
}

// Wait for SIGTERM to end the application.
func catchOsSignals() chan bool {
	sigs := make(chan os.Signal, 1)
	done := make(chan bool, 1)

	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigs

		done <- true
	}()

	return done
}

package api

import (
	"context"
	"net/http"

	"github.com/ONSdigital/go-ns/server"
	"github.com/ONSdigital/log.go/log"
	"github.com/gorilla/mux"
)

var httpServer *server.Server

// StartHealthCheck sets-up routes, listener
func StartHealthCheck(bindAddr string, serverDone chan error) {
	router := mux.NewRouter()
	router.Path("/healthcheck").HandlerFunc(healthCheck)

	httpServer = server.New(bindAddr, router)
	httpServer.HandleOSSignals = false

	go func() {
		if err := httpServer.ListenAndServe(); err != nil {
			log.Event(context.Background(), "", log.ERROR, log.Error(err))
			serverDone <- err
		}
		close(serverDone)
	}()
}

// StopHealthCheck shuts down the http listener
func StopHealthCheck(ctx context.Context) error {
	return httpServer.Shutdown(ctx)
}

// healthCheck returns the health of the application
func healthCheck(w http.ResponseWriter, r *http.Request) {
	log.Event(context.Background(), "Healthcheck endpoint.", log.INFO)
	// TODO future story for implementing healthcheck endpoint properly
	w.WriteHeader(http.StatusOK)
}

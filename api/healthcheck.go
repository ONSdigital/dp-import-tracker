package api

import (
	"context"
	"net/http"

	"github.com/ONSdigital/go-ns/log"
	"github.com/ONSdigital/go-ns/server"
	"github.com/gorilla/mux"
)

var httpServer *server.Server

func StartHealthCheck(bindAddr string, serverDone chan error) {
	router := mux.NewRouter()
	router.Path("/healthcheck").HandlerFunc(healthCheck)

	httpServer = server.New(bindAddr, router)
	httpServer.HandleOSSignals = false

	go func() {
		if err := httpServer.ListenAndServe(); err != nil {
			log.Error(err, nil)
			serverDone <- err
		}
		close(serverDone)
	}()
}

func StopHealthCheck(ctx context.Context) {
	httpServer.Shutdown(ctx)
}

// HealthCheck returns the health of the application
func healthCheck(w http.ResponseWriter, r *http.Request) {
	log.Debug("Healthcheck endpoint.", nil)
	// TODO future story for implementing healthcheck endpoint properly
	w.WriteHeader(http.StatusOK)
}

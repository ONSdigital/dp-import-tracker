package api

import (
	"context"
	"errors"

	"github.com/ONSdigital/dp-graph/v2/graph"
	"github.com/ONSdigital/dp-healthcheck/healthcheck"
	kafka "github.com/ONSdigital/dp-kafka/v2"
	dphttp "github.com/ONSdigital/dp-net/http"
	"github.com/ONSdigital/log.go/log"
	"github.com/gorilla/mux"
)

var httpServer *dphttp.Server

// StartHealthCheck sets up the Handler, starts the healthcheck and the http server that serves healthcheck endpoint
func StartHealthCheck(ctx context.Context, hc *healthcheck.HealthCheck, bindAddr string, serverDone chan error) {

	router := mux.NewRouter()
	router.Path("/health").HandlerFunc(hc.Handler)
	hc.Start(ctx)

	httpServer = dphttp.NewServer(bindAddr, router)
	httpServer.HandleOSSignals = false

	go func() {
		if err := httpServer.ListenAndServe(); err != nil {
			log.Event(ctx, "httpServer error", log.ERROR, log.Error(err))
			serverDone <- err
		}
		close(serverDone)
	}()
}

// StopHealthCheck shuts down the http listener
func StopHealthCheck(ctx context.Context, hc *healthcheck.HealthCheck) (err error) {
	err = httpServer.Shutdown(ctx)
	hc.Stop()
	return
}

// RegisterCheckers adds the checkers for the provided clients to the healthcheck object.
// VaultClient health client will only be registered if encryption is enabled.
func RegisterCheckers(ctx context.Context, hc *healthcheck.HealthCheck,
	newInstanceEventConsumer *kafka.ConsumerGroup,
	observationsInsertedEventConsumer *kafka.ConsumerGroup,
	hierarchyBuiltConsumer *kafka.ConsumerGroup,
	searchBuiltConsumer *kafka.ConsumerGroup,
	dataImportCompleteProducer *kafka.Producer,
	importAPI ImportAPIClient,
	datasetAPI DatasetClient,
	graphDB *graph.DB) (err error) {

	hasErrors := false

	if err = hc.AddCheck("Kafka NewInstanceEvent Consumer", newInstanceEventConsumer.Checker); err != nil {
		hasErrors = true
		log.Event(ctx, "Error Adding Check for Kafka New Instance Event Consumer Checker", log.ERROR, log.Error(err))
	}

	if err = hc.AddCheck("Kafka ObservationsInsertedEvent Consumer", observationsInsertedEventConsumer.Checker); err != nil {
		hasErrors = true
		log.Event(ctx, "Error Adding Check for Kafka Observations Inserted Event Consumer Checker", log.ERROR, log.Error(err))
	}

	if err = hc.AddCheck("Kafka HierarchyBuilt Consumer", hierarchyBuiltConsumer.Checker); err != nil {
		hasErrors = true
		log.Event(ctx, "Error Adding Check for Kafka Hierarchy Built Consumer Checker", log.ERROR, log.Error(err))
	}

	if err = hc.AddCheck("Kafka SearchBuilt Consumer", searchBuiltConsumer.Checker); err != nil {
		hasErrors = true
		log.Event(ctx, "Error Adding Search Built Consumer Checker", log.ERROR, log.Error(err))
	}

	if err = hc.AddCheck("Kafka DataImportComplete Producer", dataImportCompleteProducer.Checker); err != nil {
		hasErrors = true
		log.Event(ctx, "Error Adding Data Import Complete Producer Checker", log.ERROR, log.Error(err))
	}

	if err = hc.AddCheck("importAPI", importAPI.Checker); err != nil {
		hasErrors = true
		log.Event(ctx, "Error Adding importAPI Checker", log.ERROR, log.Error(err))
	}

	if err = hc.AddCheck("datasetAPI", datasetAPI.Checker); err != nil {
		hasErrors = true
		log.Event(ctx, "Error Adding datasetAPI Checker", log.ERROR, log.Error(err))
	}

	if err = hc.AddCheck("Graph DB", graphDB.Checker); err != nil {
		hasErrors = true
		log.Event(ctx, "Error Adding Graph DB Checker", log.ERROR, log.Error(err))
	}

	if hasErrors {
		return errors.New("Error(s) registering checkers for healthcheck")
	}
	return nil
}

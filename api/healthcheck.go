package api

import (
	"context"

	"github.com/ONSdigital/dp-healthcheck/healthcheck"
	kafka "github.com/ONSdigital/dp-kafka"

	"github.com/ONSdigital/go-ns/server"
	"github.com/ONSdigital/log.go/log"
	"github.com/gorilla/mux"
)

var httpServer *server.Server

// StartHealthCheck sets up the Handler, starts the healthcheck and the http server that serves healthcheck endpoint
func StartHealthCheck(ctx context.Context, hc *healthcheck.HealthCheck, bindAddr string, serverDone chan error) {

	router := mux.NewRouter()
	router.Path("/health").HandlerFunc(hc.Handler)
	hc.Start(ctx)

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
func StopHealthCheck(ctx context.Context, hc *healthcheck.HealthCheck) (err error) {
	err = httpServer.Shutdown(ctx)
	hc.Stop()
	return
}

// RegisterCheckers adds the checkers for the provided clients to the healthcheck object.
// VaultClient health client will only be registered if encryption is enabled.
func RegisterCheckers(hc *healthcheck.HealthCheck,
	newInstanceEventConsumer *kafka.ConsumerGroup,
	observationsInsertedEventConsumer *kafka.ConsumerGroup,
	hierarchyBuiltConsumer *kafka.ConsumerGroup,
	searchBuiltConsumer *kafka.ConsumerGroup,
	dataImportCompleteProducer *kafka.Producer,
	importAPI ImportAPIClient,
	datasetAPI DatasetClient) (err error) {

	if err = hc.AddCheck("Kafka New Instance Event Consumer", newInstanceEventConsumer.Checker); err != nil {
		log.Event(nil, "Error Adding Check for Kafka New Instance Event Consumer Checker", log.ERROR, log.Error(err))
	}

	if err = hc.AddCheck("Kafka Observations Inserted Event Consumer", observationsInsertedEventConsumer.Checker); err != nil {
		log.Event(nil, "Error Adding Check for Kafka Observations Inserted Event Consumer Checker", log.ERROR, log.Error(err))
	}

	if err = hc.AddCheck("Kafka Hierarchy Built Consumer", hierarchyBuiltConsumer.Checker); err != nil {
		log.Event(nil, "Error Adding Check for Kafka Hierarchy Built Consumer Checker", log.ERROR, log.Error(err))
	}

	if err = hc.AddCheck("Kafka Search Built Consumer", searchBuiltConsumer.Checker); err != nil {
		log.Event(nil, "Error Adding Search Built Consumer Checker", log.ERROR, log.Error(err))
	}

	if err = hc.AddCheck("Kafka Data Import Complete Producer", dataImportCompleteProducer.Checker); err != nil {
		log.Event(nil, "Error Adding Data Import Complete Producer Checker", log.ERROR, log.Error(err))
	}

	if err = hc.AddCheck("importAPI", importAPI.Checker); err != nil {
		log.Event(nil, "Error Adding importAPI Checker", log.ERROR, log.Error(err))
	}

	if err = hc.AddCheck("datasetAPI", datasetAPI.Checker); err != nil {
		log.Event(nil, "Error Adding datasetAPI Checker", log.ERROR, log.Error(err))
	}

	return
}

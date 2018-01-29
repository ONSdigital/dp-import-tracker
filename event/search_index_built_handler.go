package event

import (
	"context"
	"github.com/ONSdigital/dp-import-tracker/instance"
	"github.com/ONSdigital/dp-import/events"
	"github.com/ONSdigital/dp-reporter-client/reporter"
	"github.com/ONSdigital/go-ns/kafka"
	"github.com/ONSdigital/go-ns/log"
)

// SearchIndexBuiltHandler handles a search index built Kafka message
type SearchIndexBuiltHandler struct {
	context       context.Context
	instanceStore instance.Store
	errorReporter reporter.ErrorReporter
}

// NewSearchIndexBuiltHandler returns a new instance of SearchIndexBuiltHandler
func NewSearchIndexBuiltHandler(ctx context.Context, instanceStore instance.Store, errorReporter reporter.ErrorReporter) *SearchIndexBuiltHandler {
	return &SearchIndexBuiltHandler{
		context:       ctx,
		instanceStore: instanceStore,
		errorReporter: errorReporter,
	}
}

// Handle a single Kafka message
func (handler *SearchIndexBuiltHandler) Handle(message kafka.Message) {

	var event events.SearchIndexBuilt

	err := events.SearchIndexBuiltSchema.Unmarshal(message.GetData(), &event)
	if err != nil {
		log.ErrorC("failed to unmarshal event", err, nil)
		message.Commit()
		return
	}

	logData := log.Data{"instance_id": event.InstanceID, "dimension_name": event.DimensionName}
	log.Debug("processing search index built event message", logData)

	isFatal, err := handler.instanceStore.UpdateInstanceWithSearchIndexBuilt(
		handler.context,
		event.InstanceID,
		event.DimensionName,
	)

	if err != nil {
		logData["isFatal"] = isFatal
		handler.errorReporter.Notify(event.InstanceID, "failed to update instance with search index built status", err)
		log.ErrorC("failed to update instance with search index built status", err, logData)
	}

	log.Debug("updated instance with search index built. committing message", logData)
	message.Commit()
	log.Debug("message committed", logData)
}

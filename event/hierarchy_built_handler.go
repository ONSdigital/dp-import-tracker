package event

import (
	"context"
	"github.com/ONSdigital/dp-import-tracker/instance"
	"github.com/ONSdigital/dp-import/events"
	"github.com/ONSdigital/dp-reporter-client/reporter"
	"github.com/ONSdigital/go-ns/kafka"
	"github.com/ONSdigital/go-ns/log"
)

// HierarchyBuiltHandler handles a hierarchy built Kafka message
type HierarchyBuiltHandler struct {
	context       context.Context
	instanceStore instance.Store
	errorReporter reporter.ErrorReporter
}

// NewHierarchyBuiltHandler returns a new instance of HierarchyBuiltHandler
func NewHierarchyBuiltHandler(ctx context.Context, instanceStore instance.Store, errorReporter reporter.ErrorReporter) *HierarchyBuiltHandler {
	return &HierarchyBuiltHandler{
		context:       ctx,
		instanceStore: instanceStore,
		errorReporter: errorReporter,
	}
}

// Handle a single Kafka message
func (handler *HierarchyBuiltHandler) Handle(message kafka.Message) {

	var event events.HierarchyBuilt

	err := events.HierarchyBuiltSchema.Unmarshal(message.GetData(), &event)
	if err != nil {
		log.ErrorC("failed to unmarshal event", err, nil)
		message.Commit()
		return
	}

	logData := log.Data{"instance_id": event.InstanceID, "dimension_name": event.DimensionName}
	log.Debug("processing hierarchy built event message", logData)

	isFatal, err := handler.instanceStore.UpdateInstanceWithHierarchyBuilt(
		handler.context,
		event.InstanceID,
		event.DimensionName,
	)

	if err != nil {
		logData["isFatal"] = isFatal
		handler.errorReporter.Notify(event.InstanceID, "failed to update instance with hierarchy built status", err)
		log.ErrorC("failed to update instance with hierarchy built status", err, logData)
	}

	log.Debug("updated instance with hierarchy built. committing message", logData)
	message.Commit()
	log.Debug("message committed", logData)
}

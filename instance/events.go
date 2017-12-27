package instance

// ObservationImportCompleteEvent is used to notify that all observations of an instance have been imported.
type ObservationImportCompleteEvent struct {
	InstanceID string `avro:"instance_id"`
}
package instance

// ObservationImportCompleteEvent is used to notify that all observations of an instance have been imported.
type ObservationImportCompleteEvent struct {
	InstanceID string `avro:"instance_id"`
}

// InputFileAvailableEvent is consumed by the import tracker when a new input file is available.
type InputFileAvailableEvent struct {
	FileURL    string `avro:"file_url"`
	InstanceID string `avro:"instance_id"`
}
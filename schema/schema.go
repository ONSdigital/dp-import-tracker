package schema

import "github.com/ONSdigital/go-ns/avro"

var inputFileAvailable = `{
  "type": "record",
  "name": "input-file-available",
  "fields": [
    {"name": "file_url", "type": "string"},
    {"name": "instance_id", "type": "string"}
  ]
}`

// InputFileAvailableSchema is the Avro schema for each
// input file that becomes available
var InputFileAvailableSchema *avro.Schema = &avro.Schema{
	Definition: inputFileAvailable,
}

// from dp-observation-importer
var observationsInsertedEvent = `{
  "type": "record",
  "name": "import-observations-inserted",
  "fields": [
    {"name": "instance_id", "type": "string"},
    {"name": "observations_inserted", "type": "int"}
  ]
}`

// ObservationsInsertedEvent is the Avro schema for each observation batch inserted.
var ObservationsInsertedEvent avro.Schema = avro.Schema{
	Definition: observationsInsertedEvent,
}

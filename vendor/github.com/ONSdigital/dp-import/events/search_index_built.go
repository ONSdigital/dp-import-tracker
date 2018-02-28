package events

import "github.com/ONSdigital/go-ns/avro"

// SearchIndexBuilt contains data related to a search index that has just been built.
type SearchIndexBuilt struct {
	DimensionName string `avro:"dimension_name"`
	InstanceID    string `avro:"instance_id"`
}

var searchIndexBuiltSchema = `{
  "type": "record",
  "name": "search-index-built",
  "fields": [
    {"name": "instance_id", "type": "string"},
    {"name": "dimension_name", "type": "string"}
  ]
}`

// SearchIndexBuiltSchema is the Avro schema for each dimension hierarchy successfuly sent to elastic
var SearchIndexBuiltSchema = &avro.Schema{
	Definition: searchIndexBuiltSchema,
}

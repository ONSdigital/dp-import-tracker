DP Import Tracker
=================

Check for completed import instances, and mark them (and possibly
their associated/parent import job) as completed.

* At startup, ask the Dataset API for the list of active instances.

* Add to the above in-memory list when `INPUT_FILE_AVAILABLE_TOPIC` events arrive.

* Also, listen for `IMPORT_OBSERVATIONS_INSERTED_TOPIC` events,
and inform the Dataset API with those additional numbers of inserted observations.

* Regularly check the [Dataset API](../dp-dataset-api) for the total number of observations,
and the current count of inserted observations.  When the total has been
inserted, mark the **status** of the instance as `completed` (and similar
for the parent import job, if all other instances are also `completed`).

`INPUT_FILE_AVAILABLE_TOPIC` events are published by the [Import API](../dp-import-api).

`IMPORT_OBSERVATIONS_INSERTED_TOPIC` events are published by the
[Observation Importer](../dp-observation-importer).

### Getting started

Service is authenticated against `zebedee`, one can run [dp-auth-api-stub](https://github.com/ONSdigital/dp-auth-api-stub) to mimic
service identity check in zebedee.
`make debug`

### Configuration

| Environment variable                        | Default                               | Description
| ------------------------------------------- | ------------------------------------- | -----------
| INPUT_FILE_AVAILABLE_TOPIC                  | input-file-available                  | topic name for import file available events
| INPUT_FILE_AVAILABLE_CONSUMER_GROUP         | dp-import-tracker                     | consumer group name for import file available events
| IMPORT_OBSERVATIONS_INSERTED_TOPIC          | import-observations-inserted          | topic name for numbers of inserted observations
| IMPORT_OBSERVATIONS_INSERTED_CONSUMER_GROUP | dp-import-tracker                     | consumer group name for numbers of inserted observations
| HIERARCHY_BUILT_TOPIC                       | hierarchy-built                       | topic name for built hierarchies
| HIERARCHY_BUILT_CONSUMER_GROUP              | dp-import-tracker                     | consumer group name for built hierarchies
| DATA_IMPORT_COMPLETE_TOPIC                  | data-import-complete                  | topic name for hierarchies ready to be imported
| KAFKA_ADDR                                  | localhost:9092                        | A list of kafka brokers
| IMPORT_API_ADDR                             | http://localhost:21800                | The address of Import API
| DATASET_API_ADDR                            | http://localhost:22000                | The address of Dataset API
| BIND_ADDR                                   | :21300                                | address to listen on for healthcheck requests
| GRACEFUL_SHUTDOWN_TIMEOUT                   | 5s                                    | how much grace time to allow when shutting down (time.duration)
| DATABASE_ADDRESS                            | bolt://localhost:7687                 | The address of the database
| SERVICE_AUTH_TOKEN                          | AB0A5CFA-3C55-4FA8-AACC-F98039BED0AC  | The service authorization token
| ZEBEDEE_URL                                 | http://localhost:8082                 | The host name for Zebedee
| CHECK_COMPLETE_INTERVAL                     | 2000ms                                | how much time between checking for instances

### Contributing

See [CONTRIBUTING](CONTRIBUTING.md) for details.

### License

Copyright © 2016-2017, Office for National Statistics (https://www.ons.gov.uk)

Released under MIT license, see [LICENSE](LICENSE.md) for details.

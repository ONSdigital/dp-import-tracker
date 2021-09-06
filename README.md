# dp-import-tracker

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

## Getting started

Service is authenticated against `zebedee`, one can run [dp-auth-api-stub](https://github.com/ONSdigital/dp-auth-api-stub) to mimic
service identity check in zebedee.
`make debug`

### Kafka scripts

Scripts for updating and debugging Kafka can be found [here](https://github.com/ONSdigital/dp-data-tools)(dp-data-tools)

### Configuration

| Environment variable                        | Default                               | Description
| ------------------------------------------- | ------------------------------------- | -----------
| INPUT_FILE_AVAILABLE_TOPIC                  | input-file-available                  | topic name for import file available events
| INPUT_FILE_AVAILABLE_CONSUMER_GROUP         | dp-import-tracker                     | consumer group name for import file available events
| IMPORT_OBSERVATIONS_INSERTED_TOPIC          | import-observations-inserted          | topic name for numbers of inserted observations
| IMPORT_OBSERVATIONS_INSERTED_CONSUMER_GROUP | dp-import-tracker                     | consumer group name for numbers of inserted observations
| HIERARCHY_BUILT_TOPIC                       | hierarchy-built                       | topic name for built hierarchies
| HIERARCHY_BUILT_CONSUMER_GROUP              | dp-import-tracker                     | consumer group name for built hierarchies
| SEARCH_BUILT_TOPIC                          | dimension-search-built                | topic name for search built
| SEARCH_BUILT_CONSUMER_GROUP                 | dp-import-tracker                     | consumer group name for search built
| DATA_IMPORT_COMPLETE_TOPIC                  | data-import-complete                  | topic name for hierarchies ready to be imported
| KAFKA_LEGACY_ADDR                           | `localhost:9092`                      | The addresses of the kafka brokers - non-TLS
| KAFKA_LEGACY_VERSION                        | `1.0.2`                               | The version of Kafka - non-TLS
| KAFKA_ADDR                                  | `localhost:9092`                      | A list of kafka brokers
| KAFKA_VERSION                               | `1.0.2`                               | The kafka version that this service expects to connect to
| KAFKA_SEC_PROTO                             | _unset_                               | if set to `TLS`, kafka connections will use TLS [[1]](#notes_1)
| KAFKA_SEC_CLIENT_KEY                        | _unset_                               | PEM for the client key [[1]](#notes_1)
| KAFKA_SEC_CLIENT_CERT                       | _unset_                               | PEM for the client certificate [[1]](#notes_1)
| KAFKA_SEC_CA_CERTS                          | _unset_                               | CA cert chain for the server cert [[1]](#notes_1)
| KAFKA_SEC_SKIP_VERIFY                       | false                                 | ignores server certificate issues if `true` [[1]](#notes_1)
| KAFKA_OFFSET_OLDEST                         | false                                 | sets kafka offset to oldest if `true`
| IMPORT_API_ADDR                             | http://localhost:21800                | The address of Import API
| DATASET_API_ADDR                            | http://localhost:22000                | The address of Dataset API
| BIND_ADDR                                   | :21300                                | address to listen on for healthcheck requests
| GRACEFUL_SHUTDOWN_TIMEOUT                   | 5s                                    | how much grace time to allow when shutting down (time.duration)
| SERVICE_AUTH_TOKEN                          | AB0A5CFA-3C55-4FA8-AACC-F98039BED0AC  | The service authorization token
| ZEBEDEE_URL                                 | http://localhost:8082                 | The host name for Zebedee
| CHECK_COMPLETE_INTERVAL                     | 2000ms                                | how much time between checking for instances
| INITIALISE_LIST_INTERVAL                    | 4s                                    | on startup, if getting instance list fails, how much time to wait before retries
| INITIALISE_LIST_ATTEMPTS                    | 20                                    | on startup, how many times to retry for instances, before exiting, see INITIALISE_LIST_INTERVAL
| HEALTHCHECK_INTERVAL                        | 30s                                   | The period of time between health checks
| HEALTHCHECK_CRITICAL_TIMEOUT                | 90s                                   | The period of time after which failing checks will result in critical global check

**Notes:**

1. <a name="notes_1">For more info, see the [kafka TLS examples documentation](https://github.com/ONSdigital/dp-kafka/tree/main/examples#tls)</a>

### Contributing

See [CONTRIBUTING](CONTRIBUTING.md) for details.

### License

Copyright © 2016-2021, Office for National Statistics (https://www.ons.gov.uk)

Released under MIT license, see [LICENSE](LICENSE.md) for details.

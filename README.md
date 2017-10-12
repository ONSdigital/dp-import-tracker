DP Import Tracker
====

Check for completed import instances, and mark them (and possibly
their associated/parent import job) as completed.

* At startup, ask the Dataset API for the list of active instances.

* Add to the above in-memory list when `INPUT_FILE_AVAILABLE_TOPIC` events arrive.

* Also, listen for `IMPORT_OBSERVATIONS_INSERTED_TOPIC` events,
and inform the Dataset API with those additional numbers of inserted observations.

* Regularly check the [Dataset API](../dp-import-api) for the total number of observations,
and the current count of inserted observations.  When the total has been
inserted, mark the **status** of the instance as `completed` (and similar
for the parent import job, if all other instances are also `completed`).

`INPUT_FILE_AVAILABLE_TOPIC` events are published by the [Databaker](../databaker).

`IMPORT_OBSERVATIONS_INSERTED_TOPIC` events are published by the
[Observation Importer](../dp-observation-importer).

### Getting started

`make debug`

### Configuration

| Environment variable                  | Default                                       | Description
| ------------------------------------- | --------------------------------------------- | -----------
| INPUT_FILE_AVAILABLE_TOPIC            | `input-file-available`                        | topic name for import file available events
| IMPORT_OBSERVATIONS_INSERTED_TOPIC    | `import-observations-inserted`                | topic name for numbers of inserted observations
| KAFKA_ADDR                            | `localhost:9092`                              | A list of kafka brokers
| IMPORT_API_ADDR                       | `http://localhost:21800`                      | The address of Import API
| IMPORT_API_AUTH_TOKEN                 | _no default_                                  | Authentication token for access to import API
| DATASET_API_ADDR                      | `http://localhost:22000`                      | The address of Dataset API
| DATASET_API_AUTH_TOKEN                | _no default_                                  | Authentication token for access to Dataset API
| DB_ACCESS                             | `user=dp dbname=ImportJobs sslmode=disable`   | URL for Postgresql
| BIND_ADDR                             | `:21300`                                      | address to listen on for healthcheck requests
| GRACEFUL_SHUTDOWN_TIMEOUT             | `10s`                                         | how much grace time to allow when shutting down

### Contributing

See [CONTRIBUTING](CONTRIBUTING.md) for details.

### License

Copyright © 2016-2017, Office for National Statistics (https://www.ons.gov.uk)

Released under MIT license, see [LICENSE](LICENSE.md) for details.

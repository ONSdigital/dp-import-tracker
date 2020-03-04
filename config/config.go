package config

import (
	"time"

	"encoding/json"

	"github.com/kelseyhightower/envconfig"
)

// Config represents the app configuration
type Config struct {
	NewInstanceTopic                  string        `envconfig:"INPUT_FILE_AVAILABLE_TOPIC"`
	NewInstanceConsumerGroup          string        `envconfig:"INPUT_FILE_AVAILABLE_CONSUMER_GROUP"`
	ObservationsInsertedTopic         string        `envconfig:"IMPORT_OBSERVATIONS_INSERTED_TOPIC"`
	ObservationsInsertedConsumerGroup string        `envconfig:"IMPORT_OBSERVATIONS_INSERTED_CONSUMER_GROUP"`
	HierarchyBuiltTopic               string        `envconfig:"HIERARCHY_BUILT_TOPIC"`
	HierarchyBuiltConsumerGroup       string        `envconfig:"HIERARCHY_BUILT_CONSUMER_GROUP"`
	SearchBuiltTopic                  string        `envconfig:"SEARCH_BUILT_TOPIC"`
	SearchBuiltConsumerGroup          string        `envconfig:"SEARCH_BUILT_CONSUMER_GROUP"`
	DataImportCompleteTopic           string        `envconfig:"DATA_IMPORT_COMPLETE_TOPIC"`
	Brokers                           []string      `envconfig:"KAFKA_ADDR"`
	ImportAPIAddr                     string        `envconfig:"IMPORT_API_ADDR"`
	DatasetAPIAddr                    string        `envconfig:"DATASET_API_ADDR"`
	ShutdownTimeout                   time.Duration `envconfig:"GRACEFUL_SHUTDOWN_TIMEOUT"`
	BindAddr                          string        `envconfig:"BIND_ADDR"`
	ServiceAuthToken                  string        `envconfig:"SERVICE_AUTH_TOKEN"                   json:"-"`
	CheckCompleteInterval             time.Duration `envconfig:"CHECK_COMPLETE_INTERVAL"`
	InitialiseListInterval            time.Duration `envconfig:"INITIALISE_LIST_INTERVAL"`
	InitialiseListAttempts            int           `envconfig:"INITIALISE_LIST_ATTEMPTS"`
	HealthCheckInterval               time.Duration `envconfig:"HEALTHCHECK_INTERVAL"`
	HealthCheckRecoveryInterval       time.Duration `envconfig:"HEALTHCHECK_RECOVERY_INTERVAL"`
}

// NewConfig creates the config object
func NewConfig() (*Config, error) {
	cfg := Config{
		BindAddr:                          ":21300",
		ServiceAuthToken:                  "AB0A5CFA-3C55-4FA8-AACC-F98039BED0AC",
		NewInstanceTopic:                  "input-file-available",
		NewInstanceConsumerGroup:          "dp-import-tracker",
		ObservationsInsertedTopic:         "import-observations-inserted",
		ObservationsInsertedConsumerGroup: "dp-import-tracker",
		HierarchyBuiltTopic:               "hierarchy-built",
		HierarchyBuiltConsumerGroup:       "dp-import-tracker",
		SearchBuiltTopic:                  "search-built",
		SearchBuiltConsumerGroup:          "dp-import-tracker",
		DataImportCompleteTopic:           "data-import-complete",
		Brokers:                           []string{"localhost:9092"},
		ImportAPIAddr:                     "http://localhost:21800",
		DatasetAPIAddr:                    "http://localhost:22000",
		ShutdownTimeout:                   5 * time.Second,
		CheckCompleteInterval:             2000 * time.Millisecond,
		InitialiseListInterval:            4 * time.Second,
		InitialiseListAttempts:            20,
		HealthCheckInterval:               10 * time.Second,
		HealthCheckRecoveryInterval:       1 * time.Minute,
	}
	if err := envconfig.Process("", &cfg); err != nil {
		return nil, err
	}

	cfg.ServiceAuthToken = "Bearer " + cfg.ServiceAuthToken

	return &cfg, nil
}

// String is implemented to prevent sensitive fields being logged.
// The config is returned as JSON with sensitive fields omitted.
func (config Config) String() string {
	json, _ := json.Marshal(config)
	return string(json)
}

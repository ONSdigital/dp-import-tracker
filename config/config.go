package config

import (
	"time"

	"github.com/kelseyhightower/envconfig"
)

// Config represents the app configuration
type Config struct {
	NewInstanceTopic          string        `envconfig:"INPUT_FILE_AVAILABLE_TOPIC"`
	ObservationsInsertedTopic string        `envconfig:"IMPORT_OBSERVATIONS_INSERTED_TOPIC"`
	Brokers                   []string      `envconfig:"KAFKA_ADDR"`
	ImportAPIAddr             string        `envconfig:"IMPORT_API_ADDR"`
	ImportAPIAuthToken        string        `envconfig:"IMPORT_API_AUTH_TOKEN"`
	DatasetAPIAddr            string        `envconfig:"DATASET_API_ADDR"`
	DatasetAPIAuthToken       string        `envconfig:"DATASET_API_AUTH_TOKEN"`
	ShutdownTimeout           time.Duration `envconfig:"GRACEFUL_SHUTDOWN_TIMEOUT"`
	BindAddr                  string        `envconfig:"BIND_ADDR"`
	DatabaseAddress           string        `envconfig:"DATABASE_ADDRESS"`
	DatabasePoolSize          int           `envconfig:"DATABASE_POOL_SIZE"`
}

// NewConfig creates the config object
func NewConfig() (*Config, error) {
	cfg := Config{
		BindAddr:                  ":21300",
		NewInstanceTopic:          "input-file-available",
		ObservationsInsertedTopic: "import-observations-inserted",
		Brokers:                   []string{"localhost:9092"},
		ImportAPIAddr:             "http://localhost:21800",
		ImportAPIAuthToken:        "FD0108EA-825D-411C-9B1D-41EF7727F465",
		DatasetAPIAddr:            "http://localhost:22000",
		ShutdownTimeout:           5 * time.Second,
		DatasetAPIAuthToken:       "FD0108EA-825D-411C-9B1D-41EF7727F465",
		DatabaseAddress:           "bolt://localhost:7687",
		DatabasePoolSize:          30,
	}
	if err := envconfig.Process("", &cfg); err != nil {
		return nil, err
	}

	return &cfg, nil
}

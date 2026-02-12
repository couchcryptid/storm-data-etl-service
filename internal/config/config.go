package config

import (
	"errors"

	"time"

	sharedcfg "github.com/couchcryptid/storm-data-shared/config"
)

// Config holds all service settings, populated from environment variables.
type Config struct {
	KafkaBrokers     []string
	KafkaSourceTopic string
	KafkaSinkTopic   string
	KafkaGroupID     string
	HTTPAddr         string
	LogLevel         string
	LogFormat        string
	ShutdownTimeout  time.Duration

	BatchSize          int
	BatchFlushInterval time.Duration
}

// Load reads configuration from environment variables, applying defaults where unset.
func Load() (*Config, error) {
	shutdownTimeout, err := sharedcfg.ParseShutdownTimeout()
	if err != nil {
		return nil, err
	}

	batchSize, err := sharedcfg.ParseBatchSize()
	if err != nil {
		return nil, err
	}

	flushInterval, err := sharedcfg.ParseBatchFlushInterval()
	if err != nil {
		return nil, err
	}

	cfg := &Config{
		KafkaBrokers:       sharedcfg.ParseBrokers(sharedcfg.EnvOrDefault("KAFKA_BROKERS", "localhost:9092")),
		KafkaSourceTopic:   sharedcfg.EnvOrDefault("KAFKA_SOURCE_TOPIC", "raw-weather-reports"),
		KafkaSinkTopic:     sharedcfg.EnvOrDefault("KAFKA_SINK_TOPIC", "transformed-weather-data"),
		KafkaGroupID:       sharedcfg.EnvOrDefault("KAFKA_GROUP_ID", "storm-data-etl"),
		HTTPAddr:           sharedcfg.EnvOrDefault("HTTP_ADDR", ":8080"),
		LogLevel:           sharedcfg.EnvOrDefault("LOG_LEVEL", "info"),
		LogFormat:          sharedcfg.EnvOrDefault("LOG_FORMAT", "json"),
		ShutdownTimeout:    shutdownTimeout,
		BatchSize:          batchSize,
		BatchFlushInterval: flushInterval,
	}

	if len(cfg.KafkaBrokers) == 0 {
		return nil, errors.New("KAFKA_BROKERS is required")
	}
	if cfg.KafkaSourceTopic == "" {
		return nil, errors.New("KAFKA_SOURCE_TOPIC is required")
	}
	if cfg.KafkaSinkTopic == "" {
		return nil, errors.New("KAFKA_SINK_TOPIC is required")
	}

	return cfg, nil
}

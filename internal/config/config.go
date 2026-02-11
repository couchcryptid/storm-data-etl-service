package config

import (
	"errors"
	"os"
	"strconv"
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

	// Mapbox geocoding configuration.
	MapboxToken     string
	MapboxEnabled   bool
	MapboxTimeout   time.Duration
	MapboxCacheSize int
}

// Load reads configuration from environment variables, applying defaults where unset.
func Load() (*Config, error) {
	shutdownTimeout, err := sharedcfg.ParseShutdownTimeout()
	if err != nil {
		return nil, err
	}

	mapboxTimeoutStr := sharedcfg.EnvOrDefault("MAPBOX_TIMEOUT", "5s")
	mapboxTimeout, err2 := time.ParseDuration(mapboxTimeoutStr)
	if err2 != nil || mapboxTimeout <= 0 {
		return nil, errors.New("invalid MAPBOX_TIMEOUT")
	}

	batchSize, err := sharedcfg.ParseBatchSize()
	if err != nil {
		return nil, err
	}

	flushInterval, err := sharedcfg.ParseBatchFlushInterval()
	if err != nil {
		return nil, err
	}

	mapboxCacheSize := parseMapboxCacheSize()

	mapboxToken := os.Getenv("MAPBOX_TOKEN")
	mapboxEnabled := mapboxToken != ""
	if v := os.Getenv("MAPBOX_ENABLED"); v != "" {
		mapboxEnabled = v == "true"
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

		MapboxToken:     mapboxToken,
		MapboxEnabled:   mapboxEnabled,
		MapboxTimeout:   mapboxTimeout,
		MapboxCacheSize: mapboxCacheSize,
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
	if cfg.MapboxEnabled && cfg.MapboxToken == "" {
		return nil, errors.New("MAPBOX_ENABLED is true but MAPBOX_TOKEN is not set")
	}

	return cfg, nil
}

func parseMapboxCacheSize() int {
	if s := os.Getenv("MAPBOX_CACHE_SIZE"); s != "" {
		if n, err := strconv.Atoi(s); err == nil && n > 0 {
			return n
		}
	}
	return 1000
}

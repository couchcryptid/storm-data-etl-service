package config

import (
	"errors"
	"os"
	"strconv"
	"strings"
	"time"
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

	// Mapbox geocoding configuration.
	MapboxToken     string
	MapboxEnabled   bool
	MapboxTimeout   time.Duration
	MapboxCacheSize int
}

// Load reads configuration from environment variables, applying defaults where unset.
func Load() (*Config, error) {
	shutdownStr := envOrDefault("SHUTDOWN_TIMEOUT", "10s")
	shutdownTimeout, err := time.ParseDuration(shutdownStr)
	if err != nil || shutdownTimeout <= 0 {
		return nil, errors.New("invalid SHUTDOWN_TIMEOUT")
	}

	mapboxTimeoutStr := envOrDefault("MAPBOX_TIMEOUT", "5s")
	mapboxTimeout, err2 := time.ParseDuration(mapboxTimeoutStr)
	if err2 != nil || mapboxTimeout <= 0 {
		return nil, errors.New("invalid MAPBOX_TIMEOUT")
	}

	mapboxCacheSize := 1000
	if s := os.Getenv("MAPBOX_CACHE_SIZE"); s != "" {
		if n, err3 := strconv.Atoi(s); err3 == nil && n > 0 {
			mapboxCacheSize = n
		}
	}

	mapboxToken := os.Getenv("MAPBOX_TOKEN")
	mapboxEnabled := mapboxToken != ""
	if v := os.Getenv("MAPBOX_ENABLED"); v != "" {
		mapboxEnabled = v == "true"
	}

	cfg := &Config{
		KafkaBrokers:     parseBrokers(envOrDefault("KAFKA_BROKERS", "localhost:9092")),
		KafkaSourceTopic: envOrDefault("KAFKA_SOURCE_TOPIC", "raw-weather-reports"),
		KafkaSinkTopic:   envOrDefault("KAFKA_SINK_TOPIC", "transformed-weather-data"),
		KafkaGroupID:     envOrDefault("KAFKA_GROUP_ID", "storm-data-etl"),
		HTTPAddr:         envOrDefault("HTTP_ADDR", ":8080"),
		LogLevel:         envOrDefault("LOG_LEVEL", "info"),
		LogFormat:        envOrDefault("LOG_FORMAT", "json"),
		ShutdownTimeout:  shutdownTimeout,

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

func envOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func parseBrokers(value string) []string {
	parts := strings.Split(value, ",")
	brokers := make([]string, 0, len(parts))
	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed != "" {
			brokers = append(brokers, trimmed)
		}
	}
	return brokers
}

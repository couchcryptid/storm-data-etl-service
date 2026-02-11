package config

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	defaultBroker   = "localhost:9092"
	testMapboxToken = "pk.test-token"
)

func TestLoad_Defaults(t *testing.T) {
	cfg, err := Load()
	require.NoError(t, err)

	assert.Equal(t, []string{defaultBroker}, cfg.KafkaBrokers)
	assert.Equal(t, "raw-weather-reports", cfg.KafkaSourceTopic)
	assert.Equal(t, "transformed-weather-data", cfg.KafkaSinkTopic)
	assert.Equal(t, "storm-data-etl", cfg.KafkaGroupID)
	assert.Equal(t, ":8080", cfg.HTTPAddr)
	assert.Equal(t, "info", cfg.LogLevel)
	assert.Equal(t, "json", cfg.LogFormat)
	assert.Equal(t, 10*time.Second, cfg.ShutdownTimeout)
	assert.Equal(t, 50, cfg.BatchSize)
	assert.Equal(t, 500*time.Millisecond, cfg.BatchFlushInterval)
	assert.False(t, cfg.MapboxEnabled)
	assert.Empty(t, cfg.MapboxToken)
	assert.Equal(t, 5*time.Second, cfg.MapboxTimeout)
	assert.Equal(t, 1000, cfg.MapboxCacheSize)
}

func TestLoad_CustomEnv(t *testing.T) {
	t.Setenv("KAFKA_BROKERS", "broker1:9092,broker2:9092")
	t.Setenv("KAFKA_SOURCE_TOPIC", "custom-source")
	t.Setenv("KAFKA_SINK_TOPIC", "custom-sink")
	t.Setenv("KAFKA_GROUP_ID", "custom-group")
	t.Setenv("HTTP_ADDR", ":9090")
	t.Setenv("LOG_LEVEL", "debug")
	t.Setenv("LOG_FORMAT", "text")
	t.Setenv("SHUTDOWN_TIMEOUT", "30s")
	t.Setenv("BATCH_SIZE", "100")
	t.Setenv("BATCH_FLUSH_INTERVAL", "1s")
	t.Setenv("MAPBOX_TOKEN", testMapboxToken)
	t.Setenv("MAPBOX_TIMEOUT", "10s")
	t.Setenv("MAPBOX_CACHE_SIZE", "500")

	cfg, err := Load()
	require.NoError(t, err)

	assert.Equal(t, []string{"broker1:9092", "broker2:9092"}, cfg.KafkaBrokers)
	assert.Equal(t, "custom-source", cfg.KafkaSourceTopic)
	assert.Equal(t, "custom-sink", cfg.KafkaSinkTopic)
	assert.Equal(t, "custom-group", cfg.KafkaGroupID)
	assert.Equal(t, ":9090", cfg.HTTPAddr)
	assert.Equal(t, "debug", cfg.LogLevel)
	assert.Equal(t, "text", cfg.LogFormat)
	assert.Equal(t, 30*time.Second, cfg.ShutdownTimeout)
	assert.Equal(t, 100, cfg.BatchSize)
	assert.Equal(t, 1*time.Second, cfg.BatchFlushInterval)
	assert.True(t, cfg.MapboxEnabled)
	assert.Equal(t, testMapboxToken, cfg.MapboxToken)
	assert.Equal(t, 10*time.Second, cfg.MapboxTimeout)
	assert.Equal(t, 500, cfg.MapboxCacheSize)
}

func TestLoad_InvalidShutdownTimeout(t *testing.T) {
	t.Setenv("SHUTDOWN_TIMEOUT", "not-a-duration")
	_, err := Load()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "SHUTDOWN_TIMEOUT")
}

func TestLoad_NegativeShutdownTimeout(t *testing.T) {
	t.Setenv("SHUTDOWN_TIMEOUT", "-1s")
	_, err := Load()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "SHUTDOWN_TIMEOUT")
}

func TestLoad_InvalidBatchSize(t *testing.T) {
	t.Setenv("BATCH_SIZE", "0")
	_, err := Load()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "BATCH_SIZE")
}

func TestLoad_BatchSizeTooLarge(t *testing.T) {
	t.Setenv("BATCH_SIZE", "9999")
	_, err := Load()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "BATCH_SIZE")
}

func TestLoad_InvalidBatchFlushInterval(t *testing.T) {
	t.Setenv("BATCH_FLUSH_INTERVAL", "not-a-duration")
	_, err := Load()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "BATCH_FLUSH_INTERVAL")
}

func TestLoad_InvalidMapboxTimeout(t *testing.T) {
	t.Setenv("MAPBOX_TIMEOUT", "bad")
	_, err := Load()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "MAPBOX_TIMEOUT")
}

func TestLoad_MapboxEnabledWithoutToken(t *testing.T) {
	t.Setenv("MAPBOX_ENABLED", "true")
	_, err := Load()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "MAPBOX_TOKEN")
}

func TestLoad_MapboxTokenImpliesEnabled(t *testing.T) {
	t.Setenv("MAPBOX_TOKEN", testMapboxToken)
	cfg, err := Load()
	require.NoError(t, err)
	assert.True(t, cfg.MapboxEnabled)
}

func TestLoad_MapboxExplicitlyDisabled(t *testing.T) {
	t.Setenv("MAPBOX_TOKEN", testMapboxToken)
	t.Setenv("MAPBOX_ENABLED", "false")
	cfg, err := Load()
	require.NoError(t, err)
	assert.False(t, cfg.MapboxEnabled)
}

package kafka

import (
	"testing"
	"time"

	"github.com/couchcryptid/storm-data-etl/internal/domain"
	kafkago "github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMapMessageToRawEvent(t *testing.T) {
	now := time.Now()
	msg := kafkago.Message{
		Key:       []byte("key-1"),
		Value:     []byte(`{"id":"evt-1"}`),
		Topic:     "raw-weather-reports",
		Partition: 2,
		Offset:    42,
		Time:      now,
		Headers: []kafkago.Header{
			{Key: "source", Value: []byte("noaa")},
		},
	}

	raw := mapMessageToRawEvent(msg)

	assert.Equal(t, []byte("key-1"), raw.Key)
	assert.JSONEq(t, `{"id":"evt-1"}`, string(raw.Value))
	assert.Equal(t, "raw-weather-reports", raw.Topic)
	assert.Equal(t, 2, raw.Partition)
	assert.Equal(t, int64(42), raw.Offset)
	assert.Equal(t, now, raw.Timestamp)
	assert.Equal(t, "noaa", raw.Headers["source"])
}

func TestSerializeToMessage(t *testing.T) {
	now := time.Date(2024, 4, 26, 15, 10, 0, 0, time.UTC)
	event := domain.StormEvent{
		ID:          "evt-1",
		EventType:   "hail",
		Geo:         domain.Geo{Lat: 35.0, Lon: -97.0},
		ProcessedAt: now,
	}

	msg, err := serializeToMessage(event)
	require.NoError(t, err)

	assert.Equal(t, []byte("evt-1"), msg.Key)
	assert.Contains(t, string(msg.Value), `"event_type":"hail"`)
	assert.Len(t, msg.Headers, 2)
	assert.Equal(t, "event_type", msg.Headers[0].Key)
	assert.Equal(t, []byte("hail"), msg.Headers[0].Value)
	assert.Equal(t, "processed_at", msg.Headers[1].Key)
	assert.Equal(t, []byte(now.Format(time.RFC3339)), msg.Headers[1].Value)
}

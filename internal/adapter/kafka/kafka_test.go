package kafka

import (
	"testing"
	"time"

	"github.com/couchcryptid/storm-data-etl/internal/domain"
	kafkago "github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
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

func TestMapOutputEventToMessage(t *testing.T) {
	event := domain.OutputEvent{
		Key:   []byte("key-1"),
		Value: []byte(`{"id":"evt-1"}`),
		Headers: map[string]string{
			"event_type": "hail",
		},
	}

	msg := mapOutputEventToMessage(event)

	assert.Equal(t, []byte("key-1"), msg.Key)
	assert.JSONEq(t, `{"id":"evt-1"}`, string(msg.Value))
	assert.Len(t, msg.Headers, 1)
	assert.Equal(t, "event_type", msg.Headers[0].Key)
	assert.Equal(t, []byte("hail"), msg.Headers[0].Value)
}

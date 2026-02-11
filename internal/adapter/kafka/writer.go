package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/couchcryptid/storm-data-etl/internal/config"
	"github.com/couchcryptid/storm-data-etl/internal/domain"
	kafkago "github.com/segmentio/kafka-go"
)

// Writer produces messages to a Kafka topic.
// It implements pipeline.BatchLoader.
type Writer struct {
	writer *kafkago.Writer
	logger *slog.Logger
}

// NewWriter creates a Kafka producer for the configured sink topic.
func NewWriter(cfg *config.Config, logger *slog.Logger) *Writer {
	w := &kafkago.Writer{
		Addr:         kafkago.TCP(cfg.KafkaBrokers...),
		Topic:        cfg.KafkaSinkTopic,
		Balancer:     &kafkago.LeastBytes{},
		RequiredAcks: kafkago.RequireAll,
	}
	return &Writer{writer: w, logger: logger}
}

// LoadBatch serializes and publishes multiple storm events to the sink Kafka
// topic in a single WriteMessages call for efficiency.
func (w *Writer) LoadBatch(ctx context.Context, events []domain.StormEvent) error {
	if len(events) == 0 {
		return nil
	}
	msgs := make([]kafkago.Message, len(events))
	for i := range events {
		msg, err := serializeToMessage(events[i])
		if err != nil {
			return err
		}
		msgs[i] = msg
	}
	return w.writer.WriteMessages(ctx, msgs...)
}

func (w *Writer) Close() error {
	return w.writer.Close()
}

// serializeToMessage marshals a StormEvent into a Kafka message.
func serializeToMessage(event domain.StormEvent) (kafkago.Message, error) {
	data, err := json.Marshal(event)
	if err != nil {
		return kafkago.Message{}, fmt.Errorf("serialize storm event: %w", err)
	}
	return kafkago.Message{
		Key:   []byte(event.ID),
		Value: data,
		Headers: []kafkago.Header{
			{Key: "event_type", Value: []byte(event.EventType)},
			{Key: "processed_at", Value: []byte(event.ProcessedAt.Format(time.RFC3339))},
		},
	}, nil
}

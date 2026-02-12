//go:build integration

package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/couchcryptid/storm-data-etl/internal/adapter/kafka"
	"github.com/couchcryptid/storm-data-etl/internal/config"
	"github.com/couchcryptid/storm-data-etl/internal/domain"
	"github.com/couchcryptid/storm-data-etl/internal/observability"
	"github.com/couchcryptid/storm-data-etl/internal/pipeline"
	kafkago "github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testSourceTopic = "test-source"
	testSinkTopic   = "test-sink"
)

// transformedMessage holds a deserialized message read from the sink topic.
type transformedMessage struct {
	Event   domain.StormEvent
	Key     string
	Headers map[string]string
}

// readTransformed reads a single message from the sink consumer and deserializes it.
func readTransformed(ctx context.Context, t *testing.T, consumer *kafkago.Reader) transformedMessage {
	t.Helper()
	readCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	msg, err := consumer.ReadMessage(readCtx)
	require.NoError(t, err, "read from sink topic")

	headers := make(map[string]string, len(msg.Headers))
	for _, h := range msg.Headers {
		headers[h.Key] = string(h.Value)
	}
	var event domain.StormEvent
	require.NoError(t, json.Unmarshal(msg.Value, &event), "unmarshal sink message")

	return transformedMessage{
		Event:   event,
		Key:     string(msg.Key),
		Headers: headers,
	}
}

// TestKafkaReaderWriter verifies the adapter layer: kafka.Reader (Extractor) and
// kafka.Writer (Loader) correctly round-trip a message through Kafka.
func TestKafkaReaderWriter(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	broker := startKafka(ctx, t)

	createTopic(t, broker, testSourceTopic)
	createTopic(t, broker, testSinkTopic)

	cfg := &config.Config{
		KafkaBrokers:       []string{broker},
		KafkaSourceTopic:   testSourceTopic,
		KafkaSinkTopic:     testSinkTopic,
		KafkaGroupID:       fmt.Sprintf("test-reader-%d", time.Now().UnixNano()),
		BatchFlushInterval: 5 * time.Second,
	}

	// Publish a raw CSV record to the source topic.
	records := loadMockData(t)
	record := records[0] // first hail record: 8 ESE Chappel, TX
	payload, err := json.Marshal(record)
	require.NoError(t, err)

	baseDate := time.Date(2024, time.April, 26, 0, 0, 0, 0, time.UTC)
	producer := &kafkago.Writer{
		Addr:  kafkago.TCP(broker),
		Topic: testSourceTopic,
	}
	t.Cleanup(func() { _ = producer.Close() })

	require.NoError(t, producer.WriteMessages(ctx, kafkago.Message{
		Key:   []byte("test-key"),
		Value: payload,
		Time:  baseDate,
	}))

	// Extract via kafka.Reader.
	// Retry because the consumer group may need time to rebalance before
	// partitions are assigned and messages become available.
	reader := kafka.NewReader(cfg, discardLogger())
	t.Cleanup(func() { _ = reader.Close() })

	var batch []domain.RawEvent
	for {
		var err error
		batch, err = reader.ExtractBatch(ctx, 1)
		require.NoError(t, err)
		if len(batch) > 0 {
			break
		}
		if ctx.Err() != nil {
			t.Fatal("timed out waiting for message from source topic")
		}
	}
	require.Len(t, batch, 1)
	raw := batch[0]
	assert.Equal(t, []byte("test-key"), raw.Key)
	assert.Equal(t, payload, raw.Value)
	assert.Equal(t, testSourceTopic, raw.Topic)
	require.NotNil(t, raw.Commit, "commit callback should be set")

	// Commit the offset.
	require.NoError(t, raw.Commit(ctx))

	// Transform the raw event into a storm event.
	transformer := pipeline.NewTransformer(discardLogger())
	event, err := transformer.Transform(ctx, raw)
	require.NoError(t, err)

	// Load via kafka.Writer.
	writer := kafka.NewWriter(cfg, discardLogger())
	t.Cleanup(func() { _ = writer.Close() })

	require.NoError(t, writer.LoadBatch(ctx, []domain.StormEvent{event}))

	// Read from the sink topic and verify headers + value.
	consumer := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers:     []string{broker},
		Topic:       testSinkTopic,
		GroupID:     fmt.Sprintf("test-consumer-%d", time.Now().UnixNano()),
		StartOffset: kafkago.FirstOffset,
	})
	t.Cleanup(func() { _ = consumer.Close() })

	tm := readTransformed(ctx, t, consumer)
	assert.Equal(t, "hail", tm.Headers["event_type"])
	assert.Contains(t, tm.Headers, "processed_at")
	_, err = time.Parse(time.RFC3339, tm.Headers["processed_at"])
	assert.NoError(t, err, "processed_at should be valid RFC3339")

	assert.Equal(t, "hail", tm.Event.EventType)
	assert.Equal(t, "TX", tm.Event.Location.State)
	assert.Equal(t, "San Saba", tm.Event.Location.County)
	assert.Equal(t, "Chappel", tm.Event.Location.Name)
	assert.Equal(t, 1.25, tm.Event.Measurement.Magnitude)
}

// TestPipelineEndToEnd wires the full pipeline (Reader → Transformer → Writer) with
// real Kafka and verifies that all mock CSV records are correctly enriched.
func TestPipelineEndToEnd(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	broker := startKafka(ctx, t)

	createTopic(t, broker, testSourceTopic)
	createTopic(t, broker, testSinkTopic)

	cfg := &config.Config{
		KafkaBrokers:       []string{broker},
		KafkaSourceTopic:   testSourceTopic,
		KafkaSinkTopic:     testSinkTopic,
		KafkaGroupID:       fmt.Sprintf("test-pipeline-%d", time.Now().UnixNano()),
		BatchFlushInterval: 5 * time.Second,
	}

	// Publish all mock CSV records to the source topic.
	records := loadMockData(t)
	baseDate := time.Date(2024, time.April, 26, 0, 0, 0, 0, time.UTC)

	producer := &kafkago.Writer{
		Addr:  kafkago.TCP(broker),
		Topic: testSourceTopic,
	}
	t.Cleanup(func() { _ = producer.Close() })

	msgs := make([]kafkago.Message, 0, len(records))
	for i, rec := range records {
		payload, err := json.Marshal(rec)
		require.NoError(t, err)
		msgs = append(msgs, kafkago.Message{
			Key:   []byte(fmt.Sprintf("record-%d", i)),
			Value: payload,
			Time:  baseDate,
		})
	}
	require.NoError(t, producer.WriteMessages(ctx, msgs...))

	// Wire up the pipeline.
	reader := kafka.NewReader(cfg, discardLogger())
	t.Cleanup(func() { _ = reader.Close() })

	transformer := pipeline.NewTransformer(discardLogger())

	writer := kafka.NewWriter(cfg, discardLogger())
	t.Cleanup(func() { _ = writer.Close() })

	metrics := observability.NewMetricsForTesting()
	p := pipeline.New(reader, transformer, writer, discardLogger(), metrics, 50)

	// Run the pipeline in a goroutine.
	pipelineCtx, pipelineCancel := context.WithCancel(ctx)
	errCh := make(chan error, 1)
	go func() { errCh <- p.Run(pipelineCtx) }()

	// Read all enriched messages from the sink topic.
	consumer := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers:     []string{broker},
		Topic:       testSinkTopic,
		GroupID:     fmt.Sprintf("test-sink-%d", time.Now().UnixNano()),
		StartOffset: kafkago.FirstOffset,
	})
	t.Cleanup(func() { _ = consumer.Close() })

	received := make([]transformedMessage, 0, len(records))
	for len(received) < len(records) {
		tm := readTransformed(ctx, t, consumer)
		received = append(received, tm)
	}

	pipelineCancel()
	require.NoError(t, <-errCh)

	// Validate counts by event type.
	require.Len(t, received, len(records))
	typeCounts := map[string]int{}
	for _, tm := range received {
		typeCounts[tm.Event.EventType]++

		// Every message must have type and processed_at headers.
		assert.NotEmpty(t, tm.Headers["event_type"], "missing event_type header")
		assert.Contains(t, tm.Headers, "processed_at", "missing processed_at header")
		_, err := time.Parse(time.RFC3339, tm.Headers["processed_at"])
		assert.NoError(t, err, "invalid processed_at format")

		// All events should have a time bucket.
		assert.False(t, tm.Event.TimeBucket.IsZero(), "missing time_bucket")
	}

	assert.Equal(t, 79, typeCounts["hail"], "hail count")
	assert.Equal(t, 149, typeCounts["tornado"], "tornado count")
	assert.Equal(t, 43, typeCounts["wind"], "wind count")

	// Spot-check a known record: 8 ESE Chappel, San Saba TX (1.25" hail).
	var foundHail bool
	for _, tm := range received {
		if tm.Event.EventType != "hail" || tm.Event.Measurement.Magnitude != 1.25 ||
			tm.Event.Location.County != "San Saba" {
			continue
		}
		foundHail = true
		assert.Equal(t, "Chappel", tm.Event.Location.Name)
		require.NotNil(t, tm.Event.Location.Direction)
		assert.Equal(t, "ESE", *tm.Event.Location.Direction)
		require.NotNil(t, tm.Event.Location.Distance)
		assert.Equal(t, 8.0, *tm.Event.Location.Distance)
		assert.Equal(t, "SJT", tm.Event.SourceOffice)
		require.NotNil(t, tm.Event.Measurement.Severity)
		assert.Equal(t, "moderate", *tm.Event.Measurement.Severity)
		assert.Equal(t, time.Date(2024, time.April, 26, 15, 0, 0, 0, time.UTC), tm.Event.TimeBucket)
		break
	}
	assert.True(t, foundHail, "expected to find San Saba TX 1.25in hail record")

	// Spot-check a tornado record: 2 N Mcalester, Pittsburg OK.
	var foundTornado bool
	for _, tm := range received {
		if tm.Event.Location.State != "OK" || tm.Event.Location.County != "Pittsburg" || tm.Event.EventType != "tornado" {
			continue
		}
		foundTornado = true
		assert.Equal(t, "Mcalester", tm.Event.Location.Name)
		assert.Equal(t, "TSA", tm.Event.SourceOffice)
		assert.Equal(t, time.Date(2024, time.April, 26, 12, 0, 0, 0, time.UTC), tm.Event.TimeBucket)
		break
	}
	assert.True(t, foundTornado, "expected to find Pittsburg OK tornado record")
}

// TestPipelineTransformError verifies that an invalid message (poison pill) is
// skipped and the pipeline continues processing valid messages.
func TestPipelineTransformError(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	broker := startKafka(ctx, t)

	createTopic(t, broker, testSourceTopic)
	createTopic(t, broker, testSinkTopic)

	cfg := &config.Config{
		KafkaBrokers:       []string{broker},
		KafkaSourceTopic:   testSourceTopic,
		KafkaSinkTopic:     testSinkTopic,
		KafkaGroupID:       fmt.Sprintf("test-poison-%d", time.Now().UnixNano()),
		BatchFlushInterval: 5 * time.Second,
	}

	baseDate := time.Date(2024, time.April, 26, 0, 0, 0, 0, time.UTC)

	// Publish: invalid JSON, then a valid CSV record.
	records := loadMockData(t)
	validPayload, err := json.Marshal(records[0])
	require.NoError(t, err)

	producer := &kafkago.Writer{
		Addr:  kafkago.TCP(broker),
		Topic: testSourceTopic,
	}
	t.Cleanup(func() { _ = producer.Close() })

	require.NoError(t, producer.WriteMessages(ctx,
		kafkago.Message{Key: []byte("bad"), Value: []byte("not-json{{{"), Time: baseDate},
		kafkago.Message{Key: []byte("good"), Value: validPayload, Time: baseDate},
	))

	// Wire up the pipeline.
	reader := kafka.NewReader(cfg, discardLogger())
	t.Cleanup(func() { _ = reader.Close() })

	transformer := pipeline.NewTransformer(discardLogger())

	writer := kafka.NewWriter(cfg, discardLogger())
	t.Cleanup(func() { _ = writer.Close() })

	metrics := observability.NewMetricsForTesting()
	p := pipeline.New(reader, transformer, writer, discardLogger(), metrics, 50)

	pipelineCtx, pipelineCancel := context.WithCancel(ctx)
	errCh := make(chan error, 1)
	go func() { errCh <- p.Run(pipelineCtx) }()

	// Only the valid message should appear on the sink topic.
	consumer := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers:     []string{broker},
		Topic:       testSinkTopic,
		GroupID:     fmt.Sprintf("test-sink-%d", time.Now().UnixNano()),
		StartOffset: kafkago.FirstOffset,
	})
	t.Cleanup(func() { _ = consumer.Close() })

	tm := readTransformed(ctx, t, consumer)
	assert.Equal(t, "hail", tm.Event.EventType)
	assert.Equal(t, "TX", tm.Event.Location.State)

	// Verify no second message arrives (the poison pill was skipped).
	readCtx, readCancel := context.WithTimeout(ctx, 5*time.Second)
	_, err = consumer.ReadMessage(readCtx)
	readCancel()
	assert.Error(t, err, "expected no second message on sink topic")

	pipelineCancel()
	require.NoError(t, <-errCh)
}

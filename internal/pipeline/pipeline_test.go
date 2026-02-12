package pipeline_test

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/couchcryptid/storm-data-etl/internal/domain"
	"github.com/couchcryptid/storm-data-etl/internal/observability"
	"github.com/couchcryptid/storm-data-etl/internal/pipeline"
	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- mocks ---

type mockBatchExtractor struct {
	batches [][]domain.RawEvent
	index   atomic.Int64
}

func (m *mockBatchExtractor) ExtractBatch(ctx context.Context, _ int) ([]domain.RawEvent, error) {
	i := int(m.index.Add(1) - 1)
	if i >= len(m.batches) {
		// block until context cancelled to simulate waiting for messages
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return m.batches[i], nil
}

type mockTransformer struct {
	err error
}

func (m *mockTransformer) Transform(_ context.Context, raw domain.RawEvent) (domain.StormEvent, error) {
	if m.err != nil {
		return domain.StormEvent{}, m.err
	}
	var event domain.StormEvent
	if err := json.Unmarshal(raw.Value, &event); err != nil {
		return domain.StormEvent{}, err
	}
	return event, nil
}

type mockBatchLoader struct {
	batches [][]domain.StormEvent
}

func (m *mockBatchLoader) LoadBatch(_ context.Context, events []domain.StormEvent) error {
	m.batches = append(m.batches, events)
	return nil
}

func newTestMetrics() *observability.Metrics {
	// Use a fresh registry to avoid "already registered" panics in tests.
	return observability.NewMetricsForTesting()
}

const testBatchSize = 50

// --- pipeline tests ---

func TestPipeline_Run_HappyPath(t *testing.T) {
	raw := makeRawEvent(t, "evt-1", "hail")

	ext := &mockBatchExtractor{batches: [][]domain.RawEvent{{raw}}}
	transformer := &mockTransformer{}
	loader := &mockBatchLoader{}
	metrics := newTestMetrics()

	p := pipeline.New(ext, transformer, loader, slog.Default(), metrics, testBatchSize)

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	err := p.Run(ctx)
	require.NoError(t, err)
	require.Len(t, loader.batches, 1)
	assert.Len(t, loader.batches[0], 1)
	assert.Equal(t, "evt-1", loader.batches[0][0].ID)
	assert.NoError(t, p.CheckReadiness(context.Background()))
}

func TestPipeline_Run_BatchMultipleMessages(t *testing.T) {
	raw1 := makeRawEvent(t, "evt-1", "hail")
	raw2 := makeRawEvent(t, "evt-2", "tornado")

	ext := &mockBatchExtractor{batches: [][]domain.RawEvent{{raw1, raw2}}}
	transformer := &mockTransformer{}
	loader := &mockBatchLoader{}
	metrics := newTestMetrics()

	p := pipeline.New(ext, transformer, loader, slog.Default(), metrics, testBatchSize)

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	err := p.Run(ctx)
	require.NoError(t, err)
	require.Len(t, loader.batches, 1)
	assert.Len(t, loader.batches[0], 2)
}

func TestPipeline_Run_ContextCancellation(t *testing.T) {
	ext := &mockBatchExtractor{} // no batches — will block
	transformer := &mockTransformer{}
	loader := &mockBatchLoader{}
	metrics := newTestMetrics()

	p := pipeline.New(ext, transformer, loader, slog.Default(), metrics, testBatchSize)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	err := p.Run(ctx)
	require.NoError(t, err)
	assert.Empty(t, loader.batches)
}

func TestPipeline_Run_TransformError(t *testing.T) {
	raw := makeRawEvent(t, "evt-2", "hail")

	ext := &mockBatchExtractor{batches: [][]domain.RawEvent{{raw}}}
	transformer := &mockTransformer{err: errors.New("bad data")}
	loader := &mockBatchLoader{}
	metrics := newTestMetrics()

	p := pipeline.New(ext, transformer, loader, slog.Default(), metrics, testBatchSize)

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	err := p.Run(ctx)
	require.NoError(t, err)
	assert.Empty(t, loader.batches)
	assert.Error(t, p.CheckReadiness(context.Background()))
}

func TestPipeline_Run_PartialTransformFailure(t *testing.T) {
	raw1 := makeRawEvent(t, "evt-1", "hail")
	raw2 := makeRawEvent(t, "evt-2", "tornado")

	ext := &mockBatchExtractor{batches: [][]domain.RawEvent{{raw1, raw2}}}
	transformer := &partialFailTransformer{failOn: 2}
	loader := &mockBatchLoader{}
	metrics := newTestMetrics()

	p := pipeline.New(ext, transformer, loader, slog.Default(), metrics, testBatchSize)

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	err := p.Run(ctx)
	require.NoError(t, err)
	require.Len(t, loader.batches, 1)
	assert.Len(t, loader.batches[0], 1, "only the first message should be loaded")
}

func TestPipeline_Run_CommitsAfterLoad(t *testing.T) {
	var commitCount atomic.Int64

	raw := makeRawEvent(t, "evt-5", "hail")
	raw.Topic = "raw-weather-reports"
	raw.Commit = func(_ context.Context) error {
		commitCount.Add(1)
		return nil
	}

	ext := &mockBatchExtractor{batches: [][]domain.RawEvent{{raw}}}
	transformer := &mockTransformer{}
	loader := &mockBatchLoader{}
	metrics := newTestMetrics()

	p := pipeline.New(ext, transformer, loader, slog.Default(), metrics, testBatchSize)

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	err := p.Run(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(1), commitCount.Load())
}

func TestPipeline_Run_BatchCommitAll(t *testing.T) {
	var commitCount atomic.Int64
	makeCommit := func() func(context.Context) error {
		return func(_ context.Context) error {
			commitCount.Add(1)
			return nil
		}
	}

	raw1 := makeRawEvent(t, "evt-1", "hail")
	raw1.Commit = makeCommit()
	raw2 := makeRawEvent(t, "evt-2", "tornado")
	raw2.Commit = makeCommit()

	ext := &mockBatchExtractor{batches: [][]domain.RawEvent{{raw1, raw2}}}
	transformer := &mockTransformer{}
	loader := &mockBatchLoader{}
	metrics := newTestMetrics()

	p := pipeline.New(ext, transformer, loader, slog.Default(), metrics, testBatchSize)

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	err := p.Run(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(2), commitCount.Load())
}

// --- additional mocks ---

type partialFailTransformer struct {
	count  atomic.Int64
	failOn int
}

func (m *partialFailTransformer) Transform(_ context.Context, raw domain.RawEvent) (domain.StormEvent, error) {
	n := int(m.count.Add(1))
	if n == m.failOn {
		return domain.StormEvent{}, errors.New("transform failure")
	}
	var event domain.StormEvent
	if err := json.Unmarshal(raw.Value, &event); err != nil {
		return domain.StormEvent{}, err
	}
	return event, nil
}

type retryBatchExtractor struct {
	event domain.RawEvent
	max   int
	count atomic.Int64
}

func (m *retryBatchExtractor) ExtractBatch(ctx context.Context, _ int) ([]domain.RawEvent, error) {
	n := int(m.count.Add(1))
	if n > m.max {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return []domain.RawEvent{m.event}, nil
}

type failingBatchLoader struct {
	callCount atomic.Int64
	failUntil int
	batches   [][]domain.StormEvent
}

func (m *failingBatchLoader) LoadBatch(_ context.Context, events []domain.StormEvent) error {
	n := int(m.callCount.Add(1))
	if n <= m.failUntil {
		return errors.New("load failed")
	}
	m.batches = append(m.batches, events)
	return nil
}

// --- additional tests ---

func TestPipeline_Run_LoadError_Backoff(t *testing.T) {
	raw := makeRawEvent(t, "evt-backoff", "hail")

	ext := &retryBatchExtractor{event: raw, max: 2}
	transformer := &mockTransformer{}
	loader := &failingBatchLoader{failUntil: 1}
	metrics := newTestMetrics()

	p := pipeline.New(ext, transformer, loader, slog.Default(), metrics, testBatchSize)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err := p.Run(ctx)
	require.NoError(t, err)
	assert.Len(t, loader.batches, 1, "second attempt should succeed after backoff")
}

func TestPipeline_Run_CommitError(t *testing.T) {
	raw := makeRawEvent(t, "evt-commit-err", "tornado")
	raw.Commit = func(_ context.Context) error {
		return errors.New("commit failed")
	}

	ext := &mockBatchExtractor{batches: [][]domain.RawEvent{{raw}}}
	transformer := &mockTransformer{}
	loader := &mockBatchLoader{}
	metrics := newTestMetrics()

	p := pipeline.New(ext, transformer, loader, slog.Default(), metrics, testBatchSize)

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	err := p.Run(ctx)
	require.NoError(t, err)
	require.Len(t, loader.batches, 1)
	assert.Len(t, loader.batches[0], 1)
}

// --- domain tests (unchanged) ---

func TestStormTransformer_Transform(t *testing.T) {
	raw := makeRawCSVEvent(t, "tornado", "EF3")

	transformer := pipeline.NewTransformer(slog.Default())
	event, err := transformer.Transform(context.Background(), raw)
	require.NoError(t, err)
	assert.NotEmpty(t, event.ID)
	assert.Equal(t, "tornado", event.EventType)
}

func TestDomain_ParseRawEvent(t *testing.T) {
	raw := makeRawCSVEvent(t, "wind", "65")
	event, err := domain.ParseRawEvent(raw)
	require.NoError(t, err)
	assert.NotEmpty(t, event.ID)
	assert.Equal(t, "wind", event.EventType)
	assert.InDelta(t, 65.0, event.Measurement.Magnitude, 0.0001)
	assert.True(t, event.ProcessedAt.IsZero())
}

func TestDomain_ParseRawEvent_Invalid(t *testing.T) {
	raw := domain.RawEvent{Value: []byte("not json")}
	_, err := domain.ParseRawEvent(raw)
	assert.Error(t, err)
}

func TestDomain_EnrichStormEvent_NormalizesFields(t *testing.T) {
	fakeClock := clockwork.NewFakeClockAt(time.Date(2024, time.April, 26, 15, 10, 0, 0, time.UTC))
	domain.SetClock(fakeClock)
	t.Cleanup(func() {
		domain.SetClock(nil)
	})

	// Event type is metadata added by upstream service, should already be normalized
	hail := domain.EnrichStormEvent(domain.StormEvent{
		EventType:   "hail",
		Measurement: domain.Measurement{Magnitude: 175, Unit: "in"},
		Location:    domain.Location{Raw: "8 ESE Chappel"},
		Comments:    "Quarter hail reported. (FWD)",
		Geo:         domain.Geo{Lat: 31.02, Lon: -98.44},
		EventTime:   fakeClock.Now(),
	})
	assert.Equal(t, "hail", hail.EventType)
	assert.InEpsilon(t, 1.75, hail.Measurement.Magnitude, 0.0001)
	require.NotNil(t, hail.Measurement.Severity)
	assert.Equal(t, "severe", *hail.Measurement.Severity)
	assert.Equal(t, "FWD", hail.SourceOffice)
	assert.Equal(t, "Chappel", hail.Location.Name)
	require.NotNil(t, hail.Location.Distance)
	assert.InEpsilon(t, 8.0, *hail.Location.Distance, 0.0001)
	require.NotNil(t, hail.Location.Direction)
	assert.Equal(t, "ESE", *hail.Location.Direction)
	assert.Equal(t, time.Date(2024, time.April, 26, 15, 0, 0, 0, time.UTC), hail.TimeBucket)

	tornado := domain.EnrichStormEvent(domain.StormEvent{
		EventType:   "tornado",
		Measurement: domain.Measurement{Magnitude: 2},
	})
	assert.Equal(t, "tornado", tornado.EventType)
	assert.Equal(t, "f_scale", tornado.Measurement.Unit)
	require.NotNil(t, tornado.Measurement.Severity)
	assert.Equal(t, "moderate", *tornado.Measurement.Severity)

	// Invalid event types should be rejected
	unknown := domain.EnrichStormEvent(domain.StormEvent{
		EventType: "snow",
	})
	assert.Empty(t, unknown.EventType)
}

// --- helpers ---

func makeRawCSVEvent(t *testing.T, eventType, magnitude string) domain.RawEvent {
	t.Helper()
	row := map[string]string{
		"Time":      "1510",
		"Location":  "8 ESE Chappel",
		"County":    "San Saba",
		"State":     "TX",
		"Lat":       "31.02",
		"Lon":       "-98.44",
		"Comments":  "Test report. (SJT)",
		"EventType": eventType,
	}
	switch eventType {
	case "hail":
		row["Size"] = magnitude
	case "tornado":
		row["F_Scale"] = magnitude
	case "wind":
		row["Speed"] = magnitude
	}
	data, err := json.Marshal(row)
	require.NoError(t, err)
	return domain.RawEvent{
		Value:     data,
		Topic:     "raw-weather-reports",
		Timestamp: time.Date(2024, 4, 26, 0, 0, 0, 0, time.UTC),
	}
}

func makeRawEvent(t *testing.T, id, eventType string) domain.RawEvent {
	t.Helper()
	data, err := json.Marshal(domain.StormEvent{
		ID:        id,
		EventType: eventType,
		Geo:       domain.Geo{Lat: 35.0, Lon: -97.0},
		EventTime: time.Now(),
	})
	require.NoError(t, err)
	return domain.RawEvent{
		Key:   []byte(id),
		Value: data,
	}
}

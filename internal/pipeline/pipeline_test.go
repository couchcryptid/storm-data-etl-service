package pipeline_test

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/couchcryptid/storm-data-etl-service/internal/domain"
	"github.com/couchcryptid/storm-data-etl-service/internal/observability"
	"github.com/couchcryptid/storm-data-etl-service/internal/pipeline"
	"github.com/google/go-cmp/cmp"
	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- mocks ---

type mockExtractor struct {
	events []domain.RawEvent
	index  atomic.Int64
}

func (m *mockExtractor) Extract(ctx context.Context) (domain.RawEvent, error) {
	i := int(m.index.Add(1) - 1)
	if i >= len(m.events) {
		// block until context cancelled to simulate waiting for messages
		<-ctx.Done()
		return domain.RawEvent{}, ctx.Err()
	}
	return m.events[i], nil
}

type mockTransformer struct {
	err error
}

func (m *mockTransformer) Transform(_ context.Context, raw domain.RawEvent) (domain.OutputEvent, error) {
	if m.err != nil {
		return domain.OutputEvent{}, m.err
	}
	return domain.OutputEvent{Key: raw.Key, Value: raw.Value}, nil
}

type mockLoader struct {
	loaded []domain.OutputEvent
}

func (m *mockLoader) Load(_ context.Context, event domain.OutputEvent) error {
	m.loaded = append(m.loaded, event)
	return nil
}

func newTestMetrics() *observability.Metrics {
	// Use a fresh registry to avoid "already registered" panics in tests.
	return observability.NewMetricsForTesting()
}

// --- tests ---

func TestPipeline_Run_HappyPath(t *testing.T) {
	raw := makeRawEvent(t, "evt-1", "hail")

	ext := &mockExtractor{events: []domain.RawEvent{raw}}
	tfm := &mockTransformer{}
	ldr := &mockLoader{}
	metrics := newTestMetrics()

	p := pipeline.New(ext, tfm, ldr, slog.Default(), metrics)

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	err := p.Run(ctx)
	require.NoError(t, err)
	assert.Len(t, ldr.loaded, 1)
	assert.Equal(t, raw.Value, ldr.loaded[0].Value)
	assert.True(t, p.Ready())
}

func TestPipeline_Run_ContextCancellation(t *testing.T) {
	ext := &mockExtractor{} // no events — will block
	tfm := &mockTransformer{}
	ldr := &mockLoader{}
	metrics := newTestMetrics()

	p := pipeline.New(ext, tfm, ldr, slog.Default(), metrics)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	err := p.Run(ctx)
	require.NoError(t, err)
	assert.Empty(t, ldr.loaded)
}

func TestPipeline_Run_TransformError(t *testing.T) {
	raw := makeRawEvent(t, "evt-2", "hail")

	ext := &mockExtractor{events: []domain.RawEvent{raw}}
	tfm := &mockTransformer{err: errors.New("bad data")}
	ldr := &mockLoader{}
	metrics := newTestMetrics()

	p := pipeline.New(ext, tfm, ldr, slog.Default(), metrics)

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	err := p.Run(ctx)
	require.NoError(t, err)
	assert.Empty(t, ldr.loaded)
	assert.False(t, p.Ready())
}

func TestPipeline_Run_CommitsAfterLoad(t *testing.T) {
	commitCalled := false

	raw := makeRawEvent(t, "evt-5", "hail")
	raw.Topic = "raw-weather-reports"
	raw.Commit = func(_ context.Context) error {
		commitCalled = true
		return nil
	}

	ext := &mockExtractor{events: []domain.RawEvent{raw}}
	tfm := &mockTransformer{}
	ldr := &mockLoader{}
	metrics := newTestMetrics()

	p := pipeline.New(ext, tfm, ldr, slog.Default(), metrics)

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	err := p.Run(ctx)
	require.NoError(t, err)
	assert.True(t, commitCalled)
}

func TestStormTransformer_Transform(t *testing.T) {
	raw := makeRawEvent(t, "evt-3", "tornado")

	tfm := pipeline.NewTransformer(nil, slog.Default())
	out, err := tfm.Transform(context.Background(), raw)
	require.NoError(t, err)
	assert.Equal(t, []byte("evt-3"), out.Key)
	assert.Contains(t, string(out.Value), `"type":"tornado"`)
}

func TestDomain_ParseRawEvent(t *testing.T) {
	raw := makeRawEvent(t, "evt-4", "wind")
	event, err := domain.ParseRawEvent(raw)
	require.NoError(t, err)
	assert.Equal(t, "evt-4", event.ID)
	assert.Equal(t, "wind", event.EventType)
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
		EventType: "hail",
		Magnitude: 175,
		Unit:      "in",
		Location:  domain.Location{Raw: "8 ESE Chappel"},
		Comments:  "Quarter hail reported. (FWD)",
		Geo:       domain.Geo{Lat: 31.02, Lon: -98.44},
		BeginTime: fakeClock.Now(),
	})
	assert.Equal(t, "hail", hail.EventType)
	assert.InEpsilon(t, 1.75, hail.Magnitude, 0.0001)
	assert.Equal(t, "severe", hail.Severity)
	assert.Equal(t, "FWD", hail.SourceOffice)
	assert.Equal(t, "Chappel", hail.Location.Name)
	assert.InEpsilon(t, 8.0, hail.Location.Distance, 0.0001)
	assert.Equal(t, "ESE", hail.Location.Direction)
	assert.Equal(t, "2024-04-26T15:00:00Z", hail.TimeBucket)

	tornado := domain.EnrichStormEvent(domain.StormEvent{
		EventType: "tornado",
		Magnitude: 2,
	})
	assert.Equal(t, "tornado", tornado.EventType)
	assert.Equal(t, "f_scale", tornado.Unit)
	assert.Equal(t, "moderate", tornado.Severity)

	// Invalid event types should be rejected
	unknown := domain.EnrichStormEvent(domain.StormEvent{
		EventType: "snow",
	})
	assert.Empty(t, unknown.EventType)
}

func TestDomain_SerializeStormEvent(t *testing.T) {
	event := domain.StormEvent{
		ID:          "evt-1",
		EventType:   "hail",
		Geo:         domain.Geo{Lat: 35.0, Lon: -97.0},
		ProcessedAt: time.Now(),
	}

	out, err := domain.SerializeStormEvent(event)
	require.NoError(t, err)
	assert.Equal(t, []byte("evt-1"), out.Key)
	assert.Equal(t, "hail", out.Headers["type"])

	var roundtrip domain.StormEvent
	require.NoError(t, json.Unmarshal(out.Value, &roundtrip))

	type eventSummary struct {
		ID        string
		EventType string
		Lat       float64
		Lon       float64
	}

	expected := eventSummary{ID: event.ID, EventType: event.EventType, Lat: event.Geo.Lat, Lon: event.Geo.Lon}
	actual := eventSummary{ID: roundtrip.ID, EventType: roundtrip.EventType, Lat: roundtrip.Geo.Lat, Lon: roundtrip.Geo.Lon}
	if diff := cmp.Diff(expected, actual); diff != "" {
		t.Fatalf("roundtrip mismatch (-want +got):\n%s", diff)
	}
}

// --- helpers ---

func makeRawEvent(t *testing.T, id, eventType string) domain.RawEvent {
	t.Helper()
	data, err := json.Marshal(domain.StormEvent{
		ID:        id,
		EventType: eventType,
		Geo:       domain.Geo{Lat: 35.0, Lon: -97.0},
		BeginTime: time.Now(),
	})
	require.NoError(t, err)
	return domain.RawEvent{
		Key:   []byte(id),
		Value: data,
	}
}

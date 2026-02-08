package domain

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
)

// --- mock geocoder ---

type mockGeocoder struct {
	forwardResult GeocodingResult
	forwardErr    error
	reverseResult GeocodingResult
	reverseErr    error
	forwardCalls  int
	reverseCalls  int
}

func (m *mockGeocoder) ForwardGeocode(_ context.Context, _, _ string) (GeocodingResult, error) {
	m.forwardCalls++
	return m.forwardResult, m.forwardErr
}

func (m *mockGeocoder) ReverseGeocode(_ context.Context, _, _ float64) (GeocodingResult, error) {
	m.reverseCalls++
	return m.reverseResult, m.reverseErr
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// --- tests ---

func TestEnrichWithGeocoding_NilGeocoder(t *testing.T) {
	event := StormEvent{
		ID:       "evt-1",
		Location: Location{Name: "AUSTIN", State: "TX"},
	}

	result := EnrichWithGeocoding(context.Background(), event, nil, discardLogger())

	assert.Empty(t, result.GeoSource)
	assert.Empty(t, result.FormattedAddress)
}

func TestEnrichWithGeocoding_ForwardGeocode(t *testing.T) {
	geo := &mockGeocoder{
		forwardResult: GeocodingResult{
			Lat:              30.2672,
			Lon:              -97.7431,
			FormattedAddress: "Austin, Texas, United States",
			PlaceName:        "Austin",
			Confidence:       0.95,
		},
	}

	event := StormEvent{
		ID:       "evt-1",
		Location: Location{Name: "AUSTIN", State: "TX"},
		Geo:      Geo{Lat: 0, Lon: 0}, // no coordinates → forward geocode
	}

	result := EnrichWithGeocoding(context.Background(), event, geo, discardLogger())

	assert.Equal(t, 30.2672, result.Geo.Lat)
	assert.Equal(t, -97.7431, result.Geo.Lon)
	assert.Equal(t, "Austin, Texas, United States", result.FormattedAddress)
	assert.Equal(t, "Austin", result.PlaceName)
	assert.Equal(t, 0.95, result.GeoConfidence)
	assert.Equal(t, "forward", result.GeoSource)
	assert.Equal(t, 1, geo.forwardCalls)
	assert.Equal(t, 0, geo.reverseCalls)
}

func TestEnrichWithGeocoding_ReverseGeocode(t *testing.T) {
	geo := &mockGeocoder{
		reverseResult: GeocodingResult{
			FormattedAddress: "Austin, Travis County, Texas",
			PlaceName:        "Austin",
			Confidence:       0.98,
		},
	}

	event := StormEvent{
		ID:  "evt-2",
		Geo: Geo{Lat: 30.2672, Lon: -97.7431}, // has coordinates → reverse geocode
	}

	result := EnrichWithGeocoding(context.Background(), event, geo, discardLogger())

	assert.Equal(t, "Austin, Travis County, Texas", result.FormattedAddress)
	assert.Equal(t, "Austin", result.PlaceName)
	assert.Equal(t, 0.98, result.GeoConfidence)
	assert.Equal(t, "reverse", result.GeoSource)
	assert.Equal(t, 0, geo.forwardCalls)
	assert.Equal(t, 1, geo.reverseCalls)
}

func TestEnrichWithGeocoding_ForwardError_GracefulDegradation(t *testing.T) {
	geo := &mockGeocoder{
		forwardErr: errors.New("API timeout"),
	}

	event := StormEvent{
		ID:       "evt-3",
		Location: Location{Name: "AUSTIN", State: "TX"},
	}

	result := EnrichWithGeocoding(context.Background(), event, geo, discardLogger())

	assert.Equal(t, "failed", result.GeoSource)
	assert.Empty(t, result.FormattedAddress)
	assert.Equal(t, float64(0), result.Geo.Lat) // coordinates not set
}

func TestEnrichWithGeocoding_ReverseError_GracefulDegradation(t *testing.T) {
	geo := &mockGeocoder{
		reverseErr: errors.New("rate limited"),
	}

	event := StormEvent{
		ID:  "evt-4",
		Geo: Geo{Lat: 30.2672, Lon: -97.7431},
	}

	result := EnrichWithGeocoding(context.Background(), event, geo, discardLogger())

	assert.Equal(t, "failed", result.GeoSource)
	assert.Equal(t, 30.2672, result.Geo.Lat) // original coordinates preserved
}

func TestEnrichWithGeocoding_NoLocationData(t *testing.T) {
	geo := &mockGeocoder{}

	event := StormEvent{ID: "evt-5"} // no coords, no location name

	result := EnrichWithGeocoding(context.Background(), event, geo, discardLogger())

	assert.Equal(t, "original", result.GeoSource)
	assert.Equal(t, 0, geo.forwardCalls)
	assert.Equal(t, 0, geo.reverseCalls)
}

func TestEnrichWithGeocoding_CoordsPreferred_OverForward(t *testing.T) {
	geo := &mockGeocoder{
		reverseResult: GeocodingResult{
			FormattedAddress: "Austin, Texas",
			PlaceName:        "Austin",
			Confidence:       0.9,
		},
	}

	// Has both coords and location name → reverse geocode wins
	event := StormEvent{
		ID:       "evt-6",
		Geo:      Geo{Lat: 30.2672, Lon: -97.7431},
		Location: Location{Name: "AUSTIN", State: "TX"},
	}

	result := EnrichWithGeocoding(context.Background(), event, geo, discardLogger())

	assert.Equal(t, "reverse", result.GeoSource)
	assert.Equal(t, 0, geo.forwardCalls)
	assert.Equal(t, 1, geo.reverseCalls)
}

func TestEnrichWithGeocoding_ForwardEmptyResult(t *testing.T) {
	geo := &mockGeocoder{
		forwardResult: GeocodingResult{}, // no coordinates returned
	}

	event := StormEvent{
		ID:       "evt-7",
		Location: Location{Name: "UNKNOWN", State: "XX"},
	}

	result := EnrichWithGeocoding(context.Background(), event, geo, discardLogger())

	assert.Equal(t, "original", result.GeoSource)
}

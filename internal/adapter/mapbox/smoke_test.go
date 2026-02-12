//go:build mapbox

package mapbox

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/couchcryptid/storm-data-etl/internal/observability"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// These tests hit the real Mapbox API and require a valid MAPBOX_TOKEN env var.
// Run with: go test -tags=mapbox ./internal/adapter/mapbox/ -v -count=1

func smokeClient(t *testing.T) *Client {
	t.Helper()
	token := os.Getenv("MAPBOX_TOKEN")
	if token == "" {
		t.Fatal("MAPBOX_TOKEN must be set to run smoke tests")
	}
	return &Client{
		token:      token,
		httpClient: &http.Client{Timeout: 10 * time.Second},
		baseURL:    "https://api.mapbox.com/geocoding/v5/mapbox.places",
		metrics:    observability.NewMetricsForTesting(),
		logger:     slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
}

func TestSmoke_ForwardGeocode(t *testing.T) {
	c := smokeClient(t)

	result, err := c.ForwardGeocode(context.Background(), "AUSTIN", "TX")
	require.NoError(t, err)

	assert.InDelta(t, 30.27, result.Lat, 0.1, "lat should be near Austin")
	assert.InDelta(t, -97.74, result.Lon, 0.1, "lon should be near Austin")
	assert.Contains(t, result.FormattedAddress, "Austin")
	assert.Equal(t, "Austin", result.PlaceName)
	assert.Greater(t, result.Confidence, 0.5)
}

func TestSmoke_ReverseGeocode(t *testing.T) {
	c := smokeClient(t)

	// Austin, TX coordinates
	result, err := c.ReverseGeocode(context.Background(), 30.2672, -97.7431)
	require.NoError(t, err)

	assert.NotEmpty(t, result.FormattedAddress)
	assert.NotEmpty(t, result.PlaceName)
	assert.Greater(t, result.Confidence, 0.0)
}

func TestSmoke_ForwardGeocode_LowRelevance(t *testing.T) {
	c := smokeClient(t)

	// Mapbox's fuzzy matching may still return results for nonsense queries,
	// so we verify the client handles any response gracefully (no error).
	_, err := c.ForwardGeocode(context.Background(), "XYZNONEXISTENT99", "ZZ")
	require.NoError(t, err)
}

func TestSmoke_CachedGeocoder(t *testing.T) {
	c := smokeClient(t)
	cached := NewCachedGeocoder(c, 10, observability.NewMetricsForTesting())

	// First call: cache miss → real API call.
	r1, err := cached.ForwardGeocode(context.Background(), "DALLAS", "TX")
	require.NoError(t, err)
	assert.Contains(t, r1.FormattedAddress, "Dallas")

	// Second call: cache hit → no API call.
	r2, err := cached.ForwardGeocode(context.Background(), "DALLAS", "TX")
	require.NoError(t, err)
	assert.Equal(t, r1, r2)
}

package mapbox

import (
	"context"
	"testing"

	"github.com/couchcryptid/storm-data-etl-service/internal/domain"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- mock for cache tests ---

type countingGeocoder struct {
	forwardCalls int
	reverseCalls int
	result       domain.GeocodingResult
}

func (m *countingGeocoder) ForwardGeocode(_ context.Context, _, _ string) (domain.GeocodingResult, error) {
	m.forwardCalls++
	return m.result, nil
}

func (m *countingGeocoder) ReverseGeocode(_ context.Context, _, _ float64) (domain.GeocodingResult, error) {
	m.reverseCalls++
	return m.result, nil
}

// --- CachedGeocoder tests ---

func TestCachedGeocoder_ForwardCacheHit(t *testing.T) {
	inner := &countingGeocoder{
		result: domain.GeocodingResult{Lat: 30.0, Lon: -97.0, PlaceName: "Austin", FormattedAddress: "Austin, TX"},
	}
	cached := NewCachedGeocoder(inner, 10)

	r1, err := cached.ForwardGeocode(context.Background(), "AUSTIN", "TX")
	require.NoError(t, err)
	assert.Equal(t, "Austin", r1.PlaceName)

	r2, err := cached.ForwardGeocode(context.Background(), "AUSTIN", "TX")
	require.NoError(t, err)
	assert.Equal(t, "Austin", r2.PlaceName)

	assert.Equal(t, 1, inner.forwardCalls, "should only call inner once")
}

func TestCachedGeocoder_ReverseCacheHit(t *testing.T) {
	inner := &countingGeocoder{
		result: domain.GeocodingResult{FormattedAddress: "Austin, TX"},
	}
	cached := NewCachedGeocoder(inner, 10)

	_, err := cached.ReverseGeocode(context.Background(), 30.2672, -97.7431)
	require.NoError(t, err)

	_, err = cached.ReverseGeocode(context.Background(), 30.2672, -97.7431)
	require.NoError(t, err)

	assert.Equal(t, 1, inner.reverseCalls, "should only call inner once")
}

func TestCachedGeocoder_DifferentKeysMiss(t *testing.T) {
	inner := &countingGeocoder{
		result: domain.GeocodingResult{PlaceName: "Place", FormattedAddress: "Place, TX"},
	}
	cached := NewCachedGeocoder(inner, 10)

	_, _ = cached.ForwardGeocode(context.Background(), "AUSTIN", "TX")
	_, _ = cached.ForwardGeocode(context.Background(), "DALLAS", "TX")

	assert.Equal(t, 2, inner.forwardCalls)
}

// --- LRU cache unit tests ---

func TestLRUCache_BasicGetPut(t *testing.T) {
	c := newLRUCache(3)

	c.put("a", domain.GeocodingResult{PlaceName: "A"})
	c.put("b", domain.GeocodingResult{PlaceName: "B"})

	result, ok := c.get("a")
	assert.True(t, ok)
	assert.Equal(t, "A", result.PlaceName)

	_, ok = c.get("missing")
	assert.False(t, ok)
}

func TestLRUCache_Eviction(t *testing.T) {
	c := newLRUCache(2)

	c.put("a", domain.GeocodingResult{PlaceName: "A"})
	c.put("b", domain.GeocodingResult{PlaceName: "B"})
	c.put("c", domain.GeocodingResult{PlaceName: "C"}) // evicts "a"

	_, ok := c.get("a")
	assert.False(t, ok, "a should have been evicted")

	result, ok := c.get("b")
	assert.True(t, ok)
	assert.Equal(t, "B", result.PlaceName)

	result, ok = c.get("c")
	assert.True(t, ok)
	assert.Equal(t, "C", result.PlaceName)
}

func TestLRUCache_AccessPromotesEntry(t *testing.T) {
	c := newLRUCache(2)

	c.put("a", domain.GeocodingResult{PlaceName: "A"})
	c.put("b", domain.GeocodingResult{PlaceName: "B"})

	// Access "a" to promote it
	c.get("a")

	// Insert "c" — should evict "b" (LRU), not "a"
	c.put("c", domain.GeocodingResult{PlaceName: "C"})

	_, ok := c.get("a")
	assert.True(t, ok, "a was accessed recently, should not be evicted")

	_, ok = c.get("b")
	assert.False(t, ok, "b should have been evicted")
}

func TestLRUCache_UpdateExisting(t *testing.T) {
	c := newLRUCache(2)

	c.put("a", domain.GeocodingResult{PlaceName: "A1"})
	c.put("a", domain.GeocodingResult{PlaceName: "A2"})

	result, ok := c.get("a")
	assert.True(t, ok)
	assert.Equal(t, "A2", result.PlaceName)
}

package mapbox

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/couchcryptid/storm-data-etl-service/internal/observability"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testToken         = "test-token"
	contentTypeJSON   = "application/json"
	headerContentType = "Content-Type"
)

func testMetrics() *observability.Metrics {
	return observability.NewMetricsForTesting()
}

func testClient(baseURL string) *Client {
	return &Client{
		token:      testToken,
		httpClient: &http.Client{Timeout: 5 * time.Second},
		baseURL:    baseURL,
		metrics:    testMetrics(),
		logger:     slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
}

func TestClient_ForwardGeocode_Success(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Contains(t, r.URL.Path, "AUSTIN")
		assert.Equal(t, "1", r.URL.Query().Get("limit"))
		assert.Equal(t, testToken, r.URL.Query().Get("access_token"))

		resp := response{
			Features: []feature{
				{
					Center:    []float64{-97.7431, 30.2672},
					PlaceName: "Austin, Texas, United States",
					Text:      "Austin",
					Relevance: 0.95,
				},
			},
		}
		w.Header().Set(headerContentType, contentTypeJSON)
		require.NoError(t, json.NewEncoder(w).Encode(resp))
	}))
	defer srv.Close()

	c := testClient(srv.URL)
	result, err := c.ForwardGeocode(context.Background(), "AUSTIN", "TX")
	require.NoError(t, err)

	assert.Equal(t, 30.2672, result.Lat)
	assert.Equal(t, -97.7431, result.Lon)
	assert.Equal(t, "Austin, Texas, United States", result.FormattedAddress)
	assert.Equal(t, "Austin", result.PlaceName)
	assert.Equal(t, 0.95, result.Confidence)
}

func TestClient_ReverseGeocode_Success(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		resp := response{
			Features: []feature{
				{
					Center:    []float64{-97.7431, 30.2672},
					PlaceName: "Austin, Travis County, Texas",
					Text:      "Austin",
					Relevance: 0.98,
				},
			},
		}
		w.Header().Set(headerContentType, contentTypeJSON)
		require.NoError(t, json.NewEncoder(w).Encode(resp))
	}))
	defer srv.Close()

	c := testClient(srv.URL)
	result, err := c.ReverseGeocode(context.Background(), 30.2672, -97.7431)
	require.NoError(t, err)

	assert.Equal(t, "Austin, Travis County, Texas", result.FormattedAddress)
	assert.Equal(t, "Austin", result.PlaceName)
	assert.Equal(t, 0.98, result.Confidence)
}

func TestClient_ForwardGeocode_NoResults(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set(headerContentType, contentTypeJSON)
		require.NoError(t, json.NewEncoder(w).Encode(response{Features: []feature{}}))
	}))
	defer srv.Close()

	c := testClient(srv.URL)
	result, err := c.ForwardGeocode(context.Background(), "NONEXISTENT", "XX")
	require.NoError(t, err)
	assert.Equal(t, float64(0), result.Lat)
	assert.Empty(t, result.FormattedAddress)
}

func TestClient_ForwardGeocode_APIError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"message":"Not Authorized"}`))
	}))
	defer srv.Close()

	c := &Client{
		token:      "bad-token",
		httpClient: &http.Client{Timeout: 5 * time.Second},
		baseURL:    srv.URL,
		metrics:    testMetrics(),
		logger:     slog.New(slog.NewTextHandler(io.Discard, nil)),
	}

	_, err := c.ForwardGeocode(context.Background(), "AUSTIN", "TX")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "401")
}

func TestClient_ForwardGeocode_Timeout(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(200 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	c := &Client{
		token:      testToken,
		httpClient: &http.Client{Timeout: 50 * time.Millisecond},
		baseURL:    srv.URL,
		metrics:    testMetrics(),
		logger:     slog.New(slog.NewTextHandler(io.Discard, nil)),
	}

	_, err := c.ForwardGeocode(context.Background(), "AUSTIN", "TX")
	require.Error(t, err)
}

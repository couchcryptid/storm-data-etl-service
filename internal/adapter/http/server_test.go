package http_test

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	httpadapter "github.com/couchcryptid/storm-data-etl-service/internal/adapter/http"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockReadiness struct {
	err error
}

func (m *mockReadiness) CheckReadiness(_ context.Context) error { return m.err }

func newTestServer(readyErr error) *httpadapter.Server {
	return httpadapter.NewServer(":0", &mockReadiness{err: readyErr}, slog.Default())
}

func TestHealthzReturns200(t *testing.T) {
	srv := newTestServer(nil)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)

	srv.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)

	var body map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	assert.Equal(t, "healthy", body["status"])
}

func TestReadyzReturns200WhenReady(t *testing.T) {
	srv := newTestServer(nil)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)

	srv.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)

	var body map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	assert.Equal(t, "ready", body["status"])
}

func TestReadyzReturns503WhenNotReady(t *testing.T) {
	srv := newTestServer(fmt.Errorf("not ready yet"))
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)

	srv.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)

	var body map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	assert.Equal(t, "not ready", body["status"])
	assert.Equal(t, "not ready yet", body["error"])
}

func TestMetricsEndpoint(t *testing.T) {
	srv := newTestServer(nil)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)

	srv.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "go_goroutines")
}

package pipeline_test

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/couchcryptid/storm-data-etl/internal/domain"
	"github.com/couchcryptid/storm-data-etl/internal/pipeline"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockJSONRow map[string]string

func TestStormTransformer_WithMockJSONData(t *testing.T) {
	transformer := pipeline.NewTransformer(nil, slog.Default())
	baseDate := time.Date(2024, time.April, 26, 0, 0, 0, 0, time.UTC)

	cases := []struct {
		name          string
		eventType     string
		expectedUnit  string
		expectedCount int
	}{
		{name: "hail", eventType: "hail", expectedUnit: "in", expectedCount: 79},
		{name: "tornado", eventType: "tornado", expectedUnit: "f_scale", expectedCount: 149},
		{name: "wind", eventType: "wind", expectedUnit: "mph", expectedCount: 43},
	}

	rows := readCombinedRows(t)

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			filtered := filterRowsByType(rows, tc.eventType)
			require.Len(t, filtered, tc.expectedCount)

			for _, row := range filtered {
				raw := rawEventFromCSVRow(t, row, baseDate)

				event, err := transformer.Transform(context.Background(), raw)
				require.NoError(t, err)
				assert.NotEmpty(t, event.ID)
				assert.Equal(t, tc.eventType, event.EventType)
				assert.Equal(t, tc.expectedUnit, event.Measurement.Unit)
				assert.Equal(t, row["State"], event.Location.State)
				assert.Equal(t, row["County"], event.Location.County)
				assert.True(t, strings.HasPrefix(event.ID, tc.eventType+"-"))
			}
		})
	}
}

func readCombinedRows(t *testing.T) []mockJSONRow {
	t.Helper()

	path := filepath.Join("..", "..", "data", "mock", "storm_reports_240426_combined.json")
	data, err := os.ReadFile(path)
	require.NoError(t, err)

	var rows []mockJSONRow
	require.NoError(t, json.Unmarshal(data, &rows))
	return rows
}

func filterRowsByType(rows []mockJSONRow, eventType string) []mockJSONRow {
	filtered := make([]mockJSONRow, 0, len(rows))
	for _, row := range rows {
		if strings.EqualFold(row["EventType"], eventType) {
			filtered = append(filtered, row)
		}
	}
	return filtered
}

// rawEventFromCSVRow marshals the raw CSV row directly as JSON — the same format
// the collector produces — and wraps it in a RawEvent with a Kafka-style timestamp.
func rawEventFromCSVRow(t *testing.T, row mockJSONRow, baseDate time.Time) domain.RawEvent {
	t.Helper()
	payload, err := json.Marshal(row)
	require.NoError(t, err)

	return domain.RawEvent{
		Value:     payload,
		Topic:     "raw-weather-reports",
		Timestamp: baseDate,
	}
}

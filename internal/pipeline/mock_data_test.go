package pipeline_test

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/couchcryptid/storm-data-etl-service/internal/domain"
	"github.com/couchcryptid/storm-data-etl-service/internal/pipeline"
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
		expectedType  string
		magnitudeKey  string
		unit          string
		convertHailIn bool
	}{
		{
			name:          "hail",
			eventType:     "hail",
			expectedType:  "hail",
			magnitudeKey:  "Size",
			unit:          "in",
			convertHailIn: true,
		},
		{
			name:         "tornado",
			eventType:    "tornado",
			expectedType: "tornado",
			magnitudeKey: "F_Scale",
			unit:         "f_scale",
		},
		{
			name:         "wind",
			eventType:    "wind",
			expectedType: "wind",
			magnitudeKey: "Speed",
			unit:         "mph",
		},
	}

	rows := readCombinedRows(t)

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			filtered := filterRowsByType(rows, tc.eventType)
			require.Len(t, filtered, 10)

			for i, row := range filtered {
				event := stormEventFromRow(t, row, tc.eventType, tc.magnitudeKey, tc.unit, tc.convertHailIn, baseDate, i)
				raw := rawEventFromStormEvent(t, event)

				out, err := transformer.Transform(context.Background(), raw)
				require.NoError(t, err)
				assert.Equal(t, []byte(event.ID), out.Key)
				assert.Equal(t, tc.expectedType, out.Headers["type"])
				assert.NotEmpty(t, out.Headers["processed_at"])

				var roundtrip domain.StormEvent
				require.NoError(t, json.Unmarshal(out.Value, &roundtrip))
				assert.Equal(t, event.ID, roundtrip.ID)
				assert.Equal(t, tc.expectedType, roundtrip.EventType)
				assert.Equal(t, event.Location.State, roundtrip.Location.State)
				assert.Equal(t, event.Location.County, roundtrip.Location.County)
				assert.Equal(t, event.Geo.Lat, roundtrip.Geo.Lat)
				assert.Equal(t, event.Geo.Lon, roundtrip.Geo.Lon)
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
		if strings.EqualFold(row["Type"], eventType) {
			filtered = append(filtered, row)
		}
	}
	return filtered
}

func stormEventFromRow(
	t *testing.T,
	row mockJSONRow,
	eventType string,
	magnitudeKey string,
	unit string,
	convertHailIn bool,
	baseDate time.Time,
	index int,
) domain.StormEvent {
	t.Helper()

	lat := parseFloat(row["Lat"])
	lon := parseFloat(row["Lon"])
	magnitude := parseMagnitude(row[magnitudeKey], convertHailIn)
	beginTime := parseTime(baseDate, row["Time"])

	return domain.StormEvent{
		ID:         fmt.Sprintf("%s-%d", eventType, index+1),
		EventType:  eventType,
		Geo:        domain.Geo{Lat: lat, Lon: lon},
		Magnitude:  magnitude,
		Unit:       unit,
		BeginTime:  beginTime,
		EndTime:    beginTime,
		Source:     "mock",
		Location:   domain.Location{Raw: row["Location"], State: row["State"], County: row["County"]},
		Comments:   row["Comments"],
		RawPayload: nil,
	}
}

func rawEventFromStormEvent(t *testing.T, event domain.StormEvent) domain.RawEvent {
	t.Helper()
	payload, err := json.Marshal(event)
	require.NoError(t, err)

	return domain.RawEvent{
		Key:   []byte(event.ID),
		Value: payload,
		Topic: "raw-weather-reports",
	}
}

func parseFloat(value string) float64 {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0
	}
	parsed, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return 0
	}
	return parsed
}

func parseMagnitude(value string, convertHailIn bool) float64 {
	value = strings.TrimSpace(value)
	if value == "" || strings.EqualFold(value, "UNK") {
		return 0
	}
	value = strings.TrimPrefix(value, "EF")
	value = strings.TrimPrefix(value, "F")

	parsed, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return 0
	}

	if convertHailIn && parsed >= 10 {
		return parsed / 100.0
	}

	return parsed
}

func parseTime(baseDate time.Time, hhmm string) time.Time {
	hhmm = strings.TrimSpace(hhmm)
	if len(hhmm) < 3 {
		return baseDate
	}
	if len(hhmm) == 3 {
		hhmm = "0" + hhmm
	}

	hour, errHour := strconv.Atoi(hhmm[:2])
	minutes, errMin := strconv.Atoi(hhmm[2:])
	if errHour != nil || errMin != nil {
		return baseDate
	}

	return time.Date(baseDate.Year(), baseDate.Month(), baseDate.Day(), hour, minutes, 0, 0, baseDate.Location())
}

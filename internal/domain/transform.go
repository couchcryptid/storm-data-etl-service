package domain

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
)

var (
	// sourceOfficeRe matches a 3-5 letter NWS office code in parentheses at the
	// end of a comment, e.g. "Quarter hail reported. (FWD)" -> "FWD".
	sourceOfficeRe = regexp.MustCompile(`\(([A-Z]{3,5})\)\s*$`)

	// locationRe parses NWS-style relative locations: "<distance> <compass> <name>",
	// e.g. "8 ESE Chappel" -> distance=8, direction=ESE, name=Chappel.
	locationRe = regexp.MustCompile(`^(\d+(?:\.\d+)?)\s+([NSEW]{1,3})\s+(.+)$`)
)

// ParseRawEvent deserializes a RawEvent's value into a StormEvent.
// It expects the flat CSV-style JSON produced by the collector service.
func ParseRawEvent(raw RawEvent) (StormEvent, error) {
	var rec RawCSVRecord
	if err := json.Unmarshal(raw.Value, &rec); err != nil {
		return StormEvent{}, fmt.Errorf("parse raw event: %w", err)
	}

	lat := parseFloatOrZero(rec.Lat)
	lon := parseFloatOrZero(rec.Lon)
	magnitude := parseMagnitudeField(rec.Type, rec.Size, rec.FScale, rec.Speed)
	beginTime := parseHHMM(raw.Timestamp, rec.Time)

	return StormEvent{
		ID:        generateID(rec.Type, rec.State, lat, lon, rec.Time),
		EventType: rec.Type,
		Geo:       Geo{Lat: lat, Lon: lon},
		Magnitude: magnitude,
		BeginTime: beginTime,
		EndTime:   beginTime,
		Location:  Location{Raw: rec.Location, State: rec.State, County: rec.County},
		Comments:  rec.Comments,

		RawPayload: raw.Value,
	}, nil
}

// parseFloatOrZero parses a string as float64, returning 0 on failure.
func parseFloatOrZero(s string) float64 {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0
	}
	v, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0
	}
	return v
}

// parseMagnitudeField selects and parses the correct magnitude column based on event type.
// Returns 0 for unknown values like "UNK".
func parseMagnitudeField(eventType, size, fScale, speed string) float64 {
	var raw string
	switch eventType {
	case "hail":
		raw = size
	case "tornado":
		raw = fScale
	case "wind":
		raw = speed
	default:
		return 0
	}

	raw = strings.TrimSpace(raw)
	if raw == "" || strings.EqualFold(raw, "UNK") {
		return 0
	}
	raw = strings.TrimPrefix(raw, "EF")
	raw = strings.TrimPrefix(raw, "F")

	v, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		return 0
	}
	return v
}

// parseHHMM combines a base date with an HHMM time string (e.g. "1510" → 15:10).
func parseHHMM(baseDate time.Time, hhmm string) time.Time {
	hhmm = strings.TrimSpace(hhmm)
	if len(hhmm) < 3 {
		return baseDate
	}
	if len(hhmm) == 3 {
		hhmm = "0" + hhmm
	}

	hour, errH := strconv.Atoi(hhmm[:2])
	mins, errM := strconv.Atoi(hhmm[2:])
	if errH != nil || errM != nil || hour < 0 || hour > 23 || mins < 0 || mins > 59 {
		return baseDate
	}

	return time.Date(
		baseDate.Year(), baseDate.Month(), baseDate.Day(),
		hour, mins, 0, 0, time.UTC,
	)
}

// generateID produces a deterministic ID from the event's key fields.
func generateID(eventType, state string, lat, lon float64, timeStr string) string {
	input := fmt.Sprintf("%s|%s|%.4f|%.4f|%s", eventType, state, lat, lon, timeStr)
	hash := sha256.Sum256([]byte(input))
	short := hex.EncodeToString(hash[:8])
	if eventType == "" {
		return short
	}
	return eventType + "-" + short
}

// EnrichStormEvent normalizes, classifies, and enriches a parsed storm event.
// It validates the event type, infers default units, corrects magnitude encoding
// issues, derives a severity label, extracts the NWS source office from comments,
// parses structured location fields, and assigns an hourly time bucket.
func EnrichStormEvent(event StormEvent) StormEvent {
	event.EventType = normalizeEventType(event.EventType)
	event.Unit = normalizeUnit(event.EventType, event.Unit)
	event.Magnitude = normalizeMagnitude(event.EventType, event.Magnitude, event.Unit)
	event.Severity = deriveSeverity(event.EventType, event.Magnitude)
	event.SourceOffice = extractSourceOffice(event.Comments)
	locationName, locationDistance, locationDirection := parseLocation(event.Location.Raw)
	event.Location.Name = locationName
	event.Location.Distance = locationDistance
	event.Location.Direction = locationDirection
	event.TimeBucket = deriveTimeBucket(event.BeginTime)
	event.ProcessedAt = clock.Now()
	return event
}

// normalizeEventType validates and normalizes the event type metadata added by the upstream service.
// Event type is not part of the original CSV data; it's added when converting CSV to JSON.
// Accepts: "hail", "wind", "tornado" (exact matches only)
func normalizeEventType(value string) string {
	switch value {
	case "hail", "wind", "tornado":
		return value
	default:
		return ""
	}
}

// normalizeUnit returns the unit as-is if present, otherwise infers the default
// unit for the event type: inches for hail, mph for wind, F-scale for tornado.
func normalizeUnit(eventType, unit string) string {
	unit = strings.ToLower(strings.TrimSpace(unit))
	if unit != "" {
		return unit
	}

	switch eventType {
	case "hail":
		return "in"
	case "wind":
		return "mph"
	case "tornado":
		return "f_scale"
	default:
		return ""
	}
}

// normalizeMagnitude corrects known encoding issues in upstream data.
// Some hail reports encode diameter in hundredths of inches (e.g. 175 = 1.75in).
// Values >= 10 with unit "in" are assumed to use this encoding and are divided by 100.
func normalizeMagnitude(eventType string, magnitude float64, unit string) float64 {
	if magnitude == 0 {
		return magnitude
	}
	if eventType == "hail" && unit == "in" && magnitude >= 10 {
		return magnitude / 100.0
	}
	return magnitude
}

// deriveSeverity maps magnitude to a severity label based on NWS thresholds:
//   - hail: <0.75in minor, <1.5in moderate, <2.5in severe, else extreme
//   - wind: <50mph minor, <74mph moderate (tropical storm), <96mph severe, else extreme
//   - tornado: EF0-1 minor, EF2 moderate, EF3-4 severe, EF5 extreme
func deriveSeverity(eventType string, magnitude float64) string {
	if magnitude == 0 {
		return ""
	}

	switch eventType {
	case "hail":
		switch {
		case magnitude < 0.75:
			return "minor"
		case magnitude < 1.5:
			return "moderate"
		case magnitude < 2.5:
			return "severe"
		default:
			return "extreme"
		}
	case "wind":
		switch {
		case magnitude < 50:
			return "minor"
		case magnitude < 74:
			return "moderate"
		case magnitude < 96:
			return "severe"
		default:
			return "extreme"
		}
	case "tornado":
		switch {
		case magnitude <= 1:
			return "minor"
		case magnitude == 2:
			return "moderate"
		case magnitude <= 4:
			return "severe"
		default:
			return "extreme"
		}
	default:
		return ""
	}
}

// extractSourceOffice pulls the NWS Weather Forecast Office (WFO) code from the
// end of a comment string, e.g. "Large hail reported. (OUN)" -> "OUN".
func extractSourceOffice(comments string) string {
	comments = strings.TrimSpace(comments)
	if comments == "" {
		return ""
	}

	matches := sourceOfficeRe.FindStringSubmatch(comments)
	if len(matches) == 2 {
		return matches[1]
	}

	return ""
}

// parseLocation splits an NWS relative location string into (name, distance, direction).
// Input format: "<miles> <compass> <place>", e.g. "8 ESE Chappel".
// Returns the raw string as name if parsing fails.
func parseLocation(location string) (string, float64, string) {
	location = strings.TrimSpace(location)
	if location == "" {
		return "", 0, ""
	}

	matches := locationRe.FindStringSubmatch(location)
	if len(matches) != 4 {
		return location, 0, ""
	}

	distance, err := parseLocationDistance(matches[1])
	if err != nil {
		return location, 0, ""
	}

	return strings.TrimSpace(matches[3]), distance, matches[2]
}

func parseLocationDistance(value string) (float64, error) {
	return strconv.ParseFloat(value, 64)
}

// deriveTimeBucket truncates the event's begin time to the hour in UTC,
// producing a bucket key like "2024-04-26T15:00:00Z" for downstream aggregation.
func deriveTimeBucket(begin time.Time) string {
	if begin.IsZero() {
		return ""
	}

	return begin.UTC().Truncate(time.Hour).Format(time.RFC3339)
}

// SerializeStormEvent marshals a StormEvent into an OutputEvent.
func SerializeStormEvent(event StormEvent) (OutputEvent, error) {
	data, err := json.Marshal(event)
	if err != nil {
		return OutputEvent{}, fmt.Errorf("serialize storm event: %w", err)
	}
	return OutputEvent{
		Key:   []byte(event.ID),
		Value: data,
		Headers: map[string]string{
			"type":         event.EventType,
			"processed_at": event.ProcessedAt.Format(time.RFC3339),
		},
	}, nil
}

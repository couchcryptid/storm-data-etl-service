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
	magnitude := parseMagnitudeField(rec.EventType, rec.Size, rec.FScale, rec.Speed)
	eventTime := parseHHMM(raw.Timestamp, rec.Time)

	return StormEvent{
		ID:          generateID(rec.EventType, rec.State, lat, lon, rec.Time, magnitude),
		EventType:   rec.EventType,
		Geo:         Geo{Lat: lat, Lon: lon},
		Measurement: Measurement{Magnitude: magnitude},
		EventTime:   eventTime,
		Location:    Location{Raw: rec.Location, State: rec.State, County: rec.County},
		Comments:    rec.Comments,

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
// Deterministic IDs enable idempotent upserts (ON CONFLICT DO NOTHING) and
// replay safety — reprocessing the same raw event produces the same ID.
func generateID(eventType, state string, lat, lon float64, timeStr string, magnitude float64) string {
	input := fmt.Sprintf("%s|%s|%.4f|%.4f|%s|%g", eventType, state, lat, lon, timeStr, magnitude)
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
	event.Measurement.Unit = normalizeUnit(event.EventType, event.Measurement.Unit)
	event.Measurement.Magnitude = normalizeMagnitude(event.EventType, event.Measurement.Magnitude, event.Measurement.Unit)
	event.Measurement.Severity = deriveSeverity(event.EventType, event.Measurement.Magnitude)
	event.SourceOffice = extractSourceOffice(event.Comments)
	locationName, locationDistance, locationDirection := parseLocation(event.Location.Raw)
	event.Location.Name = locationName
	event.Location.Distance = locationDistance
	event.Location.Direction = locationDirection
	event.TimeBucket = deriveTimeBucket(event.EventTime)
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
// Values >= 10 with unit "in" are assumed to use this encoding and are divided
// by 100. The threshold of 10 is safe because the largest hail ever recorded in
// the US was approximately 8 inches (Vivian, SD, 2010).
func normalizeMagnitude(eventType string, magnitude float64, unit string) float64 {
	if magnitude == 0 {
		return magnitude
	}
	if eventType == "hail" && unit == "in" && magnitude >= 10 {
		return magnitude / 100.0
	}
	return magnitude
}

// deriveSeverity maps magnitude to a severity label based on operational thresholds
// informed by NWS Severe Weather Criteria and the Enhanced Fujita Scale:
//   - hail: <0.75in minor, <1.5in moderate, <2.5in severe, else extreme
//   - wind: <50mph minor, <74mph moderate (tropical storm threshold), <96mph severe (hurricane Cat 2), else extreme
//   - tornado: EF0-1 minor, EF2 moderate, EF3-4 severe, EF5 extreme
//
// The four-level scale is a project-specific simplification for user-facing queries.
// Returns nil when magnitude is 0 or the event type is unrecognized.
func deriveSeverity(eventType string, magnitude float64) *string {
	if magnitude == 0 {
		return nil
	}

	var s string
	switch eventType {
	case "hail":
		switch {
		case magnitude < 0.75:
			s = "minor"
		case magnitude < 1.5:
			s = "moderate"
		case magnitude < 2.5:
			s = "severe"
		default:
			s = "extreme"
		}
	case "wind":
		switch {
		case magnitude < 50:
			s = "minor"
		case magnitude < 74:
			s = "moderate"
		case magnitude < 96:
			s = "severe"
		default:
			s = "extreme"
		}
	case "tornado":
		switch {
		case magnitude <= 1:
			s = "minor"
		case magnitude == 2:
			s = "moderate"
		case magnitude <= 4:
			s = "severe"
		default:
			s = "extreme"
		}
	default:
		return nil
	}
	return &s
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
// Returns the raw string as name with nil distance/direction if parsing fails.
func parseLocation(location string) (string, *float64, *string) {
	location = strings.TrimSpace(location)
	if location == "" {
		return "", nil, nil
	}

	matches := locationRe.FindStringSubmatch(location)
	if len(matches) != 4 {
		return location, nil, nil
	}

	distance, err := parseLocationDistance(matches[1])
	if err != nil {
		return location, nil, nil
	}

	direction := matches[2]
	return strings.TrimSpace(matches[3]), &distance, &direction
}

func parseLocationDistance(value string) (float64, error) {
	return strconv.ParseFloat(value, 64)
}

// deriveTimeBucket truncates the event time to the hour in UTC.
// Returns zero time if the input is zero.
func deriveTimeBucket(t time.Time) time.Time {
	if t.IsZero() {
		return time.Time{}
	}

	return t.UTC().Truncate(time.Hour)
}

package domain

import (
	"context"
	"time"
)

// RawCSVRecord represents the flat JSON structure produced by the collector.
// Each CSV type has a different magnitude column (Size, F_Scale, Speed),
// but all share the remaining columns.
type RawCSVRecord struct {
	Time      string `json:"Time"`
	Size      string `json:"Size"`     // hail magnitude (hundredths of inches)
	FScale    string `json:"F_Scale"`  // tornado magnitude (EF scale)
	Speed     string `json:"Speed"`    // wind magnitude (mph)
	Location  string `json:"Location"` // NWS relative location, e.g. "8 ESE Chappel"
	County    string `json:"County"`
	State     string `json:"State"`
	Lat       string `json:"Lat"`
	Lon       string `json:"Lon"`
	Comments  string `json:"Comments"`
	EventType string `json:"EventType"` // "hail", "wind", or "tornado"
}

// RawEvent represents an unprocessed message from the source topic.
type RawEvent struct {
	Key       []byte
	Value     []byte
	Headers   map[string]string
	Topic     string
	Partition int
	Offset    int64
	Timestamp time.Time
	Commit    func(ctx context.Context) error
}

// Location holds both the raw NWS location string and its parsed components.
// Nested because these fields are tightly coupled: enrichment parses the raw
// NWS format ("8 ESE Chappel") into name/distance/direction, and all six fields
// travel together through the pipeline. The API flattens to location_* columns.
type Location struct {
	Raw       string   `json:"raw,omitempty"`
	Name      string   `json:"name,omitempty"`
	Distance  *float64 `json:"distance,omitempty"`
	Direction *string  `json:"direction,omitempty"`
	State     string   `json:"state,omitempty"`
	County    string   `json:"county,omitempty"`
}

// Geo represents a WGS-84 latitude/longitude coordinate pair.
// Nested because lat/lon are always used together (geocoding lookups, bounding-box
// queries, distance calculations). The API flattens to geo_lat/geo_lon columns.
type Geo struct {
	Lat float64 `json:"lat,omitempty"`
	Lon float64 `json:"lon,omitempty"`
}

// Measurement groups the numeric observation: what was measured, in what unit,
// and how severe. Nested because magnitude, unit, and severity form a semantic
// chain — unit determines normalization, magnitude determines severity. Maps
// directly to the GraphQL Measurement type. The API flattens to measurement_*
// columns.
type Measurement struct {
	Magnitude float64 `json:"magnitude"`
	Unit      string  `json:"unit"`
	Severity  *string `json:"severity,omitempty"`
}

// Geocoding holds the results of the optional geocoding enrichment step.
// Nested because all four fields are always set together (or all left at zero
// values) from a single Mapbox API call. Maps to the GraphQL Geocoding type.
// The API flattens to geocoding_* columns.
type Geocoding struct {
	FormattedAddress string  `json:"formatted_address,omitempty"`
	PlaceName        string  `json:"place_name,omitempty"`
	Confidence       float64 `json:"confidence,omitempty"`
	Source           string  `json:"source,omitempty"` // "forward", "reverse", "original", "failed"
}

// StormEvent is the domain-rich representation after parsing and enrichment.
//
// All fields are grouped into nested structs when they represent cohesive domain
// concepts: Geo (coordinates), Location (NWS place data), Measurement (what was
// observed), and Geocoding (reverse/forward lookup results). The Kafka wire format
// reflects this nesting. The API deserializes it via json.Unmarshal, flattens to
// prefixed DB columns, and gqlgen auto-resolves the GraphQL types from these structs.
type StormEvent struct {
	ID           string      `json:"id"`
	EventType    string      `json:"event_type"`
	Geo          Geo         `json:"geo,omitempty"`
	Measurement  Measurement `json:"measurement"`
	BeginTime    time.Time   `json:"begin_time"`
	EndTime      time.Time   `json:"end_time"`
	Source       string      `json:"source"`
	Location     Location    `json:"location,omitempty"`
	Comments     string      `json:"comments,omitempty"`
	SourceOffice string      `json:"source_office,omitempty"`
	TimeBucket   time.Time   `json:"time_bucket,omitempty"`
	Geocoding    Geocoding   `json:"geocoding,omitempty"`

	RawPayload  []byte    `json:"-"`
	ProcessedAt time.Time `json:"processed_at"`
}

// OutputEvent is the serialized form destined for the sink topic.
type OutputEvent struct {
	Key     []byte
	Value   []byte
	Headers map[string]string
}

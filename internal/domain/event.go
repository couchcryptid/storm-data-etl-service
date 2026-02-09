package domain

import (
	"context"
	"time"
)

// RawCSVRecord represents the flat JSON structure produced by the collector.
// Each CSV type has a different magnitude column (Size, F_Scale, Speed),
// but all share the remaining columns.
type RawCSVRecord struct {
	Time     string `json:"Time"`
	Size     string `json:"Size"`     // hail magnitude (hundredths of inches)
	FScale   string `json:"F_Scale"`  // tornado magnitude (EF scale)
	Speed    string `json:"Speed"`    // wind magnitude (mph)
	Location string `json:"Location"` // NWS relative location, e.g. "8 ESE Chappel"
	County   string `json:"County"`
	State    string `json:"State"`
	Lat      string `json:"Lat"`
	Lon      string `json:"Lon"`
	Comments string `json:"Comments"`
	Type     string `json:"Type"` // "hail", "wind", or "tornado"
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
type Location struct {
	Raw       string  `json:"raw,omitempty"`
	Name      string  `json:"name,omitempty"`
	Distance  float64 `json:"distance,omitempty"`
	Direction string  `json:"direction,omitempty"`
	State     string  `json:"state,omitempty"`
	County    string  `json:"county,omitempty"`
}

// Geo represents a WGS-84 latitude/longitude coordinate pair.
type Geo struct {
	Lat float64 `json:"lat,omitempty"`
	Lon float64 `json:"lon,omitempty"`
}

// StormEvent is the domain-rich representation after parsing.
type StormEvent struct {
	ID           string    `json:"id"`
	EventType    string    `json:"type"`
	Geo          Geo       `json:"geo,omitempty"`
	Magnitude    float64   `json:"magnitude"`
	Unit         string    `json:"unit"`
	BeginTime    time.Time `json:"begin_time"`
	EndTime      time.Time `json:"end_time"`
	Source       string    `json:"source"`
	Location     Location  `json:"location,omitempty"`
	Comments     string    `json:"comments,omitempty"`
	Severity     string    `json:"severity,omitempty"`
	SourceOffice string    `json:"source_office,omitempty"`
	TimeBucket   string    `json:"time_bucket,omitempty"`

	// Geocoding enrichment fields.
	FormattedAddress string  `json:"formatted_address,omitempty"`
	PlaceName        string  `json:"place_name,omitempty"`
	GeoConfidence    float64 `json:"geo_confidence,omitempty"`
	GeoSource        string  `json:"geo_source,omitempty"` // "forward", "reverse", "original", "failed"

	RawPayload  []byte    `json:"-"`
	ProcessedAt time.Time `json:"processed_at"`
}

// OutputEvent is the serialized form destined for the sink topic.
type OutputEvent struct {
	Key     []byte
	Value   []byte
	Headers map[string]string
}

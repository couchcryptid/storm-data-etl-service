package domain

import "context"

// GeocodingResult contains location data returned by a geocoding provider.
type GeocodingResult struct {
	Lat              float64
	Lon              float64
	FormattedAddress string
	PlaceName        string
	Confidence       float64 // 0.0–1.0 provider confidence score
}

// Geocoder enriches events with geolocation data.
type Geocoder interface {
	// ForwardGeocode converts a location name and state to coordinates.
	ForwardGeocode(ctx context.Context, name, state string) (GeocodingResult, error)

	// ReverseGeocode converts coordinates to place details.
	ReverseGeocode(ctx context.Context, lat, lon float64) (GeocodingResult, error)
}

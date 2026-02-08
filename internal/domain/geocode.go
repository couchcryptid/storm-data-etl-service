package domain

import (
	"context"
	"log/slog"
)

// EnrichWithGeocoding attempts to enrich an event with geocoding data.
// If geocoder is nil or geocoding fails, the event is returned with
// GeoSource set accordingly (graceful degradation).
func EnrichWithGeocoding(ctx context.Context, event StormEvent, geocoder Geocoder, logger *slog.Logger) StormEvent {
	if geocoder == nil {
		return event
	}

	hasCoords := event.Geo.Lat != 0 || event.Geo.Lon != 0
	hasName := event.Location.Name != "" && event.Location.State != ""

	// Forward geocode: location name → coordinates (when coords are missing).
	if !hasCoords && hasName {
		result, err := geocoder.ForwardGeocode(ctx, event.Location.Name, event.Location.State)
		if err != nil {
			logger.Warn("forward geocoding failed",
				"event_id", event.ID,
				"location", event.Location.Name,
				"state", event.Location.State,
				"error", err,
			)
			event.GeoSource = "failed"
			return event
		}
		if result.Lat != 0 || result.Lon != 0 {
			event.Geo.Lat = result.Lat
			event.Geo.Lon = result.Lon
			event.FormattedAddress = result.FormattedAddress
			event.PlaceName = result.PlaceName
			event.GeoConfidence = result.Confidence
			event.GeoSource = "forward"
			return event
		}
		event.GeoSource = "original"
		return event
	}

	// Reverse geocode: coordinates → place details (when coords are present).
	if hasCoords {
		result, err := geocoder.ReverseGeocode(ctx, event.Geo.Lat, event.Geo.Lon)
		if err != nil {
			logger.Warn("reverse geocoding failed",
				"event_id", event.ID,
				"lat", event.Geo.Lat,
				"lon", event.Geo.Lon,
				"error", err,
			)
			event.GeoSource = "failed"
			return event
		}
		if result.FormattedAddress != "" {
			event.FormattedAddress = result.FormattedAddress
			event.PlaceName = result.PlaceName
			event.GeoConfidence = result.Confidence
			event.GeoSource = "reverse"
			return event
		}
		event.GeoSource = "original"
		return event
	}

	event.GeoSource = "original"
	return event
}

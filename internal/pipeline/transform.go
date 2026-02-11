package pipeline

import (
	"context"
	"log/slog"

	"github.com/couchcryptid/storm-data-etl/internal/domain"
)

// StormTransformer implements Transformer using domain transform functions
// with optional geocoding enrichment.
type StormTransformer struct {
	geocoder domain.Geocoder
	logger   *slog.Logger
}

// NewTransformer creates a StormTransformer. Pass a nil geocoder to disable
// geocoding enrichment.
func NewTransformer(geocoder domain.Geocoder, logger *slog.Logger) *StormTransformer {
	return &StormTransformer{
		geocoder: geocoder,
		logger:   logger,
	}
}

func (t *StormTransformer) Transform(ctx context.Context, raw domain.RawEvent) (domain.StormEvent, error) {
	event, err := domain.ParseRawEvent(raw)
	if err != nil {
		return domain.StormEvent{}, err
	}

	event = domain.EnrichStormEvent(event)
	event = domain.EnrichWithGeocoding(ctx, event, t.geocoder, t.logger)

	return event, nil
}

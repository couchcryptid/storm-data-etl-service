package pipeline

import (
	"context"
	"log/slog"

	"github.com/couchcryptid/storm-data-etl/internal/domain"
)

// StormTransformer implements Transformer using domain transform functions.
type StormTransformer struct {
	logger *slog.Logger
}

// NewTransformer creates a StormTransformer.
func NewTransformer(logger *slog.Logger) *StormTransformer {
	return &StormTransformer{
		logger: logger,
	}
}

func (t *StormTransformer) Transform(ctx context.Context, raw domain.RawEvent) (domain.StormEvent, error) {
	event, err := domain.ParseRawEvent(raw)
	if err != nil {
		return domain.StormEvent{}, err
	}

	event = domain.EnrichStormEvent(event)

	return event, nil
}

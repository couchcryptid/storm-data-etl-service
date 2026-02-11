package observability

import (
	"log/slog"

	"github.com/couchcryptid/storm-data-etl/internal/config"
	sharedobs "github.com/couchcryptid/storm-data-shared/observability"
)

// NewLogger creates a structured logger based on config and sets it as the default.
func NewLogger(cfg *config.Config) *slog.Logger {
	return sharedobs.NewLogger(cfg.LogLevel, cfg.LogFormat)
}

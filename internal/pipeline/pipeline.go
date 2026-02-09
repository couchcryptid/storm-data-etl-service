package pipeline

import (
	"context"
	"errors"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/couchcryptid/storm-data-etl-service/internal/domain"
	"github.com/couchcryptid/storm-data-etl-service/internal/observability"
)

// Extractor reads a single raw event from the source.
type Extractor interface {
	Extract(ctx context.Context) (domain.RawEvent, error)
}

// Transformer converts a raw event into an output event.
type Transformer interface {
	Transform(ctx context.Context, raw domain.RawEvent) (domain.OutputEvent, error)
}

// Loader writes an output event to the destination.
type Loader interface {
	Load(ctx context.Context, event domain.OutputEvent) error
}

// Pipeline orchestrates the extract-transform-load loop.
type Pipeline struct {
	extractor   Extractor
	transformer Transformer
	loader      Loader
	logger      *slog.Logger
	metrics     *observability.Metrics
	ready       atomic.Bool
}

// New creates a Pipeline with the given stages and observability.
func New(e Extractor, t Transformer, l Loader, logger *slog.Logger, metrics *observability.Metrics) *Pipeline {
	return &Pipeline{
		extractor:   e,
		transformer: t,
		loader:      l,
		logger:      logger,
		metrics:     metrics,
	}
}

// CheckReadiness returns nil if the pipeline has processed at least one message,
// or an error describing why the service is not yet ready.
func (p *Pipeline) CheckReadiness(_ context.Context) error {
	if !p.ready.Load() {
		return errors.New("pipeline has not processed any messages yet")
	}
	return nil
}

// Run executes the ETL loop until the context is cancelled.
func (p *Pipeline) Run(ctx context.Context) error {
	p.logger.Info("pipeline started")
	p.metrics.PipelineRunning.Set(1)
	defer p.metrics.PipelineRunning.Set(0)

	backoff := 200 * time.Millisecond
	maxBackoff := 5 * time.Second

	for {
		select {
		case <-ctx.Done():
			p.logger.Info("pipeline stopping", "reason", ctx.Err())
			return nil
		default:
		}

		start := time.Now()

		raw, err := p.extractor.Extract(ctx)
		if err != nil {
			p.logger.Error("extract failed", "error", err)
			if !p.backoffOrStop(ctx, &backoff, maxBackoff) {
				return nil
			}
			continue
		}
		p.metrics.MessagesConsumed.Inc()
		backoff = 200 * time.Millisecond

		out, err := p.transformer.Transform(ctx, raw)
		if err != nil {
			p.logger.Warn("transform failed, skipping message",
				"error", err,
				"topic", raw.Topic,
				"partition", raw.Partition,
				"offset", raw.Offset,
			)
			p.metrics.TransformErrors.Inc()
			p.commitOffset(ctx, raw)
			continue
		}

		if err := p.loader.Load(ctx, out); err != nil {
			p.logger.Error("load failed", "error", err)
			if !p.backoffOrStop(ctx, &backoff, maxBackoff) {
				return nil
			}
			continue
		}
		p.metrics.MessagesProduced.Inc()
		p.commitOffset(ctx, raw)

		p.metrics.ProcessingDuration.Observe(time.Since(start).Seconds())
		p.ready.Store(true)
	}
}

// backoffOrStop checks for context cancellation, sleeps with the current backoff,
// and advances the backoff. Returns false if the pipeline should stop.
func (p *Pipeline) backoffOrStop(ctx context.Context, backoff *time.Duration, maxBackoff time.Duration) bool {
	if ctx.Err() != nil {
		return false
	}
	if !sleepWithContext(ctx, *backoff) {
		return false
	}
	*backoff = nextBackoff(*backoff, maxBackoff)
	return true
}

// commitOffset commits the message offset if a commit function is available.
func (p *Pipeline) commitOffset(ctx context.Context, raw domain.RawEvent) {
	if raw.Commit == nil {
		return
	}
	if err := raw.Commit(ctx); err != nil {
		p.logger.Warn("commit offset failed", "error", err,
			"topic", raw.Topic, "partition", raw.Partition, "offset", raw.Offset)
	}
}

func nextBackoff(current, maxBackoff time.Duration) time.Duration {
	next := current * 2
	if next > maxBackoff {
		return maxBackoff
	}
	return next
}

func sleepWithContext(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return true
	}

	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

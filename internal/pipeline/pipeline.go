package pipeline

import (
	"context"
	"errors"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/couchcryptid/storm-data-etl/internal/domain"
	"github.com/couchcryptid/storm-data-etl/internal/observability"
)

// BatchExtractor reads up to batchSize raw events from the source.
type BatchExtractor interface {
	ExtractBatch(ctx context.Context, batchSize int) ([]domain.RawEvent, error)
}

// Transformer converts a raw event into an output event.
type Transformer interface {
	Transform(ctx context.Context, raw domain.RawEvent) (domain.OutputEvent, error)
}

// BatchLoader writes multiple output events to the destination.
type BatchLoader interface {
	LoadBatch(ctx context.Context, events []domain.OutputEvent) error
}

// Pipeline orchestrates the extract-transform-load loop.
type Pipeline struct {
	extractor   BatchExtractor
	transformer Transformer
	loader      BatchLoader
	logger      *slog.Logger
	metrics     *observability.Metrics
	ready       atomic.Bool
	batchSize   int
}

// New creates a Pipeline with the given stages and observability.
func New(e BatchExtractor, t Transformer, l BatchLoader, logger *slog.Logger, metrics *observability.Metrics, batchSize int) *Pipeline {
	return &Pipeline{
		extractor:   e,
		transformer: t,
		loader:      l,
		logger:      logger,
		metrics:     metrics,
		batchSize:   batchSize,
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

// Run executes the batch ETL loop until the context is cancelled.
func (p *Pipeline) Run(ctx context.Context) error {
	p.logger.Info("pipeline started", "batch_size", p.batchSize)
	p.metrics.PipelineRunning.Set(1)
	defer p.metrics.PipelineRunning.Set(0)

	// Exponential backoff: start at 200ms, double each retry, cap at 5s.
	// Keeps retry storms short while avoiding tight loops during Kafka outages.
	backoff := 200 * time.Millisecond
	maxBackoff := 5 * time.Second

	for {
		select {
		case <-ctx.Done():
			p.logger.Info("pipeline stopping", "reason", ctx.Err())
			return nil
		default:
		}

		if !p.processBatch(ctx, &backoff, maxBackoff) {
			return nil
		}
	}
}

// processBatch runs one extract-transform-load cycle. Returns false if the pipeline should stop.
func (p *Pipeline) processBatch(ctx context.Context, backoff *time.Duration, maxBackoff time.Duration) bool {
	start := time.Now()

	rawBatch, err := p.extractor.ExtractBatch(ctx, p.batchSize)
	if err != nil {
		if ctx.Err() != nil {
			return false
		}
		p.logger.Error("extract batch failed", "error", err)
		return p.backoffOrStop(ctx, backoff, maxBackoff)
	}

	if len(rawBatch) == 0 {
		return ctx.Err() == nil
	}

	p.metrics.MessagesConsumed.Add(float64(len(rawBatch)))
	p.metrics.BatchSize.Observe(float64(len(rawBatch)))
	*backoff = 200 * time.Millisecond

	loaded, ok := p.transformAndLoad(ctx, rawBatch, backoff, maxBackoff)
	if !ok {
		return false
	}

	if loaded > 0 {
		p.metrics.BatchProcessingDuration.Observe(time.Since(start).Seconds())
		p.ready.Store(true)
	}
	return true
}

// transformAndLoad transforms each message in the batch, loads the successes,
// and commits offsets. Returns the number of successfully loaded messages and
// false if the pipeline should stop.
func (p *Pipeline) transformAndLoad(ctx context.Context, rawBatch []domain.RawEvent, backoff *time.Duration, maxBackoff time.Duration) (int, bool) {
	outBatch := make([]domain.OutputEvent, 0, len(rawBatch))
	successfulRaws := make([]domain.RawEvent, 0, len(rawBatch))

	for _, raw := range rawBatch {
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
		outBatch = append(outBatch, out)
		successfulRaws = append(successfulRaws, raw)
	}

	if len(outBatch) == 0 {
		return 0, true
	}

	if err := p.loader.LoadBatch(ctx, outBatch); err != nil {
		p.logger.Error("load batch failed", "error", err, "batch_size", len(outBatch))
		return 0, p.backoffOrStop(ctx, backoff, maxBackoff)
	}

	p.metrics.MessagesProduced.Add(float64(len(outBatch)))

	for _, raw := range successfulRaws {
		p.commitOffset(ctx, raw)
	}

	return len(outBatch), true
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

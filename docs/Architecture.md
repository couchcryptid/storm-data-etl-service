# Architecture

## Overview

```
             ┌──────────────────────────────────────┐
             │           internal/pipeline           │
  Kafka ────>│  BatchExtractor  Transformer  Loader  │────> Kafka
  (raw)      │       │              │          │     │      (enriched)
             └───────┼──────────────┼──────────┼─────┘
                     │              │          │
              adapter/kafka   domain/       adapter/kafka
              (reader.go)   (transform.go)  (writer.go)
```

The Storm Data ETL Service is a single-binary Go application that reads raw storm reports from Kafka, enriches them, and writes the results back to Kafka. It follows a hexagonal (ports and adapters) architecture.

## Package Layout

### `cmd/etl`

Application entry point. Wires together configuration, adapters, pipeline stages, and the HTTP server. Manages signal-based graceful shutdown.

### `internal/domain`

Pure domain logic with no infrastructure dependencies.

- **`event.go`** -- Domain types: `RawEvent`, `StormEvent`, `OutputEvent`, `Location`, `Geo`
- **`transform.go`** -- All transformation and enrichment functions: parsing, normalization, severity derivation, location parsing, serialization
- **`clock.go`** -- Swappable clock for deterministic testing

### `internal/pipeline`

Orchestration layer that defines the ETL interfaces and loop.

- **`pipeline.go`** -- `BatchExtractor`, `Transformer`, and `BatchLoader` interfaces. The `Pipeline` struct runs the continuous extract-transform-load loop with batch processing and backoff on failure.
- **`transform.go`** -- `StormTransformer` adapts domain functions to the `Transformer` interface. Calls `EnrichStormEvent` to apply all enrichment steps.

### `internal/adapter/kafka`

Kafka infrastructure adapters that directly implement the pipeline's `BatchExtractor` and `BatchLoader` interfaces.

- **`reader.go`** -- Wraps `segmentio/kafka-go` Reader with explicit offset commit (consumer group mode) and time-bounded batch extraction. Implements `pipeline.BatchExtractor`.
- **`writer.go`** -- Wraps `segmentio/kafka-go` Writer with `RequireAll` acks and batch writes. Implements `pipeline.BatchLoader`.

### `internal/adapter/httpadapter`

HTTP server for operational endpoints.

- `/healthz` -- Liveness: always 200
- `/readyz` -- Readiness: 200 after at least one message processed, 503 otherwise
- `/metrics` -- Prometheus handler

### `internal/observability`

- **`logging.go`** -- Thin wrapper that delegates to [storm-data-shared](https://github.com/couchcryptid/storm-data-shared) `observability.NewLogger()` for structured `slog` logging
- **`metrics.go`** -- Prometheus counter, histogram, and gauge definitions for pipeline observability

### `internal/config`

Environment-based configuration. Uses shared parsers from [storm-data-shared](https://github.com/couchcryptid/storm-data-shared) (`ParseShutdownTimeout`, `ParseBatchSize`, `ParseBatchFlushInterval`, `EnvOrDefault`, `ParseBrokers`) combined with ETL-specific settings (Kafka topics).

## Design Decisions

### Hexagonal Architecture

Infrastructure adapters (Kafka, HTTP) are separated from domain logic via interfaces defined in the `pipeline` package. This allows:

- Unit testing the pipeline with mock batch extractors, transformers, and loaders
- Swapping Kafka for another broker without touching domain or pipeline code

### Explicit Offset Commit

The Kafka reader uses `FetchMessage` + manual `CommitMessages` rather than auto-commit. Offsets are committed only after the message has been successfully transformed and loaded, providing at-least-once delivery semantics.

### Backoff Strategy

The pipeline uses exponential backoff (200ms to 5s) on extract or load failures via [storm-data-shared](https://github.com/couchcryptid/storm-data-shared) `retry.NextBackoff()` and `retry.SleepWithContext()`. Backoff resets immediately after a successful extract.

### Graceful Shutdown

The main function uses `signal.NotifyContext` to capture `SIGINT`/`SIGTERM`. On shutdown:

1. The pipeline loop exits via context cancellation
2. The HTTP server drains connections within the configured timeout
3. Kafka reader and writer are closed

### Thread Safety

The `Pipeline.ready` flag uses `atomic.Bool` since it is written by the pipeline goroutine and read by the HTTP readiness handler concurrently.

### Batch Processing

The pipeline extracts, transforms, and loads messages in configurable batches (`BATCH_SIZE`, `BATCH_FLUSH_INTERVAL`). The `BatchExtractor` fetches up to N messages within a time window; the `BatchLoader` writes the entire batch in one call.

**Why**: Batch writes amortize Kafka producer overhead. Time-bounded fetching ensures partial batches flush promptly rather than blocking indefinitely for a full batch. The transform step remains per-message since enrichment logic is stateless and doesn't benefit from batching.

### Deterministic IDs

Event IDs are SHA-256 hashes of `type|state|lat|lon|time`. The same raw event always produces the same ID, regardless of how many times it is processed.

**Why**: Enables idempotent writes at every downstream stage. The API's `ON CONFLICT (id) DO NOTHING` naturally deduplicates without coordination. No distributed ID generation or sequence allocation needed.

### Consumer-Defined Interfaces

The `BatchExtractor`, `Transformer`, and `BatchLoader` interfaces are defined in the `pipeline` package (the consumer), not in the adapter packages that implement them.

**Why**: Follows Go's convention of defining interfaces where they are used. The pipeline package declares what it needs; adapters satisfy those contracts. This keeps the pipeline testable with in-memory implementations and avoids import cycles.

### Poison Pill Handling

Malformed messages are logged, their offsets committed, and processing continues with the next message.

**Why**: A single bad message should not block the entire pipeline. Committing the offset prevents the poison pill from being redelivered indefinitely. The warning log provides visibility for investigation.

## Capacity

SPC data volumes are small (~1,000--5,000 records/day during storm season). The pipeline processes an entire day's data in seconds. At ~11--100 messages/second throughput, the service is over-provisioned by orders of magnitude for expected load. The 256 MB container memory limit provides 5--8x headroom over the ~30--50 MB steady-state footprint.

For horizontal scaling, deploy multiple instances with Kafka consumer groups (`KAFKA_GROUP_ID`). Throughput scales linearly up to the source topic partition count.

## Configuration

All configuration is via environment variables with defaults suitable for local development.

| Variable | Default | Description |
| -------- | ------- | ----------- |
| `KAFKA_BROKERS` | `localhost:9092` | Comma-separated Kafka broker addresses |
| `KAFKA_SOURCE_TOPIC` | `raw-weather-reports` | Topic to consume raw storm reports from |
| `KAFKA_SINK_TOPIC` | `transformed-weather-data` | Topic to produce enriched events to |
| `KAFKA_GROUP_ID` | `storm-data-etl` | Consumer group ID |
| `HTTP_ADDR` | `:8080` | Health/metrics HTTP server address |
| `LOG_LEVEL` | `info` | `debug`, `info`, `warn`, `error` |
| `LOG_FORMAT` | `json` | `json` or `text` |
| `SHUTDOWN_TIMEOUT` | `10s` | Graceful shutdown deadline |
| `BATCH_SIZE` | `50` | Messages per batch (1--1000) |
| `BATCH_FLUSH_INTERVAL` | `500ms` | Max wait before flushing a partial batch |

Loaded and validated in `internal/config/config.go`. Fails fast on empty broker list, empty topics, or invalid durations. Shared parsers from [storm-data-shared](https://github.com/couchcryptid/storm-data-shared) handle `BATCH_SIZE`, `BATCH_FLUSH_INTERVAL`, `SHUTDOWN_TIMEOUT`, and `KAFKA_BROKERS`.

## Related

- [System Architecture](https://github.com/couchcryptid/storm-data-system/wiki/Architecture) -- full pipeline design, deployment topology, and improvement roadmap
- [Collector Architecture](https://github.com/couchcryptid/storm-data-collector/wiki/Architecture) -- upstream service that publishes raw CSV events to Kafka
- [API Architecture](https://github.com/couchcryptid/storm-data-api/wiki/Architecture) -- downstream consumer of enriched events
- [Shared Architecture](https://github.com/couchcryptid/storm-data-shared/wiki/Architecture) -- shared library packages used by the ETL
- [[Enrichment]] -- severity classification, location parsing, and enrichment rules
- [[Development]] -- build, test, lint, CI, and project conventions

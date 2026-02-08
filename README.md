# Storm Data ETL Service

A Go service that consumes raw weather storm reports from a Kafka topic, enriches and normalizes the data, and produces transformed events to a downstream Kafka topic. Built for the HailTrace platform.

## How It Works

The service runs a continuous ETL loop:

1. **Extract** -- Consumes JSON storm reports from a Kafka source topic
2. **Transform** -- Parses, normalizes, and enriches each event (severity classification, location parsing, unit normalization, optional geocoding via Mapbox)
3. **Load** -- Produces the enriched event to a Kafka sink topic

Supported event types: **hail**, **wind**, and **tornado**.

## Quickstart

### Prerequisites

- Go 1.25+
- Docker and Docker Compose

### Run locally with Docker Compose

```sh
cp .env.example .env
docker compose up --build
```

This starts Zookeeper, Kafka, and the ETL service. The service begins consuming from the `raw-weather-reports` topic and producing to `transformed-weather-data`.

### Run without Docker

```sh
cp .env.example .env
make build
./bin/etl
```

Requires a running Kafka broker accessible at the address configured in `.env`.

## Configuration

All configuration is via environment variables (loaded from `.env` in Docker Compose):

| Variable             | Default                    | Description                                    |
| -------------------- | -------------------------- | ---------------------------------------------- |
| `KAFKA_BROKERS`      | `localhost:9092`           | Comma-separated list of Kafka broker addresses |
| `KAFKA_SOURCE_TOPIC` | `raw-weather-reports`      | Topic to consume raw storm reports from        |
| `KAFKA_SINK_TOPIC`   | `transformed-weather-data` | Topic to produce enriched events to            |
| `KAFKA_GROUP_ID`     | `storm-data-etl`           | Consumer group ID                              |
| `HTTP_ADDR`          | `:8080`                    | Address for the health/metrics HTTP server     |
| `LOG_LEVEL`          | `info`                     | Log level: `debug`, `info`, `warn`, `error`    |
| `LOG_FORMAT`         | `json`                     | Log format: `json` or `text`                   |
| `SHUTDOWN_TIMEOUT`   | `10s`                      | Graceful shutdown deadline                     |
| `MAPBOX_TOKEN`       | *(none)*                   | Mapbox API token; auto-enables geocoding if set |
| `MAPBOX_ENABLED`     | auto-detected              | Explicit override (`true`/`false`) for geocoding |
| `MAPBOX_TIMEOUT`     | `5s`                       | HTTP timeout for Mapbox API requests           |
| `MAPBOX_CACHE_SIZE`  | `1000`                     | Max entries in the geocoding LRU cache         |

## HTTP Endpoints

| Endpoint       | Description                                                                            |
| -------------- | -------------------------------------------------------------------------------------- |
| `GET /healthz` | Liveness probe -- always returns `200`                                                 |
| `GET /readyz`  | Readiness probe -- returns `200` after the first message is processed, `503` otherwise |
| `GET /metrics` | Prometheus metrics                                                                     |

## Prometheus Metrics

| Metric                            | Type      | Description                                   |
| --------------------------------- | --------- | --------------------------------------------- |
| `etl_messages_consumed_total`     | Counter   | Messages read from the source topic           |
| `etl_messages_produced_total`     | Counter   | Messages written to the sink topic            |
| `etl_transform_errors_total`      | Counter   | Transformation failures (malformed input)     |
| `etl_processing_duration_seconds` | Histogram | Duration of each extract-transform-load cycle |
| `etl_pipeline_running`            | Gauge     | `1` when the pipeline loop is active          |

## Make Targets

```
make build        # Build binary to bin/etl
make run          # Run with go run
make test         # Run unit + integration tests
make test-unit    # Run unit tests with race detector
make test-integration # Run integration tests (Docker required)
make test-cover   # Run tests and open HTML coverage report
make lint         # Run golangci-lint
make fmt          # Format code with gofmt and goimports
make clean        # Remove build artifacts
```

Integration tests require Docker because they use a Kafka container.

## Project Structure

```
cmd/etl/                    Entry point
internal/
  adapter/
    http/                   Health, readiness, and metrics HTTP server
    kafka/                  Kafka reader (consumer) and writer (producer)
    mapbox/                 Mapbox geocoding client with LRU cache
  config/                   Environment-based configuration
  domain/                   Domain types, transformation logic, and geocoding
  integration/              Integration tests (require Docker)
  observability/            Structured logging and Prometheus metrics
  pipeline/                 ETL orchestration (extract, transform, load)
data/mock/                  Sample storm report JSON for testing
docs/                       Wiki source (synced to GitHub Wiki)
```

## Documentation

See the [project wiki](../../wiki) for detailed documentation:

- [Architecture](../../wiki/Architecture) -- System design and data flow
- [Enrichment Rules](../../wiki/Enrichment) -- Transformation and severity classification logic
- [Deployment](../../wiki/Deployment) -- Docker Compose and production deployment
- [Development](../../wiki/Development) -- Developer workflow, testing, and CI
- [Performance](../../wiki/Performance) -- Theoretical throughput, scaling, and bottleneck analysis

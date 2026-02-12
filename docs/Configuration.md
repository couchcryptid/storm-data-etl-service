# Configuration

All configuration is via environment variables. Every variable has a default suitable for local development with the Docker Compose stack.

## Environment Variables

| Variable             | Default                    | Description                                    |
| -------------------- | -------------------------- | ---------------------------------------------- |
| `KAFKA_BROKERS`      | `localhost:9092`           | Comma-separated list of Kafka broker addresses |
| `KAFKA_SOURCE_TOPIC` | `raw-weather-reports`      | Topic to consume raw storm reports from        |
| `KAFKA_SINK_TOPIC`   | `transformed-weather-data` | Topic to produce enriched events to            |
| `KAFKA_GROUP_ID`     | `storm-data-etl`           | Consumer group ID                              |
| `HTTP_ADDR`          | `:8080`                    | Address for the health/metrics HTTP server     |
| `LOG_LEVEL`          | `info`                     | Log level: `debug`, `info`, `warn`, `error`    |
| `LOG_FORMAT`         | `json`                     | Log format: `json` or `text`                   |
| `SHUTDOWN_TIMEOUT`   | `10s`                      | Graceful shutdown deadline (Go duration)       |
| `BATCH_SIZE`         | `50`                       | Messages per batch (1--1000)                   |
| `BATCH_FLUSH_INTERVAL` | `500ms`                  | Max wait before flushing a partial batch (Go duration) |

## Shared Parsers

Several environment variables use parsers from the [storm-data-shared](https://github.com/couchcryptid/storm-data-shared/wiki/Configuration) library:

| Variable | Shared Parser |
|----------|--------------|
| `KAFKA_BROKERS` | `config.ParseBrokers()` |
| `BATCH_SIZE` | `config.ParseBatchSize()` |
| `BATCH_FLUSH_INTERVAL` | `config.ParseBatchFlushInterval()` |
| `SHUTDOWN_TIMEOUT` | `config.ParseShutdownTimeout()` |
| `LOG_LEVEL`, `LOG_FORMAT` | `observability.NewLogger()` |

ETL-specific variables (`KAFKA_SOURCE_TOPIC`, `KAFKA_SINK_TOPIC`) are parsed in `internal/config/config.go`.

## Validation

Configuration is loaded and validated in `internal/config/config.go`. The `Load()` function returns `(*Config, error)` and fails fast on:

- Empty `KAFKA_BROKERS`
- Empty `KAFKA_SOURCE_TOPIC` or `KAFKA_SINK_TOPIC`
- Invalid `SHUTDOWN_TIMEOUT` (must be a valid Go duration, e.g. `10s`, `1m`)

## Docker Compose Environment Files

The Compose stack uses per-service env files to keep configuration out of `compose.yml`:

| File         | Service          | Contents                                           |
| ------------ | ---------------- | -------------------------------------------------- |
| `.env.example` | Reference      | Template for the ETL service config                |
| `.env`       | `storm-data-etl` | Actual ETL config (gitignored)                     |
| `.env.kafka` | `kafka`          | Kafka KRaft broker settings (listeners, controller, replication) |

## Local Development

```sh
cp .env.example .env
docker compose up --build
```

The defaults connect to the Docker Compose Kafka instance at `localhost:9092`.

## Custom Configuration

Override any variable as needed:

```sh
LOG_LEVEL=debug make run
```

## Related

- [Shared Configuration](https://github.com/couchcryptid/storm-data-shared/wiki/Configuration) -- shared parsers for Kafka, batch, and shutdown settings
- [Collector Configuration](https://github.com/couchcryptid/storm-data-collector/wiki/Configuration) -- upstream Kafka producer settings
- [[Architecture]] -- design decisions and hexagonal architecture
- [[Enrichment]] -- enrichment pipeline and severity classification
- [[Development]] -- build, test, lint, CI, and project conventions

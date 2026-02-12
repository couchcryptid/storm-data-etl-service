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

### Mapbox Geocoding (Feature-Flagged)

Geocoding is an optional enrichment step. Setting `MAPBOX_TOKEN` automatically enables it.

| Variable           | Default        | Description                                    |
| ------------------ | -------------- | ---------------------------------------------- |
| `MAPBOX_TOKEN`     | *(none)*       | Mapbox API token; auto-enables geocoding if set |
| `MAPBOX_ENABLED`   | auto-detected  | Explicit override (`true`/`false`) for geocoding |
| `MAPBOX_TIMEOUT`   | `5s`           | HTTP timeout for Mapbox API requests           |
| `MAPBOX_CACHE_SIZE`| `1000`         | Max entries in the geocoding LRU cache         |

**Feature flag behavior:**

- If `MAPBOX_TOKEN` is set and `MAPBOX_ENABLED` is unset, geocoding is enabled automatically
- Set `MAPBOX_ENABLED=false` to disable geocoding even when a token is present
- Setting `MAPBOX_ENABLED=true` without a token returns a startup error

## Shared Parsers

Several environment variables use parsers from the [storm-data-shared](https://github.com/couchcryptid/storm-data-shared/wiki/Configuration) library:

| Variable | Shared Parser |
|----------|--------------|
| `KAFKA_BROKERS` | `config.ParseBrokers()` |
| `BATCH_SIZE` | `config.ParseBatchSize()` |
| `BATCH_FLUSH_INTERVAL` | `config.ParseBatchFlushInterval()` |
| `SHUTDOWN_TIMEOUT` | `config.ParseShutdownTimeout()` |
| `LOG_LEVEL`, `LOG_FORMAT` | `observability.NewLogger()` |

ETL-specific variables (`KAFKA_SOURCE_TOPIC`, `KAFKA_SINK_TOPIC`, `MAPBOX_*`) are parsed in `internal/config/config.go`.

## Validation

Configuration is loaded and validated in `internal/config/config.go`. The `Load()` function returns `(*Config, error)` and fails fast on:

- Empty `KAFKA_BROKERS`
- Empty `KAFKA_SOURCE_TOPIC` or `KAFKA_SINK_TOPIC`
- Invalid `SHUTDOWN_TIMEOUT` or `MAPBOX_TIMEOUT` (must be valid Go durations, e.g. `10s`, `1m`)
- `MAPBOX_ENABLED=true` without `MAPBOX_TOKEN`

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
LOG_LEVEL=debug MAPBOX_TOKEN=pk.xxx make run
```

## Related

- [System Configuration](https://github.com/couchcryptid/storm-data-system/wiki/Configuration) -- environment variables across all services
- [Shared Configuration](https://github.com/couchcryptid/storm-data-shared/wiki/Configuration) -- shared parsers for Kafka, batch, and shutdown settings
- [Collector Configuration](https://github.com/couchcryptid/storm-data-collector/wiki/Configuration) -- upstream Kafka producer settings
- [[Architecture]] -- design decisions and hexagonal architecture
- [[Enrichment]] -- geocoding feature flag behavior and Mapbox settings
- [[Deployment]] -- Docker Compose environment files

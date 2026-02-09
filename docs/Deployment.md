# Deployment

## Docker Compose (Local Development)

The `compose.yml` at the repo root runs the full stack: Kafka (KRaft mode) and the ETL service.

### Start

```sh
cp .env.example .env
docker compose up --build
```

### Stop

```sh
docker compose down
```

To remove volumes (Kafka data):

```sh
docker compose down -v
```

### Services

| Service | Image | Port | Description |
|---|---|---|---|
| `kafka` | `apache/kafka:3.7.0` | 29092 | Message broker (KRaft, no Zookeeper) |
| `storm-data-etl` | Built from `Dockerfile` | 8080 | ETL service |

### Health Checks

All services have health checks configured:

- **Kafka**: `kafka-topics --list` against the internal listener
- **ETL**: HTTP `GET /healthz`

Services start in dependency order: Kafka -> ETL (waits for healthy Kafka).

### Resource Limits

| Service | Memory Limit | Memory Reservation |
|---|---|---|
| Kafka | 1 GB | 512 MB |
| ETL | 256 MB | 128 MB |

## Docker Image

The service uses a multi-stage build:

1. **Build stage** (`golang:1.25-alpine`): Compiles the binary with `-ldflags="-s -w"` for a smaller output
2. **Runtime stage** (`gcr.io/distroless/static-debian12`): Minimal image with no shell or package manager

The final image contains only the binary and CA certificates.

### Build Manually

```sh
docker build -t storm-data-etl .
```

### Run Standalone

```sh
docker run -p 8080:8080 \
  -e KAFKA_BROKERS=host.docker.internal:9092 \
  -e KAFKA_SOURCE_TOPIC=raw-weather-reports \
  -e KAFKA_SINK_TOPIC=transformed-weather-data \
  storm-data-etl
```

## Environment Files

| File | Used By | Description |
|---|---|---|
| `.env.example` | Reference | Template for the ETL service config (includes Mapbox geocoding vars) |
| `.env` | `storm-data-etl` container | Actual ETL config (gitignored) |
| `.env.kafka` | `kafka` container | Kafka KRaft broker settings (listeners, controller, replication) |

### Kafka Environment (.env.kafka)

Key settings:

- `KAFKA_ADVERTISED_LISTENERS`: Exposes both an internal listener (`kafka:9092` for inter-container traffic) and a host-accessible listener (`localhost:29092` for host access)
- `KAFKA_AUTO_CREATE_TOPICS_ENABLE=true`: Topics are auto-created when the service starts consuming/producing
- `KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1`: Single-node setup for local development

## Production Considerations

- Replace the single-node Kafka setup with a managed Kafka service or multi-broker cluster
- Set `KAFKA_AUTO_CREATE_TOPICS_ENABLE=false` and pre-create topics with appropriate partition counts and replication factors
- Configure `KAFKA_GROUP_ID` per environment to isolate consumer groups
- Set `LOG_FORMAT=json` for structured log aggregation
- Monitor the Prometheus metrics endpoint with your observability stack
- Consider running multiple ETL instances for horizontal scaling (Kafka consumer groups handle partition assignment automatically)
- **Geocoding (Mapbox)**: Set `MAPBOX_TOKEN` to enable geocoding enrichment. Be aware of Mapbox API rate limits and pricing. The LRU cache (`MAPBOX_CACHE_SIZE`, default 1000) reduces redundant calls for frequently seen locations. Set `MAPBOX_TIMEOUT` appropriately for your network latency to the Mapbox API. Geocoding failures are non-fatal -- the pipeline continues without geocoded data.

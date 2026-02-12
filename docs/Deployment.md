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

## Production

For cloud deployment options and cost analysis, see the [system Architecture wiki](https://github.com/couchcryptid/storm-data-system/wiki/Architecture#gcp-cloud-cost-analysis).

## Related

- [System Deployment](https://github.com/couchcryptid/storm-data-system/wiki/Deployment) -- full-stack Docker Compose with all services
- [System Architecture](https://github.com/couchcryptid/storm-data-system/wiki/Architecture) -- cloud cost analysis and deployment topology
- [[Configuration]] -- environment variables, feature flags, and env files
- [[Development]] -- local development setup and testing

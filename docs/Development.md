# Development

## Prerequisites

- Go 1.25+
- Docker and Docker Compose (for integration tests and local Kafka)
- [golangci-lint](https://golangci-lint.run/) (for linting)
- [pre-commit](https://pre-commit.com/) (optional, for git hooks)

## Setup

```sh
git clone <repo-url>
cd storm-data-etl
cp .env.example .env
```

Install pre-commit hooks (optional):

```sh
pre-commit install
```

## Build

```sh
make build
```

Output binary: `bin/etl`

## Testing

### Unit Tests

```sh
make test-unit
```

Runs unit tests with the race detector enabled (`-race -count=1`).

### Coverage

```sh
make test-cover
```

Generates `coverage.out` and opens an HTML coverage report in the browser.

### Integration Tests

Integration tests use [testcontainers-go](https://github.com/testcontainers/testcontainers-go) to spin up Kafka and verify end-to-end message flow.

```sh
make test-integration
```

These tests require Docker to be running and may take 1-2 minutes to start the containers.

### Test Data

Sample storm report JSON files live in `data/mock/`. These are used by the `TestStormTransformer_WithMockJSONData` test to verify transformation against realistic data for all three event types (hail, tornado, wind).

## Linting

```sh
make lint
```

Uses `golangci-lint` with the configuration in `.golangci.yml`. Enabled linters include:

- `errcheck`, `govet`, `staticcheck` -- correctness
- `gosec` -- security
- `gocyclo` -- complexity (threshold: 15)
- `revive`, `gocritic` -- style
- `gofmt`, `goimports` -- formatting
- `misspell`, `unparam` -- hygiene

## Formatting

```sh
make fmt
```

Runs `gofmt` and `goimports` across the project.

## Pre-commit Hooks

The `.pre-commit-config.yaml` configures hooks that run on every commit:

- Trailing whitespace removal
- End-of-file newline
- YAML and JSON validation
- Merge conflict markers
- `yamllint`
- `markdownlint`
- `gofmt` and `goimports`
- `golangci-lint`

## CI Pipeline

The `.github/workflows/ci.yml` workflow runs on pushes and pull requests to `main`:

| Job | What It Does |
|---|---|
| `test-unit` | `make test-unit` (unit tests with race detector) |
| `lint` | `make lint` (golangci-lint with the project config) |
| `build` | `make build` (compile check) |

A separate `release.yml` workflow (triggered by CI success on `main`) handles versioning, GitHub releases, and Docker image publishing.

## Project Conventions

- **Interfaces defined by consumers**: The `BatchExtractor`, `Transformer`, and `BatchLoader` interfaces are defined in the `pipeline` package (which uses them), not in the adapter packages that implement them. The Kafka adapters satisfy these interfaces directly.
- **Domain logic is pure**: The `domain` package has no infrastructure imports. Transformation functions are stateless and operate on domain types.
- **Testable time**: The `clock` package variable (via `clockwork`) allows tests to control time without relying on `time.Sleep` or wall-clock assertions.
- **Structured logging**: All logging uses `log/slog` with key-value pairs. The pipeline logs include Kafka metadata (topic, partition, offset) for traceability.
- **Feature flags via environment**: Optional features follow the `FOO_ENABLED` + `FOO_TOKEN` pattern. Setting `MAPBOX_TOKEN` auto-enables geocoding; `MAPBOX_ENABLED` provides an explicit override. The `nil` geocoder path is always safe.
- **Domain interfaces for external services**: The `domain.Geocoder` interface defines the geocoding port. The Mapbox adapter implements it, and a `nil` geocoder disables the feature. Pass `nil` for the geocoder and `slog.Default()` for the logger in tests that don't need geocoding.
- **Adapter constructor injection**: All adapters (Kafka, HTTP, Mapbox) accept `*slog.Logger` via their constructors for consistent, testable logging.

## Related

- [System Development](https://github.com/couchcryptid/storm-data-system/wiki/Development) -- multi-repo workflow, CI conventions, and cross-service patterns
- [System Testing](https://github.com/couchcryptid/storm-data-system/wiki/Testing) -- E2E and UAT tests that validate the full pipeline
- [Shared Development](https://github.com/couchcryptid/storm-data-shared/wiki/Development) -- shared library development and versioning
- [[Configuration]] -- environment variables and feature flags
- [[Architecture]] -- hexagonal design, batch processing, and offset strategy
- [[Enrichment]] -- severity classification, location parsing, and geocoding
- [[Code Quality]] -- linting, static analysis, and quality gates

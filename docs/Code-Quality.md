# Code Quality

Quality philosophy, tooling, and enforcement for the ETL service. For pipeline-wide quality standards, see the [system Code Quality page](https://github.com/couchcryptid/storm-data-system/wiki/Code-Quality).

## Coding Philosophy

### Hexagonal architecture

Infrastructure adapters (Kafka, HTTP, Mapbox) are separated from domain logic via interfaces defined in the `pipeline` package. The domain layer has no knowledge of Kafka, HTTP, or any external system. This allows unit testing the pipeline with mock extractors, transformers, and loaders, and swapping infrastructure without touching business logic.

### Consumer-defined interfaces

`BatchExtractor`, `Transformer`, and `BatchLoader` are defined in the `pipeline` package -- where they're used -- not in the adapter packages that implement them. The Kafka reader and writer satisfy these interfaces directly. Go's structural typing makes this seamless.

### Pure domain logic

The `domain` package has no infrastructure imports. Transformation functions are stateless and operate on domain types: severity classification, location parsing, unit normalization, and time bucketing are all pure functions. Same input always produces the same output.

### Stateless transforms

Enrichment functions are pure: they take a raw event and return an enriched event. No side effects, no shared mutable state, no database calls. This makes them trivially testable and safe to run concurrently.

### Testable time

The `clock` package variable (via `clockwork`) allows tests to control time deterministically. No `time.Sleep`, no wall-clock assertions. Tests set the clock to a known value and assert against it.

### Feature flags via environment

Optional features follow the `FOO_ENABLED` + `FOO_TOKEN` pattern. Setting `MAPBOX_TOKEN` auto-enables geocoding; `MAPBOX_ENABLED` provides an explicit override. The `nil` geocoder path is always safe -- no special casing needed in the pipeline.

### Domain interfaces for external services

The `domain.Geocoder` interface defines the geocoding port. The Mapbox adapter implements it. Passing `nil` disables the feature. Tests that don't need geocoding pass `nil` and `slog.Default()` -- no mocking framework required.

### Constructor injection

All adapters accept `*slog.Logger` and configuration via their constructors. No global state. Every component is testable in isolation.

## What the Codebase Reveals

The ETL's code tells a story about where complexity lives and how it's managed.

### Domain logic earns its isolation

The `domain` package has zero infrastructure imports. Every enrichment function is pure: `DeriveSeverity`, `ParseLocation`, `NormalizeMagnitude`, `EnrichWithGeocoding` all take a `StormEvent` and return a `StormEvent`. This isn't incidental -- it's the reason the enrichment pipeline can be tested with a single `go test` in under a second, with no Docker, no Kafka, no network calls. Changes to business rules (new severity thresholds, new location formats) are fast to implement and fast to verify.

### The hexagonal boundary is real, not ceremonial

`BatchExtractor`, `Transformer`, and `BatchLoader` aren't just interface definitions -- they're the reason the integration test can spin up real Kafka via testcontainers and validate the full pipeline without mocking. The adapter layer is thin (Kafka reader/writer) and the domain layer does the real work. This means infrastructure changes (upgrading kafka-go, adding a new transport) don't touch business logic, and business logic changes don't require infrastructure.

### Feature flags through nil

Geocoding is enabled by passing a `Geocoder` implementation and disabled by passing `nil`. No boolean flags, no config checks in the pipeline -- just a nil check in `EnrichWithGeocoding`. This pattern means the pipeline always runs the same code path; the only difference is whether the geocoder does real work or short-circuits. Changes to enable/disable geocoding require zero code changes in the pipeline itself.

### Graceful degradation is a first-class concern

The ETL never blocks on enrichment failure. Malformed messages are logged and skipped. Geocoding API errors produce `source: "failed"` but the event continues through the pipeline. Magnitude normalization handles legacy hundredths format silently. This reflects a design where data throughput is prioritized over data perfection -- better to have a record with empty geocoding than to lose the record entirely.

## Static Analysis

### golangci-lint

14 enabled linters covering correctness, security, style, and performance:

| Category | Linters |
|----------|---------|
| Correctness | `errcheck`, `govet`, `staticcheck`, `errorlint`, `exhaustive` |
| Security | `gosec`, `noctx`, `bodyclose` |
| Style | `gocritic` (diagnostic/style/performance), `revive` (exported) |
| Complexity | `gocyclo` (threshold: 15) |
| Hygiene | `misspell`, `unparam`, `errname`, `unconvert`, `prealloc` |
| Test quality | `testifylint` |

`bodyclose` is enabled here (unlike the shared library) because the ETL makes HTTP calls to the Mapbox geocoding API.

### SonarQube Cloud

Analyzed via CI on every push and pull request: [SonarCloud dashboard](https://sonarcloud.io/summary/overall?id=couchcryptid_storm-data-etl)

SonarCloud configuration (`sonar-project.properties`):

- Reports Go coverage via `coverage.out`
- Allows idiomatic Go test naming (`TestX_Y_Z`) on test files

## Security

| Layer | What It Catches |
|-------|----------------|
| `gosec` | Security-sensitive patterns, weak crypto, hardcoded credentials |
| `noctx` | HTTP requests without context (timeout/cancellation) |
| `bodyclose` | Unclosed HTTP response bodies (Mapbox API calls) |
| `gitleaks` | Secrets in source code |
| Poison pill handling | Malformed messages logged and skipped, never block the pipeline |

## Quality Gates

### Pre-commit Hooks

`.pre-commit-config.yaml` runs on every commit:

- File hygiene: trailing whitespace, end-of-file newline, YAML/JSON validation
- Formatting: `gofmt`, `goimports`
- Linting: `golangci-lint` (5-minute timeout)
- Security: `gitleaks`, `detect-private-key`, `check-added-large-files`
- Documentation: `yamllint`, `markdownlint`

### CI Pipeline

Every push and pull request to `main` runs:

| Job | Command | What It Enforces |
|-----|---------|-----------------|
| `test-unit` | `make test-unit` | Unit tests with race detector (`-race -count=1`) |
| `lint` | `make lint` | golangci-lint with 14 linters |
| `build` | `make build` | Compile check |
| `sonarcloud` | SonarCloud scan | Coverage, bugs, vulnerabilities, code smells, security hotspots |

### SonarCloud Quality Gate

Uses the default "Sonar way" gate on new code: >= 80% coverage, <= 3% duplication, A ratings for reliability/security/maintainability, 100% security hotspots reviewed.

## Testing

Tests are organized in two tiers. See [[Development]] for commands.

| Tier | What It Covers | Docker |
|------|---------------|--------|
| Unit | Transformation logic, severity classification, location parsing, time bucketing | No |
| Integration | Full extract-transform-load pipeline with real Kafka (testcontainers) | Yes |

All tests run with `-race -count=1`. Uses `clockwork` for deterministic time control. Mock data in `data/mock/` provides realistic test fixtures for all three event types.

## Related

- [System Code Quality](https://github.com/couchcryptid/storm-data-system/wiki/Code-Quality) -- pipeline-wide quality standards
- [Shared Library Code Quality](https://github.com/couchcryptid/storm-data-shared/wiki/Code-Quality) -- shared tooling and conventions
- [[Development]] -- commands, CI pipeline, project conventions
- [[Architecture]] -- hexagonal design, offset strategy, backoff

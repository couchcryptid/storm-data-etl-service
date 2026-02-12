# Storm Data ETL Service

A Go service that consumes raw weather storm reports from Kafka, enriches and normalizes the data, and produces transformed events to a downstream Kafka topic. Part of the storm data pipeline.

## Pipeline Position

```
Collector --> Kafka --> [ETL] --> Kafka --> API --> PostgreSQL + GraphQL
```

**Upstream**: The [collector service](https://github.com/couchcryptid/storm-data-collector/wiki) publishes raw CSV events to the `raw-weather-reports` topic.

**Downstream**: The [API service](https://github.com/couchcryptid/storm-data-api/wiki) consumes enriched events from the `transformed-weather-data` topic.

Uses the [storm-data-shared](https://github.com/couchcryptid/storm-data-shared/wiki) library for logging, health endpoints, config parsing, and retry logic. For the full pipeline architecture, see the [system wiki](https://github.com/couchcryptid/storm-data-system/wiki).

## Pages

- [[Architecture]] -- System design, package layout, design decisions, and capacity
- [[Configuration]] -- Environment variables and validation
- [[Development]] -- Build, test, lint, CI, and project conventions
- [[Enrichment Rules|Enrichment]] -- Transformation pipeline, normalization, and severity classification

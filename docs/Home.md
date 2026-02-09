# Storm Data ETL Service

Welcome to the Storm Data ETL Service wiki.

A Go service that consumes raw weather storm reports from Kafka, enriches and normalizes the data, and produces transformed events to a downstream Kafka topic. Built for the HailTrace platform.

## Pages

- [[Architecture]] -- System design, package layout, and design decisions
- [[Enrichment Rules|Enrichment]] -- Transformation pipeline, normalization, and severity classification
- [[Deployment]] -- Docker Compose setup and production considerations
- [[Development]] -- Build, test, lint, CI, and project conventions
- [[Performance]] -- Theoretical throughput, scaling, and bottleneck analysis

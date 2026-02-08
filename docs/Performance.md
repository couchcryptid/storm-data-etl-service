# Performance

This page describes the theoretical performance characteristics of the Storm Data ETL Service: where time is spent, what the bottlenecks are, and how the system scales.

## Processing Model

The pipeline processes messages **sequentially** in a single goroutine: one extract-transform-load cycle must complete before the next begins. This simplifies ordering guarantees and offset management but means throughput is bounded by the latency of each cycle.

### Per-Message Cost Breakdown

| Stage | Dominant Cost | Typical Latency |
|---|---|---|
| **Extract** | Kafka fetch (network I/O, broker response) | 1--50 ms when messages are available; blocks indefinitely when the topic is idle |
| **Transform** (no geocoding) | JSON unmarshal, regex matching, field normalization, JSON marshal | < 0.1 ms for a typical storm event (~1 KB) |
| **Transform** (with geocoding) | Above + Mapbox API HTTP round-trip (when cache misses) | 50--200 ms on cache miss; < 0.1 ms on cache hit |
| **Load** | Kafka produce with `RequireAll` acks (network round-trip, broker replication) | 5--30 ms depending on broker latency and replication factor |
| **Commit** | Kafka offset commit (network round-trip) | 2--10 ms |

Without geocoding, transform is CPU-bound and negligible compared to the I/O-bound extract, load, and commit stages. **Kafka round-trips dominate end-to-end latency.** With geocoding enabled, cache misses add a Mapbox API round-trip to the transform stage; the LRU cache mitigates this for frequently seen locations.

### Theoretical Single-Instance Throughput

Under ideal conditions (messages always available, no errors, low-latency broker):

- **Per-cycle latency**: ~10--90 ms (extract + transform + load + commit)
- **Throughput**: ~11--100 messages/second per instance
- **Transform ceiling** (CPU only, no I/O): ~100,000+ messages/second (JSON parse + enrich + serialize on a single core)

The gap between the transform ceiling and actual throughput confirms that I/O is the bottleneck, not CPU.

## Scaling

### Horizontal Scaling via Consumer Groups

The service uses Kafka consumer groups (`KAFKA_GROUP_ID`). Deploying N instances of the service automatically distributes partitions across instances:

| Source Partitions | Instances | Partitions per Instance | Theoretical Aggregate Throughput |
|---|---|---|---|
| 1 | 1 | 1 | 11--100 msg/s |
| 6 | 3 | 2 | 33--300 msg/s |
| 12 | 6 | 2 | 66--600 msg/s |
| 12 | 12 | 1 | 132--1,200 msg/s |

**Scaling is limited by the number of source topic partitions.** Instances beyond the partition count sit idle. For higher throughput, increase the partition count on the source topic before adding instances.

### Partition Ordering

Each instance processes its assigned partitions sequentially. Messages within a single partition maintain order. Cross-partition ordering is not guaranteed (nor typically required for independent storm events).

## Memory Profile

| Component | Memory Characteristics |
|---|---|
| **Message buffer** | One message in flight at a time. Bounded by `MaxBytes` (10 MB) on the consumer fetch. |
| **Transform** | Allocates a `StormEvent` struct (~0.5--2 KB) plus JSON marshal output per message. Short-lived, GC-friendly. |
| **Regex** | Two compiled regexes (`sourceOfficeRe`, `locationRe`) allocated once at package init. |
| **Prometheus** | Fixed set of 5 metric collectors. Negligible memory. |
| **Kafka client** | Internal buffers for fetch and produce batching. Typically 5--20 MB per reader/writer. |
| **Geocoding LRU cache** | Thread-safe doubly-linked list + map. Up to `MAPBOX_CACHE_SIZE` (default 1000) entries, each ~0.5 KB. Max ~0.5 MB at capacity. Only present when geocoding is enabled. |

**Steady-state memory**: ~30--50 MB for a single instance under normal load (add ~0.5 MB when geocoding cache is at capacity). The 256 MB container limit in `compose.yml` provides ample headroom.

## Backoff Impact

When extract or load fails, the pipeline applies exponential backoff:

| Consecutive Failures | Backoff Delay | Cumulative Idle Time |
|---|---|---|
| 1 | 200 ms | 200 ms |
| 2 | 400 ms | 600 ms |
| 3 | 800 ms | 1.4 s |
| 4 | 1.6 s | 3.0 s |
| 5+ | 5.0 s (max) | +5.0 s each |

Backoff resets immediately after a successful extract. During sustained failures, throughput drops to ~0.2 messages/second (one attempt per 5-second ceiling). This is intentional to avoid hammering a degraded broker.

## Bottleneck Summary

| Bottleneck | Impact | Mitigation |
|---|---|---|
| Kafka produce latency (`RequireAll`) | Largest per-cycle cost. Each message waits for all in-sync replicas to acknowledge. | Reduce replication factor (trades durability), use `RequireOne` for lower-criticality data, or batch writes. |
| Sequential processing | Single message in flight. Throughput = 1 / cycle_latency. | Scale horizontally with more partitions and instances. |
| Single-partition source topic | Cannot scale beyond one instance. | Increase source topic partition count. |
| Offset commit per message | Extra round-trip after every successful load. | Batch commits (commit every N messages or every T seconds) at the cost of at-least-once replay window. |
| `RequireAll` acks on producer | Waits for full ISR replication before acknowledging. | Acceptable for correctness; only reduce if latency is more important than durability. |
| Geocoding API latency (cache miss) | Adds 50--200 ms per message on cache miss when geocoding is enabled. | LRU cache (`MAPBOX_CACHE_SIZE`) absorbs repeated locations. Increase cache size for higher hit rates. Geocoding is optional and can be disabled entirely. |

## Monitoring Throughput

Use the Prometheus metrics to observe actual performance:

```promql
# Messages processed per second
rate(etl_messages_consumed_total[1m])

# End-to-end cycle latency (p99)
histogram_quantile(0.99, rate(etl_processing_duration_seconds_bucket[5m]))

# Transform error rate
rate(etl_transform_errors_total[5m]) / rate(etl_messages_consumed_total[5m])

# Pipeline availability
etl_pipeline_running
```

The `etl_processing_duration_seconds` histogram buckets (`1ms, 5ms, 10ms, 50ms, 100ms, 500ms, 1s, 5s`) are tuned for the expected latency range of the ETL cycle.

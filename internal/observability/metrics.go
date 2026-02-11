package observability

import (
	"github.com/prometheus/client_golang/prometheus"
)

// Metrics holds the Prometheus counters, histograms, and gauges for the ETL pipeline.
type Metrics struct {
	MessagesConsumed prometheus.Counter
	MessagesProduced prometheus.Counter
	TransformErrors  prometheus.Counter
	PipelineRunning  prometheus.Gauge

	// Batch processing metrics.
	BatchSize               prometheus.Histogram
	BatchProcessingDuration prometheus.Histogram

	// Geocoding metrics.
	GeocodeRequests    *prometheus.CounterVec   // labels: method={forward,reverse}, outcome={success,error,empty}
	GeocodeCache       *prometheus.CounterVec   // labels: method={forward,reverse}, result={hit,miss}
	GeocodeAPIDuration *prometheus.HistogramVec // labels: method={forward,reverse}
	GeocodeEnabled     prometheus.Gauge
}

// NewMetrics creates and registers all pipeline metrics with the default Prometheus registry.
func NewMetrics() *Metrics {
	m := &Metrics{
		MessagesConsumed: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "storm_etl",
			Name:      "messages_consumed_total",
			Help:      "Total messages read from the source topic.",
		}),
		MessagesProduced: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "storm_etl",
			Name:      "messages_produced_total",
			Help:      "Total messages written to the sink topic.",
		}),
		TransformErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "storm_etl",
			Name:      "transform_errors_total",
			Help:      "Total transformation failures.",
		}),
		PipelineRunning: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "storm_etl",
			Name:      "pipeline_running",
			Help:      "1 when the pipeline is active, 0 when shut down.",
		}),
		BatchSize: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: "storm_etl",
			Name:      "batch_size",
			Help:      "Number of messages per batch extracted from Kafka.",
			Buckets:   []float64{1, 5, 10, 20, 30, 40, 50, 75, 100},
		}),
		BatchProcessingDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: "storm_etl",
			Name:      "batch_processing_duration_seconds",
			Help:      "Duration of a complete batch extract-transform-load cycle.",
			Buckets:   []float64{0.01, 0.05, 0.1, 0.5, 1, 2.5, 5, 10},
		}),
		GeocodeRequests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "storm_etl",
			Name:      "geocode_requests_total",
			Help:      "Geocoding API requests by method and outcome.",
		}, []string{"method", "outcome"}),
		GeocodeCache: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "storm_etl",
			Name:      "geocode_cache_total",
			Help:      "Geocoding cache lookups by method and result.",
		}, []string{"method", "result"}),
		GeocodeAPIDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "storm_etl",
			Name:      "geocode_api_duration_seconds",
			Help:      "Mapbox API request duration in seconds.",
			Buckets:   []float64{0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5},
		}, []string{"method"}),
		GeocodeEnabled: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "storm_etl",
			Name:      "geocode_enabled",
			Help:      "1 when geocoding enrichment is enabled, 0 otherwise.",
		}),
	}

	prometheus.MustRegister(
		m.MessagesConsumed,
		m.MessagesProduced,
		m.TransformErrors,
		m.PipelineRunning,
		m.BatchSize,
		m.BatchProcessingDuration,
		m.GeocodeRequests,
		m.GeocodeCache,
		m.GeocodeAPIDuration,
		m.GeocodeEnabled,
	)

	return m
}

// NewMetricsForTesting creates Metrics with a fresh registry to avoid
// "already registered" panics when called from multiple tests.
func NewMetricsForTesting() *Metrics {
	return &Metrics{
		MessagesConsumed:        prometheus.NewCounter(prometheus.CounterOpts{Namespace: "storm_etl", Name: "messages_consumed_total"}),
		MessagesProduced:        prometheus.NewCounter(prometheus.CounterOpts{Namespace: "storm_etl", Name: "messages_produced_total"}),
		TransformErrors:         prometheus.NewCounter(prometheus.CounterOpts{Namespace: "storm_etl", Name: "transform_errors_total"}),
		PipelineRunning:         prometheus.NewGauge(prometheus.GaugeOpts{Namespace: "storm_etl", Name: "pipeline_running"}),
		BatchSize:               prometheus.NewHistogram(prometheus.HistogramOpts{Namespace: "storm_etl", Name: "batch_size"}),
		BatchProcessingDuration: prometheus.NewHistogram(prometheus.HistogramOpts{Namespace: "storm_etl", Name: "batch_processing_duration_seconds"}),
		GeocodeRequests:         prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: "storm_etl", Name: "geocode_requests_total"}, []string{"method", "outcome"}),
		GeocodeCache:            prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: "storm_etl", Name: "geocode_cache_total"}, []string{"method", "result"}),
		GeocodeAPIDuration:      prometheus.NewHistogramVec(prometheus.HistogramOpts{Namespace: "storm_etl", Name: "geocode_api_duration_seconds"}, []string{"method"}),
		GeocodeEnabled:          prometheus.NewGauge(prometheus.GaugeOpts{Namespace: "storm_etl", Name: "geocode_enabled"}),
	}
}

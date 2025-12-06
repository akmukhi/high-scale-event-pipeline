"""Prometheus metrics for ingestion service."""
from prometheus_client import Counter, Histogram, Gauge

# Metrics
events_received = Counter(
    'ingestion_events_received_total',
    'Total number of events received',
)

events_published = Counter(
    'ingestion_events_published_total',
    'Total number of events published to Pub/Sub',
)

events_failed = Counter(
    'ingestion_events_failed_total',
    'Total number of events that failed to publish',
    ['error_type']
)

publish_duration = Histogram(
    'ingestion_publish_duration_seconds',
    'Time taken to publish event to Pub/Sub',
    buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0]
)

active_connections = Gauge(
    'ingestion_active_connections',
    'Number of active HTTP connections'
)


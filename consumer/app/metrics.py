"""Prometheus metrics for consumer service."""
from prometheus_client import Counter, Histogram, Gauge

# Metrics
messages_received = Counter(
    'consumer_messages_received_total',
    'Total number of messages received from Pub/Sub',
)

messages_processed = Counter(
    'consumer_messages_processed_total',
    'Total number of messages successfully processed',
)

messages_failed = Counter(
    'consumer_messages_failed_total',
    'Total number of messages that failed to process',
    ['error_type']
)

processing_duration = Histogram(
    'consumer_processing_duration_seconds',
    'Time taken to process a message',
    buckets=[0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0]
)

queue_lag = Gauge(
    'consumer_queue_lag',
    'Current number of unprocessed messages in queue'
)

active_workers = Gauge(
    'consumer_active_workers',
    'Number of active worker threads processing messages'
)


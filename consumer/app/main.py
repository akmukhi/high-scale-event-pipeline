"""Consumer service for processing events from Pub/Sub."""
import os
import json
import logging
import time
import signal
import sys
from concurrent.futures import ThreadPoolExecutor
from typing import Callable, Optional

from google.cloud import pubsub_v1
from google.api_core import exceptions
from prometheus_client import start_http_server, generate_latest, CONTENT_TYPE_LATEST
from flask import Flask, Response

from app.metrics import (
    messages_received,
    messages_processed,
    messages_failed,
    processing_duration,
    queue_lag,
    active_workers
)

# Configure logging for Loki (structured JSON logging)
logging.basicConfig(
    level=logging.INFO,
    format='%(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# OpenTelemetry instrumentation
try:
    from opentelemetry import trace
    from opentelemetry.instrumentation.requests import RequestsInstrumentor
    from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor
    from opentelemetry.sdk.resources import Resource
    
    # Initialize tracing
    resource = Resource.create({"service.name": "consumer-service"})
    trace.set_tracer_provider(TracerProvider(resource=resource))
    tracer = trace.get_tracer(__name__)
    
    # Configure OTLP exporter
    otlp_endpoint = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://otel-collector:4317")
    otlp_exporter = OTLPSpanExporter(endpoint=otlp_endpoint, insecure=True)
    span_processor = BatchSpanProcessor(otlp_exporter)
    trace.get_tracer_provider().add_span_processor(span_processor)
    
    RequestsInstrumentor().instrument()
    
    OTEL_ENABLED = True
except ImportError:
    logger.warning("OpenTelemetry not available, tracing disabled")
    OTEL_ENABLED = False
    tracer = None

# Environment variables
PROJECT_ID = os.getenv("GCP_PROJECT_ID", "test-project")
SUBSCRIPTION_NAME = os.getenv("PUBSUB_SUBSCRIPTION", "events-subscription")
PUBSUB_EMULATOR_HOST = os.getenv("PUBSUB_EMULATOR_HOST", "pubsub-emulator:8085")
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "4"))
PROCESSING_DELAY = float(os.getenv("PROCESSING_DELAY", "0.1"))  # Simulate processing time

# Global state
running = True
executor = None
subscriber = None
subscription_path = None

# Flask app for metrics endpoint
app = Flask(__name__)


def log_structured(level: str, message: str, **kwargs):
    """Emit structured JSON log for Loki."""
    log_entry = {
        "level": level,
        "message": message,
        "service": "consumer",
        **kwargs
    }
    logger.info(json.dumps(log_entry))


def process_message(message: pubsub_v1.subscriber.message.Message) -> None:
    """Process a single message from Pub/Sub.
    
    Args:
        message: Pub/Sub message object
    """
    messages_received.inc()
    active_workers.inc()
    
    # Start trace span
    span = None
    if OTEL_ENABLED and tracer:
        span = tracer.start_span("process_message")
        span.set_attribute("message.id", message.message_id)
    
    start_time = time.time()
    
    try:
        # Decode message
        data = json.loads(message.data.decode('utf-8'))
        event_type = data.get('type', 'unknown')
        
        log_structured(
            "info",
            "Processing message",
            message_id=message.message_id,
            event_type=event_type
        )
        
        if span:
            span.set_attribute("event.type", event_type)
        
        # Simulate processing (replace with actual business logic)
        time.sleep(PROCESSING_DELAY)
        
        # Process the event (example: log it, transform it, store it, etc.)
        # In a real implementation, this would do actual work
        log_structured(
            "info",
            "Message processed successfully",
            message_id=message.message_id,
            event_type=event_type
        )
        
        # Acknowledge the message
        message.ack()
        messages_processed.inc()
        processing_duration.observe(time.time() - start_time)
        
        if span and OTEL_ENABLED:
            from opentelemetry import trace as otel_trace
            span.set_status(otel_trace.Status(otel_trace.StatusCode.OK))
        
    except json.JSONDecodeError as e:
        error_msg = f"Failed to decode message: {e}"
        log_structured("error", error_msg, message_id=message.message_id)
        messages_failed.labels(error_type="JSONDecodeError").inc()
        message.nack()  # Nack to retry
        processing_duration.observe(time.time() - start_time)
        
        if span and OTEL_ENABLED:
            from opentelemetry import trace as otel_trace
            span.set_status(otel_trace.Status(otel_trace.StatusCode.ERROR, error_msg))
            
    except Exception as e:
        error_msg = f"Failed to process message: {e}"
        log_structured("error", error_msg, message_id=message.message_id, error=str(e))
        messages_failed.labels(error_type=type(e).__name__).inc()
        message.nack()  # Nack to retry
        processing_duration.observe(time.time() - start_time)
        
        if span and OTEL_ENABLED:
            from opentelemetry import trace as otel_trace
            span.set_status(otel_trace.Status(otel_trace.StatusCode.ERROR, error_msg))
    finally:
        active_workers.dec()
        if span:
            span.end()


def callback(message: pubsub_v1.subscriber.message.Message) -> None:
    """Callback function for Pub/Sub messages.
    
    Args:
        message: Pub/Sub message object
    """
    if executor:
        executor.submit(process_message, message)
    else:
        process_message(message)


def update_queue_lag_metric():
    """Periodically update queue lag metric."""
    global running
    while running:
        try:
            # Get subscription stats
            # Note: This is a simplified version. In production, you'd use
            # the Pub/Sub API to get actual subscription metrics
            # For now, we'll use a placeholder
            queue_lag.set(0)  # Will be updated by autoscaler monitoring
            time.sleep(10)
        except Exception as e:
            log_structured("error", "Failed to update queue lag", error=str(e))
            time.sleep(10)


def signal_handler(sig, frame):
    """Handle shutdown signals."""
    global running
    log_structured("info", "Received shutdown signal, stopping consumer...")
    running = False
    if subscriber:
        subscriber.close()
    sys.exit(0)


@app.route("/health")
def health():
    """Health check endpoint."""
    return {"status": "healthy", "service": "consumer"}


@app.route("/metrics")
def metrics():
    """Prometheus metrics endpoint."""
    return Response(generate_latest(), mimetype=CONTENT_TYPE_LATEST)


def main():
    """Main function to start the consumer service."""
    global subscriber, subscription_path, executor
    
    # Set emulator host if provided
    if PUBSUB_EMULATOR_HOST:
        os.environ["PUBSUB_EMULATOR_HOST"] = PUBSUB_EMULATOR_HOST
    
    # Initialize subscriber
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_NAME)
    
    # Ensure subscription exists
    try:
        subscriber.get_subscription(request={"subscription": subscription_path})
        log_structured("info", f"Subscription {subscription_path} exists")
    except exceptions.NotFound:
        log_structured("error", f"Subscription {subscription_path} not found")
        sys.exit(1)
    
    # Start metrics server
    metrics_port = int(os.getenv("METRICS_PORT", "9090"))
    start_http_server(metrics_port)
    log_structured("info", f"Metrics server started on port {metrics_port}")
    
    # Start queue lag updater thread
    import threading
    lag_thread = threading.Thread(target=update_queue_lag_metric, daemon=True)
    lag_thread.start()
    
    # Register signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Start thread pool for processing
    executor = ThreadPoolExecutor(max_workers=MAX_WORKERS)
    log_structured("info", f"Started consumer with {MAX_WORKERS} workers")
    
    # Start Flask app for health/metrics in background
    import threading
    flask_port = int(os.getenv("FLASK_PORT", "8080"))
    flask_thread = threading.Thread(
        target=lambda: app.run(host="0.0.0.0", port=flask_port, debug=False),
        daemon=True
    )
    flask_thread.start()
    
    # Start pulling messages
    log_structured("info", f"Starting to pull messages from {subscription_path}")
    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
    
    try:
        streaming_pull_future.result()
    except KeyboardInterrupt:
        streaming_pull_future.cancel()
        log_structured("info", "Consumer stopped")


if __name__ == "__main__":
    main()


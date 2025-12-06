"""Autoscaler service that monitors queue lag and scales consumer containers."""
import os
import json
import logging
import time
import signal
import sys
from typing import Optional

from prometheus_client import Counter, Gauge, Histogram, start_http_server, generate_latest, CONTENT_TYPE_LATEST
from flask import Flask, Response

from app.pubsub_monitor import PubSubMonitor
from app.docker_client import DockerClient

# Configure logging for Loki (structured JSON logging)
logging.basicConfig(
    level=logging.INFO,
    format='%(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# Prometheus metrics
scaling_actions = Counter(
    'autoscaler_scaling_actions_total',
    'Total number of scaling actions taken',
    ['direction']
)

current_replicas = Gauge(
    'autoscaler_current_replicas',
    'Current number of consumer replicas',
    ['service']
)

target_replicas = Gauge(
    'autoscaler_target_replicas',
    'Target number of consumer replicas',
    ['service']
)

queue_lag_observed = Gauge(
    'autoscaler_queue_lag_observed',
    'Queue lag observed by autoscaler'
)

scaling_duration = Histogram(
    'autoscaler_scaling_duration_seconds',
    'Time taken to perform scaling action',
    buckets=[0.1, 0.5, 1.0, 2.0, 5.0, 10.0]
)

# Environment variables
PROJECT_ID = os.getenv("GCP_PROJECT_ID", "test-project")
SUBSCRIPTION_NAME = os.getenv("PUBSUB_SUBSCRIPTION", "events-subscription")
SERVICE_NAME = os.getenv("SERVICE_NAME", "consumer")
MIN_REPLICAS = int(os.getenv("MIN_REPLICAS", "1"))
MAX_REPLICAS = int(os.getenv("MAX_REPLICAS", "10"))
LAG_THRESHOLD_UP = int(os.getenv("LAG_THRESHOLD_UP", "100"))  # Scale up if lag > this
LAG_THRESHOLD_DOWN = int(os.getenv("LAG_THRESHOLD_DOWN", "10"))  # Scale down if lag < this
MESSAGES_PER_REPLICA = int(os.getenv("MESSAGES_PER_REPLICA", "50"))  # Target messages per replica
SCALE_UP_COOLDOWN = int(os.getenv("SCALE_UP_COOLDOWN", "30"))  # Seconds to wait before scaling up again
SCALE_DOWN_COOLDOWN = int(os.getenv("SCALE_DOWN_COOLDOWN", "60"))  # Seconds to wait before scaling down
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "10"))  # Seconds between checks
PUBSUB_EMULATOR_HOST = os.getenv("PUBSUB_EMULATOR_HOST", "pubsub-emulator:8085")

# Global state
running = True
last_scale_up_time = 0
last_scale_down_time = 0

# Flask app for metrics endpoint
app = Flask(__name__)


def log_structured(level: str, message: str, **kwargs):
    """Emit structured JSON log for Loki."""
    log_entry = {
        "level": level,
        "message": message,
        "service": "autoscaler",
        **kwargs
    }
    logger.info(json.dumps(log_entry))


def calculate_target_replicas(queue_lag: int) -> int:
    """Calculate target number of replicas based on queue lag.
    
    Args:
        queue_lag: Current queue lag (undelivered messages)
        
    Returns:
        Target number of replicas
    """
    if queue_lag <= 0:
        return MIN_REPLICAS
    
    # Calculate based on messages per replica
    target = max(MIN_REPLICAS, (queue_lag + MESSAGES_PER_REPLICA - 1) // MESSAGES_PER_REPLICA)
    target = min(target, MAX_REPLICAS)
    
    return target


def get_queue_lag_alternative(monitor: PubSubMonitor) -> Optional[int]:
    """Get queue lag using alternative method when monitoring API is unavailable.
    
    For local development, we'll use a simplified approach:
    - Query the subscription and estimate based on available metrics
    - In production, use Cloud Monitoring API
    
    Args:
        monitor: PubSubMonitor instance
        
    Returns:
        Estimated queue lag, or None if unavailable
    """
    # For local emulator, we can't easily get num_undelivered_messages
    # In a real implementation, you would:
    # 1. Use Cloud Monitoring API to query the metric
    # 2. Or use a custom metric exposed by the consumer service
    
    # For now, return None and the autoscaler will use a fallback
    # In production, implement proper monitoring API call
    return None


def autoscale_loop(monitor: PubSubMonitor, docker_client: DockerClient):
    """Main autoscaling loop.
    
    Args:
        monitor: PubSubMonitor instance
        docker_client: DockerClient instance
    """
    global last_scale_up_time, last_scale_down_time
    
    log_structured("info", "Starting autoscaler loop")
    
    while running:
        try:
            # Get current queue lag
            queue_lag = monitor.get_queue_lag()
            
            # Fallback: if queue lag is unavailable, use alternative method
            if queue_lag is None:
                queue_lag = get_queue_lag_alternative(monitor)
            
            # If still unavailable, skip this iteration
            if queue_lag is None:
                log_structured("warning", "Queue lag unavailable, skipping autoscale check")
                time.sleep(POLL_INTERVAL)
                continue
            
            queue_lag_observed.set(queue_lag)
            
            # Get current replica count
            current_replica_count = docker_client.get_running_count(SERVICE_NAME)
            current_replicas.labels(service=SERVICE_NAME).set(current_replica_count)
            
            # Calculate target replicas
            target_replica_count = calculate_target_replicas(queue_lag)
            target_replicas.labels(service=SERVICE_NAME).set(target_replica_count)
            
            log_structured(
                "info",
                "Autoscale check",
                queue_lag=queue_lag,
                current_replicas=current_replica_count,
                target_replicas=target_replica_count
            )
            
            # Determine if scaling is needed
            should_scale_up = (
                target_replica_count > current_replica_count and
                time.time() - last_scale_up_time >= SCALE_UP_COOLDOWN
            )
            
            should_scale_down = (
                target_replica_count < current_replica_count and
                time.time() - last_scale_down_time >= SCALE_DOWN_COOLDOWN and
                queue_lag < LAG_THRESHOLD_DOWN
            )
            
            if should_scale_up:
                log_structured(
                    "info",
                    "Scaling up",
                    from_replicas=current_replica_count,
                    to_replicas=target_replica_count
                )
                
                start_time = time.time()
                success = docker_client.scale_service(SERVICE_NAME, target_replica_count)
                scaling_duration.observe(time.time() - start_time)
                
                if success:
                    scaling_actions.labels(direction="up").inc()
                    last_scale_up_time = time.time()
                else:
                    log_structured("error", "Failed to scale up")
                    
            elif should_scale_down:
                log_structured(
                    "info",
                    "Scaling down",
                    from_replicas=current_replica_count,
                    to_replicas=target_replica_count
                )
                
                start_time = time.time()
                success = docker_client.scale_service(SERVICE_NAME, target_replica_count)
                scaling_duration.observe(time.time() - start_time)
                
                if success:
                    scaling_actions.labels(direction="down").inc()
                    last_scale_down_time = time.time()
                else:
                    log_structured("error", "Failed to scale down")
            else:
                log_structured("debug", "No scaling needed")
            
            time.sleep(POLL_INTERVAL)
            
        except Exception as e:
            log_structured("error", "Error in autoscale loop", error=str(e))
            time.sleep(POLL_INTERVAL)


def signal_handler(sig, frame):
    """Handle shutdown signals."""
    global running
    log_structured("info", "Received shutdown signal, stopping autoscaler...")
    running = False
    sys.exit(0)


@app.route("/health")
def health():
    """Health check endpoint."""
    return {"status": "healthy", "service": "autoscaler"}


@app.route("/metrics")
def metrics():
    """Prometheus metrics endpoint."""
    return Response(generate_latest(), mimetype=CONTENT_TYPE_LATEST)


def main():
    """Main function to start the autoscaler service."""
    global running
    
    # Set emulator host if provided
    if PUBSUB_EMULATOR_HOST:
        os.environ["PUBSUB_EMULATOR_HOST"] = PUBSUB_EMULATOR_HOST
    
    log_structured("info", "Starting autoscaler service")
    log_structured("info", f"Service: {SERVICE_NAME}")
    log_structured("info", f"Min replicas: {MIN_REPLICAS}, Max replicas: {MAX_REPLICAS}")
    log_structured("info", f"Lag thresholds: up={LAG_THRESHOLD_UP}, down={LAG_THRESHOLD_DOWN}")
    
    # Initialize clients
    try:
        monitor = PubSubMonitor(PROJECT_ID, SUBSCRIPTION_NAME)
        docker_client = DockerClient()
    except Exception as e:
        log_structured("error", "Failed to initialize clients", error=str(e))
        sys.exit(1)
    
    # Start metrics server
    metrics_port = int(os.getenv("METRICS_PORT", "9090"))
    start_http_server(metrics_port)
    log_structured("info", f"Metrics server started on port {metrics_port}")
    
    # Register signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Start Flask app for health/metrics in background
    import threading
    flask_port = int(os.getenv("FLASK_PORT", "8080"))
    flask_thread = threading.Thread(
        target=lambda: app.run(host="0.0.0.0", port=flask_port, debug=False),
        daemon=True
    )
    flask_thread.start()
    
    # Start autoscaling loop
    try:
        autoscale_loop(monitor, docker_client)
    except KeyboardInterrupt:
        log_structured("info", "Autoscaler stopped")


if __name__ == "__main__":
    main()


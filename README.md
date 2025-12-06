# High-Scale Event Pipeline with Autoscaling

A production-ready event-driven pipeline demonstrating high-scale event processing with autoscaling capabilities. This project includes end-to-end observability with Prometheus metrics, OpenTelemetry traces, and Loki logs.

## Architecture

The pipeline consists of four main components:

1. **Ingestion Service** (FastAPI) - Receives events via REST API and publishes to Google Cloud Pub/Sub
2. **Consumer Service** (Python) - Subscribes to Pub/Sub and processes events
3. **Autoscaler Service** (Python) - Monitors Pub/Sub queue lag and scales consumer containers dynamically
4. **Observability Stack** - Prometheus, Loki, OpenTelemetry Collector, Jaeger, and Grafana

### Data Flow

```
Events → Ingestion Service → Pub/Sub Topic → Consumer Service(s) → Processed
                                    ↓
                            Autoscaler (monitors lag)
                                    ↓
                            Scales Consumer Replicas
```

## Features

- **Event Ingestion**: FastAPI-based REST API for receiving events
- **Message Queue**: Google Cloud Pub/Sub for reliable message delivery
- **Autoscaling**: Custom autoscaler that monitors queue lag and scales consumer containers
- **Observability**:
  - **Prometheus**: Metrics collection from all services
  - **OpenTelemetry**: Distributed tracing across services
  - **Loki**: Centralized log aggregation
  - **Grafana**: Visualization dashboards
  - **Jaeger**: Trace visualization

## Prerequisites

- Docker and Docker Compose
- Python 3.11+ (for local development)
- Google Cloud SDK (optional, for production Pub/Sub)

## Quick Start

### 1. Clone the Repository

```bash
git clone <repository-url>
cd high-scale-event-pipeline
```

### 2. Start the Services

```bash
docker-compose up -d
```

This will start:
- Pub/Sub emulator
- Ingestion service (port 8000)
- Consumer service (scalable)
- Autoscaler service
- Prometheus (port 9090)
- Loki (port 3100)
- OpenTelemetry Collector
- Jaeger (port 16686)
- Grafana (port 3000)

### 3. Setup Pub/Sub Topic and Subscription

```bash
./setup-pubsub.sh
```

Or manually using the Pub/Sub emulator:

```bash
export PUBSUB_EMULATOR_HOST=localhost:8085
python3 -c "
from google.cloud import pubsub_v1
publisher = pubsub_v1.PublisherClient()
subscriber = pubsub_v1.SubscriberClient()
topic_path = publisher.topic_path('test-project', 'events')
subscription_path = subscriber.subscription_path('test-project', 'events-subscription')

# Create topic
try:
    publisher.create_topic(request={'name': topic_path})
    print('Topic created')
except:
    print('Topic exists')

# Create subscription
try:
    subscriber.create_subscription(request={'name': subscription_path, 'topic': topic_path})
    print('Subscription created')
except:
    print('Subscription exists')
"
```

### 4. Send Test Events

```bash
# Send a single event
curl -X POST http://localhost:8000/events \
  -H "Content-Type: application/json" \
  -d '{
    "type": "user_action",
    "data": {
      "user_id": "123",
      "action": "click",
      "timestamp": "2024-01-01T00:00:00Z"
    }
  }'

# Send multiple events (load test)
for i in {1..100}; do
  curl -X POST http://localhost:8000/events \
    -H "Content-Type: application/json" \
    -d "{\"type\": \"test\", \"data\": {\"id\": $i}}" &
done
wait
```

### 5. Access Dashboards

- **Grafana**: http://localhost:3000 (admin/admin)
- **Prometheus**: http://localhost:9090
- **Jaeger**: http://localhost:16686
- **Ingestion API**: http://localhost:8000/docs (FastAPI Swagger UI)

## Configuration

### Environment Variables

Key environment variables can be set in `docker-compose.yml` or via `.env` file:

- `GCP_PROJECT_ID`: GCP project ID (default: `test-project`)
- `PUBSUB_TOPIC`: Pub/Sub topic name (default: `events`)
- `PUBSUB_SUBSCRIPTION`: Pub/Sub subscription name (default: `events-subscription`)
- `MIN_REPLICAS`: Minimum consumer replicas (default: `1`)
- `MAX_REPLICAS`: Maximum consumer replicas (default: `10`)
- `LAG_THRESHOLD_UP`: Queue lag threshold to scale up (default: `100`)
- `LAG_THRESHOLD_DOWN`: Queue lag threshold to scale down (default: `10`)
- `MESSAGES_PER_REPLICA`: Target messages per replica (default: `50`)

### Autoscaling Configuration

The autoscaler monitors the Pub/Sub subscription queue lag and scales consumer containers based on:

- **Scale Up**: When queue lag > `LAG_THRESHOLD_UP` and cooldown period has passed
- **Scale Down**: When queue lag < `LAG_THRESHOLD_DOWN` and cooldown period has passed
- **Cooldown**: Prevents rapid scaling oscillations (30s up, 60s down by default)

## Project Structure

```
high-scale-event-pipeline/
├── ingestion/           # Ingestion service (FastAPI)
│   ├── app/
│   │   ├── main.py      # FastAPI application
│   │   ├── pubsub_client.py
│   │   └── metrics.py
│   ├── Dockerfile
│   └── requirements.txt
├── consumer/            # Consumer service
│   ├── app/
│   │   ├── main.py      # Pub/Sub subscriber
│   │   └── metrics.py
│   ├── Dockerfile
│   └── requirements.txt
├── autoscaler/          # Autoscaler service
│   ├── app/
│   │   ├── main.py      # Autoscaler logic
│   │   ├── pubsub_monitor.py
│   │   └── docker_client.py
│   ├── Dockerfile
│   └── requirements.txt
├── prometheus/          # Prometheus config
│   └── prometheus.yml
├── otel-collector/      # OpenTelemetry config
│   └── config.yaml
├── grafana/             # Grafana dashboards
│   └── dashboards/
├── docker-compose.yml    # All services
├── promtail-config.yml  # Log collection config
└── setup-pubsub.sh      # Pub/Sub setup script
```

## Observability

### Metrics

All services expose Prometheus metrics:

- **Ingestion Service** (`/metrics`):
  - `ingestion_events_received_total`
  - `ingestion_events_published_total`
  - `ingestion_events_failed_total`
  - `ingestion_publish_duration_seconds`

- **Consumer Service** (`/metrics`):
  - `consumer_messages_received_total`
  - `consumer_messages_processed_total`
  - `consumer_messages_failed_total`
  - `consumer_processing_duration_seconds`
  - `consumer_queue_lag`

- **Autoscaler Service** (`/metrics`):
  - `autoscaler_scaling_actions_total`
  - `autoscaler_current_replicas`
  - `autoscaler_target_replicas`
  - `autoscaler_queue_lag_observed`

### Traces

OpenTelemetry traces are automatically instrumented and exported to Jaeger. View traces at http://localhost:16686.

### Logs

All services emit structured JSON logs that are collected by Promtail and stored in Loki. Query logs in Grafana.

## Scaling

### Manual Scaling

Scale consumer service manually:

```bash
docker-compose up -d --scale consumer=5
```

### Autoscaling

The autoscaler automatically scales consumer containers based on queue lag. Monitor scaling actions:

```bash
# View autoscaler logs
docker-compose logs -f autoscaler

# Check current replicas
docker-compose ps consumer
```

## Development

### Running Services Locally

```bash
# Ingestion service
cd ingestion
pip install -r requirements.txt
python -m app.main

# Consumer service
cd consumer
pip install -r requirements.txt
python -m app.main

# Autoscaler service
cd autoscaler
pip install -r requirements.txt
python -m app.main
```

### Testing

```bash
# Health checks
curl http://localhost:8000/health  # Ingestion
curl http://localhost:8080/health   # Consumer/Autoscaler

# Metrics
curl http://localhost:8000/metrics
curl http://localhost:9090/metrics
```

## Production Considerations

For production deployment:

1. **Use Real Pub/Sub**: Replace emulator with Google Cloud Pub/Sub
   - Set `GCP_PROJECT_ID` to your project ID
   - Remove `PUBSUB_EMULATOR_HOST`
   - Configure GCP credentials

2. **Kubernetes Deployment**: Use Kubernetes HPA instead of Docker-based autoscaling
   - Deploy services as Kubernetes deployments
   - Use HPA with custom metrics from Prometheus

3. **Monitoring**: Use Cloud Monitoring API for accurate queue lag metrics
   - Implement `get_queue_lag()` in `autoscaler/app/pubsub_monitor.py` using Monitoring API

4. **Security**: 
   - Use service accounts with minimal permissions
   - Enable TLS for all services
   - Secure Grafana and other UIs

5. **High Availability**:
   - Deploy multiple ingestion service replicas
   - Use Pub/Sub's built-in message ordering and deduplication
   - Configure proper health checks and readiness probes

## Troubleshooting

### Services not starting

```bash
# Check logs
docker-compose logs

# Check specific service
docker-compose logs ingestion
docker-compose logs consumer
docker-compose logs autoscaler
```

### Pub/Sub connection issues

```bash
# Verify emulator is running
curl http://localhost:8085

# Check topic/subscription exist
./setup-pubsub.sh
```

### Autoscaler not scaling

- Check autoscaler logs for errors
- Verify Docker socket is mounted (`/var/run/docker.sock`)
- Ensure queue lag metrics are available
- Check cooldown periods haven't expired

## License

See LICENSE file for details.

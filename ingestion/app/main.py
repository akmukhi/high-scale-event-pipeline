"""FastAPI ingestion service for receiving and publishing events."""
import os
import json
import logging
import time
from contextlib import asynccontextmanager
from typing import Dict, Any

from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST
from starlette.responses import Response

from app.pubsub_client import PubSubClient
from app.metrics import (
    events_received,
    events_published,
    events_failed,
    publish_duration,
    active_connections
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
    from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
    from opentelemetry.instrumentation.requests import RequestsInstrumentor
    from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor
    from opentelemetry.sdk.resources import Resource
    
    # Initialize tracing
    resource = Resource.create({"service.name": "ingestion-service"})
    trace.set_tracer_provider(TracerProvider(resource=resource))
    tracer = trace.get_tracer(__name__)
    
    # Configure OTLP exporter
    otlp_endpoint = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://otel-collector:4317")
    otlp_exporter = OTLPSpanExporter(endpoint=otlp_endpoint, insecure=True)
    span_processor = BatchSpanProcessor(otlp_exporter)
    trace.get_tracer_provider().add_span_processor(span_processor)
    
    # Instrument FastAPI
    FastAPIInstrumentor.instrument_app = lambda app: None  # Will be called in lifespan
    RequestsInstrumentor().instrument()
    
    OTEL_ENABLED = True
except ImportError:
    logger.warning("OpenTelemetry not available, tracing disabled")
    OTEL_ENABLED = False
    tracer = None

# Environment variables
PROJECT_ID = os.getenv("GCP_PROJECT_ID", "test-project")
TOPIC_NAME = os.getenv("PUBSUB_TOPIC", "events")
PUBSUB_EMULATOR_HOST = os.getenv("PUBSUB_EMULATOR_HOST", "pubsub-emulator:8085")

# Initialize Pub/Sub client
pubsub_client = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan context manager for startup/shutdown."""
    global pubsub_client
    
    # Startup
    logger.info("Starting ingestion service...")
    logger.info(f"Pub/Sub emulator: {PUBSUB_EMULATOR_HOST}")
    logger.info(f"Project ID: {PROJECT_ID}, Topic: {TOPIC_NAME}")
    
    # Set emulator host if provided
    if PUBSUB_EMULATOR_HOST:
        os.environ["PUBSUB_EMULATOR_HOST"] = PUBSUB_EMULATOR_HOST
    
    # Initialize Pub/Sub client
    pubsub_client = PubSubClient(PROJECT_ID, TOPIC_NAME)
    
    # Instrument FastAPI with OpenTelemetry
    if OTEL_ENABLED:
        from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
        FastAPIInstrumentor.instrument_app(app)
    
    yield
    
    # Shutdown
    logger.info("Shutting down ingestion service...")


app = FastAPI(
    title="Event Ingestion Service",
    description="High-scale event ingestion service with Pub/Sub",
    version="1.0.0",
    lifespan=lifespan
)


def log_structured(level: str, message: str, **kwargs):
    """Emit structured JSON log for Loki."""
    log_entry = {
        "level": level,
        "message": message,
        "service": "ingestion",
        **kwargs
    }
    logger.info(json.dumps(log_entry))


@app.middleware("http")
async def metrics_middleware(request: Request, call_next):
    """Middleware to track active connections."""
    active_connections.inc()
    try:
        response = await call_next(request)
        return response
    finally:
        active_connections.dec()


@app.post("/events")
async def ingest_event(request: Request, event: Dict[str, Any]):
    """Ingest an event and publish it to Pub/Sub.
    
    Expected payload:
    {
        "type": "user_action",
        "data": {...},
        "timestamp": "2024-01-01T00:00:00Z"
    }
    """
    if not pubsub_client:
        raise HTTPException(status_code=503, detail="Service not ready")
    
    # Track received events
    events_received.inc()
    
    # Start trace span
    span = None
    if OTEL_ENABLED and tracer:
        span = tracer.start_span("ingest_event")
        span.set_attribute("event.type", event.get("type", "unknown"))
    
    try:
        # Validate event
        if not isinstance(event, dict):
            raise HTTPException(status_code=400, detail="Event must be a JSON object")
        
        # Add timestamp if not present
        if "timestamp" not in event:
            event["timestamp"] = time.time()
        
        # Publish to Pub/Sub
        start_time = time.time()
        try:
            message_id = pubsub_client.publish(event)
            publish_duration.observe(time.time() - start_time)
            events_published.inc()
            
            log_structured(
                "info",
                "Event ingested successfully",
                event_type=event.get("type", "unknown"),
                message_id=message_id
            )
            
            if span:
                span.set_attribute("message.id", message_id)
                span.set_status(trace.Status(trace.StatusCode.OK))
            
            return JSONResponse(
                status_code=202,
                content={
                    "status": "accepted",
                    "message_id": message_id,
                    "event_type": event.get("type", "unknown")
                }
            )
            
        except Exception as e:
            publish_duration.observe(time.time() - start_time)
            events_failed.labels(error_type=type(e).__name__).inc()
            
            log_structured(
                "error",
                "Failed to publish event",
                error=str(e),
                event_type=event.get("type", "unknown")
            )
            
            if span and OTEL_ENABLED:
                from opentelemetry import trace as otel_trace
                span.set_status(otel_trace.Status(otel_trace.StatusCode.ERROR, str(e)))
            
            raise HTTPException(status_code=500, detail=f"Failed to publish event: {str(e)}")
            
    except HTTPException:
        raise
    except Exception as e:
        events_failed.labels(error_type=type(e).__name__).inc()
        log_structured("error", "Unexpected error", error=str(e))
            if span and OTEL_ENABLED:
                from opentelemetry import trace as otel_trace
                span.set_status(otel_trace.Status(otel_trace.StatusCode.ERROR, str(e)))
        raise HTTPException(status_code=500, detail="Internal server error")
    finally:
        if span:
            span.end()


@app.get("/health")
async def health():
    """Health check endpoint."""
    return {"status": "healthy", "service": "ingestion"}


@app.get("/metrics")
async def metrics():
    """Prometheus metrics endpoint."""
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)


if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", "8000"))
    uvicorn.run(app, host="0.0.0.0", port=port)


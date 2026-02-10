"""Health check and metrics API endpoint."""

from fastapi import FastAPI
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST
from starlette.responses import Response

app = FastAPI(title="Streaming Platform API", version="1.0.0")


@app.get("/health")
def health():
    return {"status": "healthy"}


@app.get("/metrics")
def metrics():
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/ready")
def ready():
    return {"status": "ready"}

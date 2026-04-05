"""libs/common/health: Shared FastAPI app with /health, /ready, /metrics endpoints.

Used by all services. Runs as a background asyncio task alongside the main loop.
"""

from collections.abc import Callable

from fastapi import FastAPI
from fastapi.responses import PlainTextResponse


def create_health_app(
    service_name: str,
    readiness_check: Callable[..., bool] | None = None,
) -> FastAPI:
    """Create a minimal FastAPI app for K8s probes and Prometheus scraping.

    Args:
        service_name: Used in response bodies.
        readiness_check: Callable returning True if service is ready to receive traffic.
                        If None, always reports ready.

    Endpoints:
        GET /health  -> 200 if process is alive (liveness probe)
        GET /ready   -> 200 if ready to process, 503 otherwise (readiness probe)
        GET /metrics -> Prometheus text exposition format
    """
    app = FastAPI(title=f"{service_name} health", docs_url=None, redoc_url=None)

    @app.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "ok", "service": service_name}

    @app.get("/ready")
    async def ready() -> dict[str, str] | PlainTextResponse:
        if readiness_check and not readiness_check():
            return PlainTextResponse("not ready", status_code=503)
        return {"status": "ready", "service": service_name}

    @app.get("/metrics")
    async def metrics() -> PlainTextResponse:
        from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

        return PlainTextResponse(
            generate_latest().decode("utf-8"),
            media_type=CONTENT_TYPE_LATEST,
        )

    return app


async def run_health_server(app: FastAPI, port: int = 8080) -> None:
    """Run the health/metrics server as a background asyncio task.

    Args:
        app: FastAPI application created by create_health_app().
        port: Port to listen on (default 8080).
    """
    import uvicorn

    config = uvicorn.Config(app, host="0.0.0.0", port=port, log_level="warning")
    server = uvicorn.Server(config)
    await server.serve()

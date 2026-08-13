"""Metrics dashboard.

Read-only. Every panel is a query against tables the worker writes; this service
never touches Redis and never writes anything, so a bug here cannot damage the
pipeline it observes.

It also reports the pipeline's own health — SDK drops and spills, Redis stream
depth, consumer lag, dead-letter count. An observability tool that cannot show
its own data loss is asking to be trusted on faith: a half-empty chart looks
exactly like a quiet system unless something says otherwise.
"""

from __future__ import annotations

import asyncio
import os
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, Request, Response
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from . import queries

CHAT_URL = os.getenv("CHAT_URL", "http://chat:8000")
INGEST_URL = os.getenv("INGEST_URL", "http://ingestion:8001")

_client: httpx.AsyncClient | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _client
    _client = httpx.AsyncClient(timeout=3.0)
    yield
    await _client.aclose()
    await queries.aclose()


app = FastAPI(title="argus-dash", version="0.1.0", lifespan=lifespan)
app.mount("/static", StaticFiles(directory="dashboard/static"), name="static")
templates = Jinja2Templates(directory="dashboard/templates")


@app.get("/health")
async def health(response: Response) -> dict:
    ok = await queries.postgres_ok()
    if not ok:
        response.status_code = 503
    return {"status": "ok" if ok else "degraded", "service": "dashboard", "postgres": ok}


@app.get("/", response_class=HTMLResponse)
async def index(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(request, "index.html")


# --------------------------------------------------------------------------- #
# metrics
# --------------------------------------------------------------------------- #


@app.get("/api/overview")
async def api_overview(window: int = 60) -> dict:
    return await queries.overview(window)


@app.get("/api/latency")
async def api_latency(window: int = 60) -> dict:
    return await queries.latency_series(window)


@app.get("/api/throughput")
async def api_throughput(window: int = 60) -> dict:
    return await queries.throughput_series(window)


@app.get("/api/errors")
async def api_errors(window: int = 60) -> dict:
    return await queries.errors(window)


@app.get("/api/cost")
async def api_cost(window: int = 1440, group_by: str = "model") -> dict:
    return await queries.cost(window, group_by)


@app.get("/api/models")
async def api_models(window: int = 60) -> dict:
    return await queries.models(window)


@app.get("/api/recent")
async def api_recent(limit: int = 25) -> dict:
    return await queries.recent(limit)


# --------------------------------------------------------------------------- #
# pipeline health
# --------------------------------------------------------------------------- #


async def _get_json(url: str) -> dict | None:
    """Fetch a sibling service's stats, tolerating it being down.

    A dead dependency must degrade one panel, not the whole page — the moment
    the dashboard 500s because a service it monitors is unhealthy, it stops
    being useful at exactly the time you need it.
    """
    try:
        response = await _client.get(url)
        response.raise_for_status()
        return response.json()
    except Exception:  # noqa: BLE001
        return None


@app.get("/api/pipeline")
async def api_pipeline() -> dict:
    sdk, ingest, dlq = await asyncio.gather(
        _get_json(f"{CHAT_URL}/sdk-stats"),
        _get_json(f"{INGEST_URL}/v1/stats"),
        queries.dead_letters(10),
    )

    stream = (ingest or {}).get("stream", {})
    groups = stream.get("groups") or []
    return {
        "sdk": sdk,
        "sdk_reachable": sdk is not None,
        "ingest_counts": (ingest or {}).get("counts"),
        "ingest_reachable": ingest is not None,
        "stream": {
            "name": stream.get("stream"),
            "length": stream.get("length"),
            # Lag is the number of entries a consumer group has not yet read.
            # It is the single best indicator of whether workers are keeping up.
            "lag": sum(g.get("lag") or 0 for g in groups),
            "pending": sum(g.get("pending") or 0 for g in groups),
            "consumers": sum(g.get("consumers") or 0 for g in groups),
        },
        "dead_letters": dlq,
    }

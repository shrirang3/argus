"""Ingestion service.

Receives inference events from the SDK, validates them, redacts, enriches with
cost, and publishes to Redis Streams. The worker drains the stream into Postgres.

Two behaviours define this service:

**Rows are validated individually.** A collector that rejects 49 good events
because of the 50th is not a collector. Bad rows are quarantined in
`dead_letter_events` with the raw payload and the reason, so a schema mismatch
can be diagnosed and replayed rather than guessed at.

**The response does not wait on Postgres.** Accepted events are handed to Redis
and the caller gets 202. Durability is the worker's job, which is what keeps the
SDK's send fast and the chat path insulated from database latency.
"""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager

from argus import redact
from argus.schema import InferenceEvent, IngestResult
from fastapi import FastAPI, HTTPException, Request, Response
from pydantic import ValidationError
from sqlalchemy import text

from . import otlp, store
from .config import REDACT_AT_EDGE, cost_usd

logging.basicConfig(level="INFO")
log = logging.getLogger("argus.ingest")


@asynccontextmanager
async def lifespan(app: FastAPI):
    yield
    await store.aclose()


app = FastAPI(title="argus-ingest", version="0.1.0", lifespan=lifespan)

_COUNTS = {"accepted": 0, "rejected": 0, "published": 0}


@app.get("/health")
async def health(response: Response) -> dict:
    """Reports Redis reachability, because without it this service cannot do its
    job — a health check that only proves the process is running is theatre."""
    ok = await store.redis_ok()
    if not ok:
        response.status_code = 503
    return {"status": "ok" if ok else "degraded", "service": "ingestion", "redis": ok}


def _enrich(event: InferenceEvent) -> InferenceEvent:
    """Edge redaction and cost, applied after validation.

    Redacting again here is defence in depth: our own SDK already redacted
    before the event left its process, but events can arrive from senders we do
    not control — including the OTLP endpoint.
    """
    if REDACT_AT_EDGE:
        for field in ("input_preview", "output_preview"):
            value = getattr(event, field)
            if value:
                cleaned, hits = redact.redact(value)
                setattr(event, field, cleaned)
                event.redaction_hits = redact.merge_hits(event.redaction_hits, hits)

    if event.cost_usd is None:
        event.cost_usd = cost_usd(
            event.provider, event.model, event.prompt_tokens, event.completion_tokens
        )
    return event


async def _ingest(rows: list, source: str) -> IngestResult:
    """Validate, enrich, publish. Shared by the native and OTLP endpoints."""
    accepted: list[dict] = []
    rejected: list[tuple[dict, str, str]] = []

    for row in rows:
        if isinstance(row, InferenceEvent):
            accepted.append(_enrich(row).model_dump(mode="json"))
            continue
        try:
            event = InferenceEvent.model_validate(row)
        except ValidationError as exc:
            rejected.append((row, str(exc.errors()[:3]), source))
            continue
        accepted.append(_enrich(event).model_dump(mode="json"))

    published = 0
    if accepted:
        try:
            published = await store.publish(accepted)
        except Exception as exc:  # noqa: BLE001
            # Redis is down. Tell the SDK so it buffers and retries rather than
            # believing the events were delivered — a false 202 is data loss
            # that looks like success.
            #
            # 503, not 500: this says "the dependency is unavailable, try again",
            # which is actionable. 500 says "something is broken here", which
            # invites a client to give up.
            log.warning("publish failed: %s", exc)
            raise HTTPException(status_code=503, detail="event store unavailable, retry") from exc

    if rejected:
        try:
            await store.dead_letter(rejected)
        except Exception:  # noqa: BLE001
            # A failure to record a reject must not fail the good rows that were
            # already published.
            log.warning("dead-letter write failed", exc_info=True)

    _COUNTS["accepted"] += len(accepted)
    _COUNTS["rejected"] += len(rejected)
    _COUNTS["published"] += published
    return IngestResult(accepted=len(accepted), rejected=len(rejected))


@app.post("/v1/events", status_code=202)
async def receive_events(request: Request) -> IngestResult:
    """Native batch endpoint.

    The body is parsed by hand rather than declared as `batch: EventBatch`,
    because a declared model makes FastAPI reject the entire request on one bad
    row — exactly the behaviour the dead-letter table exists to avoid.
    """
    payload = await request.json()
    rows = payload.get("events", []) if isinstance(payload, dict) else []
    return await _ingest(rows, source="events")


@app.post("/v1/traces", status_code=202)
async def receive_traces(request: Request) -> dict:
    """OTLP/HTTP JSON endpoint.

    Makes the pipeline framework-agnostic in a way that can be demonstrated:
    anything emitting OpenTelemetry GenAI spans lands in the same tables as our
    own SDK, with no new code on either side.
    """
    payload = await request.json()
    events, skipped = otlp.parse(payload)
    result = await _ingest(events, source="otlp")
    return {"accepted": result.accepted, "rejected": result.rejected, "skipped": len(skipped)}


@app.get("/v1/stats")
async def stats() -> dict:
    return {"counts": dict(_COUNTS), "stream": await store.stream_info()}


@app.get("/v1/stream/peek")
async def stream_peek(limit: int = 10) -> dict:
    """Look at the queue without consuming it. Diagnostics, not production."""
    return {"entries": await store.peek(limit)}


@app.get("/v1/dead-letters")
async def dead_letters(limit: int = 20) -> dict:
    async with store.SessionLocal() as session:
        rows = await session.execute(
            text(
                "SELECT id, error, source, created_at, raw "
                "FROM dead_letter_events ORDER BY created_at DESC LIMIT :limit"
            ),
            {"limit": limit},
        )
        return {
            "rows": [
                {
                    "id": row.id,
                    "error": row.error,
                    "source": row.source,
                    "created_at": row.created_at.isoformat(),
                    "raw": row.raw,
                }
                for row in rows
            ]
        }

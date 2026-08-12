"""Ingestion service entrypoint.

Currently a minimal receiver: it validates each event against the shared schema
and holds the last few in memory so the SDK can be verified end to end. The real
pipeline — PII redaction at the edge, publishing to Redis Streams, a dead-letter
table and the OTLP endpoint — lands in P3.

The one behaviour that is already final: rows are validated **individually**, so
one malformed event cannot poison a batch.
"""

from collections import deque

from argus.schema import IngestResult
from fastapi import FastAPI, Request
from pydantic import ValidationError

app = FastAPI(title="argus-ingest", version="0.1.0")

# Replaced by Redis Streams in P3. Bounded so a long-running container cannot
# grow without limit.
_RECENT: deque[dict] = deque(maxlen=200)
_REJECTED: deque[dict] = deque(maxlen=50)
_COUNTS = {"accepted": 0, "rejected": 0}


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok", "service": "ingestion"}


@app.post("/v1/events")
async def receive_events(request: Request) -> IngestResult:
    """Accept a batch of inference events.

    Parsed manually rather than by declaring `batch: EventBatch`, because a
    declared model makes FastAPI reject the whole request on one bad row. A
    collector that drops 49 good events because of the 50th is not a collector.
    """
    from argus.schema import InferenceEvent

    payload = await request.json()
    rows = payload.get("events", []) if isinstance(payload, dict) else []

    accepted = rejected = 0
    for row in rows:
        try:
            event = InferenceEvent.model_validate(row)
        except ValidationError as exc:
            # Quarantined, not discarded — this becomes the dead_letter_events
            # table in P3, with the raw payload and the validation error.
            _REJECTED.append({"raw": row, "error": exc.errors()[:3]})
            rejected += 1
            continue
        _RECENT.appendleft(event.model_dump(mode="json"))
        accepted += 1

    _COUNTS["accepted"] += accepted
    _COUNTS["rejected"] += rejected
    return IngestResult(accepted=accepted, rejected=rejected)


@app.get("/v1/events/recent")
async def recent(limit: int = 20) -> dict:
    """Read back what was received. Exists for verification, not for production."""
    return {"counts": dict(_COUNTS), "events": list(_RECENT)[:limit]}


@app.get("/v1/events/rejected")
async def rejected() -> dict:
    return {"count": _COUNTS["rejected"], "rows": list(_REJECTED)}

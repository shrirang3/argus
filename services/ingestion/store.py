"""Outbound writes: Redis Streams for accepted events, Postgres for rejects.

The asymmetry is deliberate. Accepted events go to the queue because they are
the hot path and the worker owns their durable write. Rejected events go
straight to Postgres because there is nothing downstream to process them — a
dead letter is a thing to read later, not a thing to pipeline.
"""

from __future__ import annotations

import json
import logging

import redis.asyncio as redis
from config import DATABASE_URL, REDIS_URL, STREAM_MAXLEN, STREAM_NAME
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

log = logging.getLogger("argus.ingest.store")

_redis: redis.Redis | None = None

engine = create_async_engine(DATABASE_URL, pool_size=5, max_overflow=5, pool_pre_ping=True)
SessionLocal = async_sessionmaker(engine, expire_on_commit=False)


def get_redis() -> redis.Redis:
    """One client per process; the library pools connections internally."""
    global _redis
    if _redis is None:
        _redis = redis.from_url(REDIS_URL, decode_responses=True)
    return _redis


async def publish(events: list[dict]) -> int:
    """XADD a batch in a single pipeline round trip.

    Pipelining matters at batch size: fifty separate XADDs is fifty round trips,
    which would make the collector's latency scale with batch size for no reason.
    """
    client = get_redis()
    async with client.pipeline(transaction=False) as pipe:
        for event in events:
            pipe.xadd(
                STREAM_NAME,
                {"data": json.dumps(event)},
                maxlen=STREAM_MAXLEN,
                approximate=True,
            )
        await pipe.execute()
    return len(events)


async def dead_letter(rows: list[tuple[dict, str, str]]) -> int:
    """Quarantine invalid payloads with the reason they failed.

    Raw JSON is kept verbatim. Storing a parsed or partially-coerced version
    would destroy the evidence needed to work out what the sender actually sent.
    """
    if not rows:
        return 0
    statement = text(
        "INSERT INTO dead_letter_events (raw, error, source) "
        "VALUES (CAST(:raw AS jsonb), :error, :source)"
    )
    async with SessionLocal() as session:
        await session.execute(
            statement,
            [
                {"raw": json.dumps(raw), "error": error[:2000], "source": source}
                for raw, error, source in rows
            ],
        )
        await session.commit()
    return len(rows)


async def stream_info() -> dict:
    """Stream depth and consumer-group lag, for the dashboard and for debugging."""
    client = get_redis()
    try:
        length = await client.xlen(STREAM_NAME)
    except Exception:  # noqa: BLE001 — the stream may not exist yet
        return {"stream": STREAM_NAME, "length": 0, "groups": []}

    groups = []
    try:
        for group in await client.xinfo_groups(STREAM_NAME):
            groups.append(
                {
                    "name": group.get("name"),
                    "consumers": group.get("consumers"),
                    "pending": group.get("pending"),
                    "lag": group.get("lag"),
                }
            )
    except Exception:  # noqa: BLE001 — no consumer group until the worker starts
        pass

    return {"stream": STREAM_NAME, "length": length, "groups": groups}


async def peek(limit: int = 10) -> list[dict]:
    """Newest entries in the stream, without consuming them.

    XREVRANGE reads; it does not acknowledge or remove. Deliberately separate
    from the worker's XREADGROUP path so looking at the queue can never
    interfere with draining it.
    """
    client = get_redis()
    try:
        entries = await client.xrevrange(STREAM_NAME, count=limit)
    except Exception:  # noqa: BLE001
        return []
    out = []
    for entry_id, fields in entries:
        try:
            out.append({"id": entry_id, "event": json.loads(fields["data"])})
        except (KeyError, json.JSONDecodeError):
            out.append({"id": entry_id, "event": None})
    return out


async def redis_ok() -> bool:
    try:
        return bool(await get_redis().ping())
    except Exception:  # noqa: BLE001
        return False


async def aclose() -> None:
    if _redis is not None:
        await _redis.aclose()
    await engine.dispose()

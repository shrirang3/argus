"""Postgres writes: inference logs, rollups, dead letters.

Two correctness rules live here, and both are easy to get wrong.

**The insert is idempotent; the rollup is not.** `ON CONFLICT DO NOTHING`
absorbs a redelivered event harmlessly, but blindly adding that event to the
one-minute counters would double-count it. So the insert uses `RETURNING` to
report which rows *actually landed*, and only those are aggregated. Without
this, at-least-once delivery would silently inflate every metric on the
dashboard.

**Percentiles are not aggregable.** The p99 of a union is not the union of p99s,
so a rollup of sums cannot produce one. The rollup stores sums, counts and a max
— from which averages and throughput are exact — and true percentiles are
computed from the raw table over the window being asked about. The honest
alternative would be a t-digest per bucket; the dishonest one is averaging
percentiles, which is a number that means nothing.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Integer,
    MetaData,
    Numeric,
    Table,
    Text,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID, insert
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from .config import DATABASE_URL

log = logging.getLogger("argus.worker.writer")

engine = create_async_engine(DATABASE_URL, pool_size=5, max_overflow=5, pool_pre_ping=True)
SessionLocal = async_sessionmaker(engine, expire_on_commit=False)

metadata = MetaData()

# Core table definitions rather than ORM models: this service only writes, and a
# second declarative Base duplicating the chat app's would be two sources of
# truth for one schema.
inference_logs = Table(
    "inference_logs",
    metadata,
    Column("event_id", UUID(as_uuid=True)),
    Column("conversation_id", UUID(as_uuid=True)),
    Column("message_id", UUID(as_uuid=True)),
    Column("session_id", Text),
    Column("service", Text),
    Column("provider", Text),
    Column("model", Text),
    Column("response_model", Text),
    Column("operation", Text),
    Column("status", Text),
    Column("error_type", Text),
    Column("error_message", Text),
    Column("finish_reason", Text),
    Column("latency_ms", Integer),
    Column("ttft_ms", Integer),
    Column("streamed", Boolean),
    Column("prompt_tokens", Integer),
    Column("completion_tokens", Integer),
    Column("total_tokens", Integer),
    Column("cost_usd", Numeric(12, 6)),
    Column("input_preview", Text),
    Column("output_preview", Text),
    Column("redaction_hits", JSONB),
    Column("request_params", JSONB),
    Column("sdk_version", Text),
    Column("started_at", DateTime(timezone=True)),
    Column("ended_at", DateTime(timezone=True)),
)

_COLUMNS = [c.name for c in inference_logs.columns]

_ROLLUP_SQL = text("""
INSERT INTO inference_metrics_1m
    (bucket, provider, model, status, count, sum_latency_ms, sum_ttft_ms,
     ttft_count, sum_tokens, sum_cost_usd, max_latency_ms)
VALUES
    (:bucket, :provider, :model, :status, :count, :sum_latency_ms, :sum_ttft_ms,
     :ttft_count, :sum_tokens, :sum_cost_usd, :max_latency_ms)
ON CONFLICT (bucket, provider, model, status) DO UPDATE SET
    count          = inference_metrics_1m.count          + EXCLUDED.count,
    sum_latency_ms = inference_metrics_1m.sum_latency_ms + EXCLUDED.sum_latency_ms,
    sum_ttft_ms    = inference_metrics_1m.sum_ttft_ms    + EXCLUDED.sum_ttft_ms,
    ttft_count     = inference_metrics_1m.ttft_count     + EXCLUDED.ttft_count,
    sum_tokens     = inference_metrics_1m.sum_tokens     + EXCLUDED.sum_tokens,
    sum_cost_usd   = inference_metrics_1m.sum_cost_usd   + EXCLUDED.sum_cost_usd,
    max_latency_ms = GREATEST(
        COALESCE(inference_metrics_1m.max_latency_ms, 0), COALESCE(EXCLUDED.max_latency_ms, 0))
""")


def _row(event: dict) -> dict:
    """Project an event onto the table's columns.

    Unknown keys are dropped rather than rejected: a newer SDK adding a field
    must not break an older worker. Extra data is a compatibility event, not an
    error.
    """
    row = {name: event.get(name) for name in _COLUMNS}
    for name in ("redaction_hits", "request_params"):
        value = row.get(name)
        if isinstance(value, dict):
            row[name] = json.dumps(value)
    for name in ("started_at", "ended_at"):
        value = row.get(name)
        if isinstance(value, str):
            row[name] = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return row


def _aggregate(events: list[dict]) -> list[dict]:
    """Fold events into one-minute buckets keyed by (bucket, provider, model, status)."""
    buckets: dict[tuple, dict] = {}

    for event in events:
        started = event["started_at"]
        if isinstance(started, str):
            started = datetime.fromisoformat(started.replace("Z", "+00:00"))
        bucket = started.replace(second=0, microsecond=0)

        key = (bucket, event["provider"], event["model"], event["status"])
        agg = buckets.setdefault(
            key,
            {
                "bucket": bucket,
                "provider": event["provider"],
                "model": event["model"],
                "status": event["status"],
                "count": 0,
                "sum_latency_ms": 0,
                "sum_ttft_ms": 0,
                # Counted separately: only streamed calls have a TTFT, so
                # dividing by `count` would understate the average.
                "ttft_count": 0,
                "sum_tokens": 0,
                "sum_cost_usd": 0.0,
                "max_latency_ms": 0,
            },
        )

        agg["count"] += 1
        latency = event.get("latency_ms") or 0
        agg["sum_latency_ms"] += latency
        agg["max_latency_ms"] = max(agg["max_latency_ms"], latency)
        if event.get("ttft_ms") is not None:
            agg["sum_ttft_ms"] += event["ttft_ms"]
            agg["ttft_count"] += 1
        agg["sum_tokens"] += event.get("total_tokens") or 0
        agg["sum_cost_usd"] += float(event.get("cost_usd") or 0)

    return list(buckets.values())


async def write_batch(events: list[dict]) -> tuple[int, int]:
    """Insert events and update rollups in one transaction.

    Returns (inserted, duplicates). The caller acknowledges the messages only
    after this returns — a crash before the commit means redelivery, which the
    conflict clause absorbs.
    """
    if not events:
        return 0, 0

    rows = [_row(event) for event in events]

    async with SessionLocal() as session:
        statement = (
            insert(inference_logs)
            .values(rows)
            # The conflict target is the composite key, because a partitioned
            # table's unique constraint must include the partition column.
            .on_conflict_do_nothing(index_elements=["event_id", "started_at"])
            .returning(inference_logs.c.event_id)
        )
        result = await session.execute(statement)
        inserted_ids = {str(row[0]) for row in result}

        # Only the rows that actually landed are rolled up. Aggregating the
        # whole batch would double-count every redelivery.
        fresh = [e for e in events if str(e.get("event_id")) in inserted_ids]
        for agg in _aggregate(fresh):
            await session.execute(_ROLLUP_SQL, agg)

        await session.commit()

    return len(inserted_ids), len(events) - len(inserted_ids)


async def dead_letter(rows: list[tuple[dict, str, str]]) -> int:
    """Quarantine a payload the worker cannot process."""
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


async def postgres_ok() -> bool:
    try:
        async with SessionLocal() as session:
            await session.execute(text("SELECT 1"))
        return True
    except Exception:  # noqa: BLE001
        return False


async def aclose() -> None:
    await engine.dispose()

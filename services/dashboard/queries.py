"""Dashboard queries.

One rule governs every function here: **windows of 15 minutes or less read raw
rows; wider windows read the one-minute rollup.**

The reason is not performance, it is correctness in two directions.

*Freshness.* The worker writes one-minute buckets, so the rollup is up to a
minute stale. The first thing anyone looks at is "the last minute", and a chart
that read the rollup would show that minute as empty.

*Percentiles.* p99 is not aggregable — the p99 of a union is not the union of
p99s — so it cannot be recovered from sums. Raw rows give exact percentiles.
Wide windows return averages and a max, and say so, rather than inventing a
number that looks precise and means nothing.
"""

from __future__ import annotations

import os
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql+asyncpg://argus:argus@localhost:5433/argus")

engine = create_async_engine(DATABASE_URL, pool_size=5, max_overflow=5, pool_pre_ping=True)
SessionLocal = async_sessionmaker(engine, expire_on_commit=False)

RAW_WINDOW_MINUTES = 15
MAX_WINDOW_MINUTES = 60 * 24 * 30
MAX_ROWS = 200


def clamp(minutes: Any, default: int = 60) -> int:
    """Bound the window so no request can ask for an unbounded scan."""
    try:
        value = int(minutes)
    except (TypeError, ValueError):
        return default
    return max(1, min(value, MAX_WINDOW_MINUTES))


def _clean(rows) -> list[dict]:
    """Decimal and datetime do not survive JSON serialisation."""
    out = []
    for row in rows:
        item = {}
        for key, value in row._mapping.items():
            if value is None:
                item[key] = None
            elif hasattr(value, "isoformat"):
                item[key] = value.isoformat()
            elif hasattr(value, "quantize"):
                item[key] = float(value)
            else:
                item[key] = value
        out.append(item)
    return out


async def _fetch(sql: str, params: dict) -> list[dict]:
    async with SessionLocal() as session:
        return _clean(await session.execute(text(sql), params))


# --------------------------------------------------------------------------- #
# headline numbers
# --------------------------------------------------------------------------- #


async def overview(window_minutes: int) -> dict:
    minutes = clamp(window_minutes)

    rows = await _fetch(
        """
        SELECT count(*)                                                      AS calls,
               count(*) FILTER (WHERE status <> 'success')                   AS failures,
               count(*) FILTER (WHERE status = 'rate_limited')               AS rate_limited,
               count(*) FILTER (WHERE status = 'cancelled')                  AS cancelled,
               coalesce(sum(total_tokens), 0)::bigint                        AS tokens,
               round(coalesce(sum(cost_usd), 0)::numeric, 6)                 AS cost_usd,
               count(*) FILTER (WHERE cost_usd IS NULL)                      AS unpriced,
               round(avg(latency_ms))::int                                   AS avg_ms,
               percentile_cont(0.5)  WITHIN GROUP (ORDER BY latency_ms)::int AS p50_ms,
               percentile_cont(0.95) WITHIN GROUP (ORDER BY latency_ms)::int AS p95_ms,
               percentile_cont(0.99) WITHIN GROUP (ORDER BY latency_ms)::int AS p99_ms,
               percentile_cont(0.5)  WITHIN GROUP (ORDER BY ttft_ms)::int    AS p50_ttft_ms,
               percentile_cont(0.95) WITHIN GROUP (ORDER BY ttft_ms)::int    AS p95_ttft_ms
          FROM inference_logs
         WHERE started_at >= now() - make_interval(mins => :minutes)
        """,
        {"minutes": minutes},
    )
    stats = rows[0] if rows else {}

    calls = stats.get("calls") or 0
    failures = stats.get("failures") or 0
    stats["error_rate_pct"] = round(100 * failures / calls, 2) if calls else 0.0
    stats["calls_per_min"] = round(calls / minutes, 2)
    stats["window_minutes"] = minutes
    # Percentiles here always come from raw rows; the flag tells the UI whether
    # the time series beside them is exact or averaged.
    stats["series_source"] = "raw" if minutes <= RAW_WINDOW_MINUTES else "rollup"
    return stats


# --------------------------------------------------------------------------- #
# time series
# --------------------------------------------------------------------------- #


async def latency_series(window_minutes: int) -> dict:
    minutes = clamp(window_minutes)

    if minutes <= RAW_WINDOW_MINUTES:
        rows = await _fetch(
            """
            SELECT date_trunc('minute', started_at)                              AS bucket,
                   count(*)                                                      AS calls,
                   percentile_cont(0.5)  WITHIN GROUP (ORDER BY latency_ms)::int AS p50,
                   percentile_cont(0.95) WITHIN GROUP (ORDER BY latency_ms)::int AS p95,
                   percentile_cont(0.99) WITHIN GROUP (ORDER BY latency_ms)::int AS p99,
                   percentile_cont(0.5)  WITHIN GROUP (ORDER BY ttft_ms)::int    AS ttft
              FROM inference_logs
             WHERE started_at >= now() - make_interval(mins => :minutes)
               AND latency_ms IS NOT NULL
             GROUP BY 1 ORDER BY 1
            """,
            {"minutes": minutes},
        )
        return {"source": "raw", "exact_percentiles": True, "points": rows}

    rows = await _fetch(
        """
        SELECT bucket,
               sum(count)::int                                        AS calls,
               (sum(sum_latency_ms) / NULLIF(sum(count), 0))::int     AS avg,
               max(max_latency_ms)                                    AS max,
               (sum(sum_ttft_ms) / NULLIF(sum(ttft_count), 0))::int   AS ttft
          FROM inference_metrics_1m
         WHERE bucket >= date_trunc('minute', now()) - make_interval(mins => :minutes)
         GROUP BY bucket ORDER BY bucket
        """,
        {"minutes": minutes},
    )
    return {
        "source": "rollup",
        "exact_percentiles": False,
        "note": (
            "percentiles are not aggregable from one-minute rollups — showing average "
            "and max. Choose 15 minutes or less for exact p50/p95/p99."
        ),
        "points": rows,
    }


async def throughput_series(window_minutes: int) -> dict:
    minutes = clamp(window_minutes)
    rows = await _fetch(
        """
        SELECT bucket, sum(count)::int AS calls, sum(sum_tokens)::bigint AS tokens
          FROM inference_metrics_1m
         WHERE bucket >= date_trunc('minute', now()) - make_interval(mins => :minutes)
         GROUP BY bucket ORDER BY bucket
        """,
        {"minutes": minutes},
    )
    return {"points": rows}


# --------------------------------------------------------------------------- #
# breakdowns
# --------------------------------------------------------------------------- #


async def errors(window_minutes: int) -> dict:
    """Failures by kind.

    rate_limited, timeout and cancelled are kept apart from generic errors: they
    call for different responses, and on a free tier throttling is the expected
    failure rather than a fault.
    """
    minutes = clamp(window_minutes)
    by_kind = await _fetch(
        """
        SELECT status, provider, model, error_type, count(*) AS calls,
               max(started_at) AS last_seen
          FROM inference_logs
         WHERE started_at >= now() - make_interval(mins => :minutes)
           AND status <> 'success'
         GROUP BY 1,2,3,4 ORDER BY calls DESC
         LIMIT :limit
        """,
        {"minutes": minutes, "limit": MAX_ROWS},
    )
    series = await _fetch(
        """
        SELECT bucket,
               coalesce(sum(count) FILTER (WHERE status = 'success'), 0)::int  AS ok,
               coalesce(sum(count) FILTER (WHERE status <> 'success'), 0)::int AS failed
          FROM inference_metrics_1m
         WHERE bucket >= date_trunc('minute', now()) - make_interval(mins => :minutes)
         GROUP BY bucket ORDER BY bucket
        """,
        {"minutes": minutes},
    )
    return {"by_kind": by_kind, "series": series}


async def cost(window_minutes: int, group_by: str = "model") -> dict:
    """Spend, with unpriced calls reported rather than silently counted as zero."""
    minutes = clamp(window_minutes, default=1440)
    column = "provider" if group_by == "provider" else "model"
    rows = await _fetch(
        f"""
        SELECT {column} AS key, count(*) AS calls,
               coalesce(sum(prompt_tokens), 0)::bigint     AS prompt_tokens,
               coalesce(sum(completion_tokens), 0)::bigint AS completion_tokens,
               round(coalesce(sum(cost_usd), 0)::numeric, 6) AS cost_usd,
               count(*) FILTER (WHERE cost_usd IS NULL) AS unpriced_calls
          FROM inference_logs
         WHERE started_at >= now() - make_interval(mins => :minutes)
         GROUP BY 1 ORDER BY cost_usd DESC NULLS LAST
         LIMIT :limit
        """,
        {"minutes": minutes, "limit": MAX_ROWS},
    )
    return {"group_by": column, "rows": rows}


async def models(window_minutes: int) -> dict:
    minutes = clamp(window_minutes)
    rows = await _fetch(
        """
        SELECT provider, model, count(*) AS calls,
               round(avg(latency_ms))::int AS avg_ms,
               percentile_cont(0.95) WITHIN GROUP (ORDER BY latency_ms)::int AS p95_ms,
               count(*) FILTER (WHERE status <> 'success') AS failures
          FROM inference_logs
         WHERE started_at >= now() - make_interval(mins => :minutes)
         GROUP BY 1,2 ORDER BY calls DESC
         LIMIT :limit
        """,
        {"minutes": minutes, "limit": MAX_ROWS},
    )
    return {"rows": rows}


async def recent(limit: int = 25) -> dict:
    rows = await _fetch(
        """
        SELECT started_at, service, provider, model, status, error_type,
               latency_ms, ttft_ms, streamed, total_tokens, cost_usd,
               left(input_preview, 90) AS input_preview,
               conversation_id
          FROM inference_logs
         ORDER BY started_at DESC
         LIMIT :limit
        """,
        {"limit": max(1, min(int(limit or 25), MAX_ROWS))},
    )
    return {"rows": rows}


async def dead_letters(limit: int = 20) -> dict:
    rows = await _fetch(
        """
        SELECT id, error, source, created_at
          FROM dead_letter_events
         ORDER BY created_at DESC
         LIMIT :limit
        """,
        {"limit": max(1, min(int(limit or 20), MAX_ROWS))},
    )
    total = await _fetch("SELECT count(*) AS total FROM dead_letter_events", {})
    return {"total": total[0]["total"] if total else 0, "rows": rows}


async def postgres_ok() -> bool:
    try:
        async with SessionLocal() as session:
            await session.execute(text("SELECT 1"))
        return True
    except Exception:  # noqa: BLE001
        return False


async def aclose() -> None:
    await engine.dispose()

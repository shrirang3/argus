"""Synthetic load generator.

Posts realistic inference events straight at the ingestion endpoint, exercising
the full pipeline — validate, redact, price, XADD, consume, insert, roll up —
without touching a provider.

That bypass is the point, not a shortcut. Pointed at real Groq, a fifty-
concurrent load test measures Groq's free-tier rate limiter rather than this
system. Synthetic events let the pipeline be measured on its own terms, and let
the dashboard be populated with a distribution that actually has a tail.

    uv run python tools/loadgen.py --events 500 --concurrency 20
    uv run python tools/loadgen.py --events 200 --spread-minutes 60
"""

from __future__ import annotations

import argparse
import asyncio
import random
import time
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import httpx

# Weighted so the mix looks like a real workload: a cheap router model called
# often, a large model called less, and a long tail of failures.
MODELS = [
    ("groq", "llama-3.1-8b-instant", 0.45, 180, 0.45),
    ("groq", "llama-3.3-70b-versatile", 0.30, 520, 0.55),
    ("groq", "openai/gpt-oss-120b", 0.15, 900, 0.60),
    ("mock", "mock-1", 0.10, 120, 0.30),
]

# Real traffic is not all successes. Without failures the error panel is empty
# and the p99 sits on top of the p50, which makes the dashboard look broken
# rather than healthy.
STATUSES = [
    ("success", 0.90, None),
    ("error", 0.03, "APIStatusError"),
    ("rate_limited", 0.03, "RateLimitError"),
    ("timeout", 0.02, "APITimeoutError"),
    ("cancelled", 0.02, None),
]

PROMPTS = [
    "what is my p99 latency in the last hour?",
    "which model is burning the most cost today?",
    "show me the errors from the last 15 minutes",
    "summarise throughput for this afternoon",
    "email me at demo@example.com when the p95 drops",
    "why is time to first token so high?",
]


def _weighted(rows: list[tuple], weight_index: int) -> tuple:
    """Pick one row, weighted. The index is explicit because the two tables put
    their weight in different positions, and assuming otherwise is how the first
    version of this silently summed the wrong column."""
    return random.choices(rows, weights=[row[weight_index] for row in rows], k=1)[0]


def make_event(spread_minutes: int) -> dict:
    provider, model, _weight, base_latency, ttft_ratio = _weighted(MODELS, 2)
    status, _weight, error_type = _weighted(STATUSES, 1)

    # Log-normal, because latency distributions are right-skewed: a long tail of
    # slow calls is what makes p99 differ from p50 at all.
    latency = int(random.lognormvariate(0, 0.45) * base_latency)
    started = datetime.now(UTC) - timedelta(seconds=random.uniform(0, spread_minutes * 60))

    prompt_tokens = random.randint(30, 900)
    completion_tokens = random.randint(5, 400)

    event = {
        "event_id": str(uuid4()),
        "conversation_id": str(uuid4()),
        "service": "loadgen",
        "provider": provider,
        "model": model,
        "response_model": model,
        "operation": "chat",
        "status": status,
        "error_type": error_type,
        "streamed": True,
        "started_at": started.isoformat(),
        "ended_at": (started + timedelta(milliseconds=latency)).isoformat(),
        "latency_ms": latency,
        "ttft_ms": int(latency * ttft_ratio),
        "input_preview": random.choice(PROMPTS),
        "output_preview": "synthetic response body",
        "sdk_version": "loadgen",
        "request_params": {"queue_time": round(random.uniform(0.01, 0.4), 4)},
    }

    if status in ("success", "cancelled"):
        event["prompt_tokens"] = prompt_tokens
        event["completion_tokens"] = (
            completion_tokens if status == "success" else completion_tokens // 3
        )
        event["total_tokens"] = event["prompt_tokens"] + event["completion_tokens"]
        event["finish_reason"] = "stop" if status == "success" else None
    else:
        # A failed call has no usage. Filling it in would fabricate spend that
        # never happened.
        event["finish_reason"] = None

    return event


async def send_batch(client: httpx.AsyncClient, url: str, events: list[dict]) -> tuple[int, int]:
    try:
        response = await client.post(url, json={"events": events})
        if response.status_code >= 400:
            return 0, len(events)
        body = response.json()
        return body.get("accepted", 0), body.get("rejected", 0)
    except Exception:  # noqa: BLE001
        return 0, len(events)


async def main() -> None:
    parser = argparse.ArgumentParser(description="Generate synthetic inference events.")
    parser.add_argument("--url", default="http://localhost:8001/v1/events")
    parser.add_argument("--events", type=int, default=500)
    parser.add_argument("--batch", type=int, default=25)
    parser.add_argument("--concurrency", type=int, default=10)
    parser.add_argument(
        "--spread-minutes",
        type=int,
        default=0,
        help="Backdate events across this many minutes, to fill a time series.",
    )
    args = parser.parse_args()

    batches = [
        [make_event(args.spread_minutes) for _ in range(min(args.batch, args.events - i))]
        for i in range(0, args.events, args.batch)
    ]

    semaphore = asyncio.Semaphore(args.concurrency)
    started = time.perf_counter()

    async with httpx.AsyncClient(timeout=15.0) as client:

        async def run(batch):
            async with semaphore:
                return await send_batch(client, args.url, batch)

        results = await asyncio.gather(*(run(batch) for batch in batches))

    accepted = sum(a for a, _ in results)
    rejected = sum(r for _, r in results)
    elapsed = time.perf_counter() - started

    print(f"  sent      {args.events} events in {len(batches)} batches")
    print(f"  accepted  {accepted}")
    print(f"  rejected  {rejected}")
    print(f"  elapsed   {elapsed:.2f}s  ({accepted / elapsed:.0f} events/sec)")
    if rejected:
        print("  check GET /v1/dead-letters for the reasons")


if __name__ == "__main__":
    asyncio.run(main())

"""Ingestion configuration and model pricing."""

from __future__ import annotations

import os

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql+asyncpg://argus:argus@localhost:5433/argus")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6380/0")
STREAM_NAME = os.getenv("STREAM_NAME", "llm.inference.v1")

# Cap the stream so a stalled worker cannot grow Redis without bound. The `~`
# form lets Redis trim on node boundaries, which is much cheaper than exact
# trimming and is why the limit is approximate by design.
STREAM_MAXLEN = int(os.getenv("STREAM_MAXLEN", "1000000"))

REDACT_AT_EDGE = os.getenv("ARGUS_REDACT", "true").lower() != "false"


# USD per million tokens. Kept here rather than in the SDK: pricing is a platform
# concern that changes without any client changing, and an SDK that shipped a
# price list would be wrong in production the day a vendor updates it.
#
# A model missing from this table produces cost_usd = NULL, never 0. Zero is
# indistinguishable from a free call and would quietly understate spend.
PRICING: dict[str, dict[str, float]] = {
    "groq": {
        "llama-3.3-70b-versatile": {"input": 0.59, "output": 0.79},
        "llama-3.1-8b-instant": {"input": 0.05, "output": 0.08},
        "openai/gpt-oss-120b": {"input": 0.15, "output": 0.75},
        "openai/gpt-oss-20b": {"input": 0.10, "output": 0.50},
        "qwen/qwen3.6-27b": {"input": 0.29, "output": 0.39},
    },
    "openai": {
        "gpt-4.1": {"input": 2.00, "output": 8.00},
        "gpt-4.1-mini": {"input": 0.40, "output": 1.60},
        "gpt-4.1-nano": {"input": 0.10, "output": 0.40},
    },
    "mock": {
        "mock-1": {"input": 0.0, "output": 0.0},
    },
}


def cost_usd(
    provider: str, model: str, prompt_tokens: int | None, completion_tokens: int | None
) -> float | None:
    """Cost for one call, or None when the price is unknown.

    Returning None for an unpriced model is deliberate. The dashboard can then
    show "unpriced" as its own category instead of blending unknown spend into
    zero and reporting a total that is confidently wrong.
    """
    prices = PRICING.get(provider, {}).get(model)
    if prices is None or prompt_tokens is None or completion_tokens is None:
        return None
    return round(
        (prompt_tokens * prices["input"] + completion_tokens * prices["output"]) / 1_000_000,
        6,
    )

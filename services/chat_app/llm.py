"""Provider abstraction.

One interface, several backends. `stream_chat` yields text deltas and returns a
final usage record — the shape every provider gets normalised into, so nothing
downstream (routes, the SDK in P2, the schema) knows which vendor answered.

`mock` is the default and needs no API key. That is a deliberate product
decision, not a testing convenience: a reviewer who clones this repo with no
credentials still gets a working `docker compose up` instead of a 401. It is
also what the load test runs against, since a free-tier rate limit would
otherwise measure the provider's throttle rather than our pipeline.
"""

from __future__ import annotations

import asyncio
import os
import random
from collections.abc import AsyncIterator
from dataclasses import dataclass, field

PROVIDER = os.getenv("DEFAULT_PROVIDER", "mock")
ANSWER_MODEL = os.getenv("ANSWER_MODEL", "mock-1")


@dataclass
class Usage:
    """Normalised end-of-stream facts. Providers report these differently."""

    provider: str
    model: str
    prompt_tokens: int = 0
    completion_tokens: int = 0
    finish_reason: str | None = None
    extra: dict = field(default_factory=dict)


class ProviderError(RuntimeError):
    """Raised for provider failures the app should surface to the user."""

    def __init__(self, message: str, *, kind: str = "error") -> None:
        super().__init__(message)
        # kind maps onto the inference_logs status vocabulary in P3.
        self.kind = kind


_MOCK_REPLY = (
    "This is the mock provider. It streams with realistic pacing so cancellation, "
    "partial persistence and the SSE contract can all be exercised without an API "
    "key or a rate limit. Point DEFAULT_PROVIDER at groq or openai and the same "
    "interface returns real tokens — nothing downstream changes, because the "
    "provider is normalised into a single Usage record here."
)


async def _stream_mock(messages: list[dict[str, str]]) -> AsyncIterator[str | Usage]:
    # Rough approximation of a real prompt cost, so the P6 dashboard has
    # something plausible to chart before real providers are wired.
    prompt_tokens = sum(len(m["content"]) for m in messages) // 4

    await asyncio.sleep(random.uniform(0.15, 0.4))  # time to first token

    words = _MOCK_REPLY.split(" ")
    for word in words:
        yield word + " "
        await asyncio.sleep(random.uniform(0.02, 0.06))

    yield Usage(
        provider="mock",
        model=ANSWER_MODEL,
        prompt_tokens=prompt_tokens,
        completion_tokens=len(words),
        finish_reason="stop",
    )


async def stream_chat(
    messages: list[dict[str, str]],
    *,
    provider: str | None = None,
    model: str | None = None,
) -> AsyncIterator[str | Usage]:
    """Stream a reply.

    Yields `str` deltas, then exactly one `Usage` as the final item. Callers
    distinguish by type — a sentinel object rather than a separate callback,
    because the usage is only knowable once the stream ends and this keeps it on
    the same channel as the tokens.
    """
    provider = provider or PROVIDER

    if provider == "mock":
        async for chunk in _stream_mock(messages):
            yield chunk
        return

    # groq / openai / cerebras adapters land next, once a key is available.
    # They are all OpenAI-wire-compatible, so they share one implementation
    # parameterised by base_url and key.
    raise ProviderError(f"provider '{provider}' is not wired yet", kind="error")

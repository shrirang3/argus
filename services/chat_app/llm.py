"""Provider abstraction.

One interface, several backends. `stream_chat` yields text deltas and then
exactly one `Usage` sentinel — the shape every provider is normalised into, so
nothing downstream (routes, the SDK in P2, the schema) knows which vendor
answered.

Providers are reached through their official SDKs rather than raw HTTP. That is
a requirement, not a preference: P2 instruments by patching the SDK's client
class, and a hand-rolled httpx call would be invisible to it.

`mock` is the default and needs no API key. Deliberate: a reviewer cloning this
repo with no credentials gets a working `docker compose up` instead of a 401,
and the load test needs a backend that a free-tier rate limit cannot throttle.
"""

from __future__ import annotations

import asyncio
import os
import random
from collections.abc import AsyncIterator
from dataclasses import dataclass, field

PROVIDER = os.getenv("DEFAULT_PROVIDER", "mock")
ANSWER_MODEL = os.getenv("ANSWER_MODEL", "llama-3.3-70b-versatile")
ROUTER_MODEL = os.getenv("ROUTER_MODEL", "llama-3.1-8b-instant")
MOCK_MODEL = "mock-1"

# Cerebras serves open-weight models (Llama, Qwen) behind an OpenAI-wire
# compatible endpoint — no closed-weight provider is wired into this stack.
CEREBRAS_MODEL = os.getenv("CEREBRAS_MODEL", "llama-3.3-70b")
CEREBRAS_BASE_URL = "https://api.cerebras.ai/v1"


@dataclass
class Usage:
    """Normalised end-of-stream facts. Providers report these differently."""

    provider: str
    model: str
    prompt_tokens: int = 0
    completion_tokens: int = 0
    finish_reason: str | None = None
    # Provider-specific extras worth keeping — Groq reports queue_time, which is
    # time spent waiting before inference started and is invisible in latency
    # alone. Lands in the request_params JSONB column in P3.
    extra: dict = field(default_factory=dict)


class ProviderError(RuntimeError):
    """A provider failure the app should surface to the user."""

    def __init__(self, message: str, *, kind: str = "error") -> None:
        super().__init__(message)
        # kind maps onto the inference_logs status vocabulary in P3.
        self.kind = kind


# --------------------------------------------------------------------------- #
# mock
# --------------------------------------------------------------------------- #

_MOCK_REPLY = (
    "This is the mock provider. It streams with realistic pacing so cancellation, "
    "partial persistence and the SSE contract can all be exercised without an API "
    "key or a rate limit. Set DEFAULT_PROVIDER=groq and the same interface returns "
    "real tokens — nothing downstream changes, because every provider is normalised "
    "into a single Usage record here."
)


async def _stream_mock(messages: list[dict[str, str]]) -> AsyncIterator[str | Usage]:
    # Rough stand-in for real prompt cost, so the P6 dashboard has something
    # plausible to chart when running without keys.
    prompt_tokens = sum(len(m["content"]) for m in messages) // 4

    await asyncio.sleep(random.uniform(0.15, 0.4))  # time to first token

    words = _MOCK_REPLY.split(" ")
    for word in words:
        yield word + " "
        await asyncio.sleep(random.uniform(0.02, 0.06))

    yield Usage(
        provider="mock",
        model=MOCK_MODEL,
        prompt_tokens=prompt_tokens,
        completion_tokens=len(words),
        finish_reason="stop",
    )


# --------------------------------------------------------------------------- #
# groq
# --------------------------------------------------------------------------- #

_groq_client = None


def _get_groq():
    """One client per process.

    The SDK holds an httpx connection pool; constructing it per request would
    open a fresh TLS connection every time and discard it. Also gives P2 a
    single, stable object graph to instrument.
    """
    global _groq_client
    if _groq_client is None:
        from groq import AsyncGroq

        api_key = os.getenv("GROQ_API_KEY")
        if not api_key:
            raise ProviderError("GROQ_API_KEY is not set", kind="error")
        _groq_client = AsyncGroq(api_key=api_key, max_retries=2, timeout=60.0)
    return _groq_client


def _classify_groq(exc: Exception) -> str:
    """Map SDK exceptions onto our status vocabulary.

    Rate limiting deserves its own status rather than being lumped into
    'error': on a free tier it is the expected failure, and the P6 dashboard
    should show throttling separately from genuine faults.
    """
    import groq

    if isinstance(exc, groq.RateLimitError):
        return "rate_limited"
    if isinstance(exc, groq.APITimeoutError):
        return "timeout"
    return "error"


async def _stream_groq(messages: list[dict[str, str]], model: str) -> AsyncIterator[str | Usage]:
    client = _get_groq()

    try:
        stream = await client.chat.completions.create(
            model=model,
            messages=messages,
            stream=True,
        )

        usage: Usage | None = None

        async for chunk in stream:
            # Groq attaches usage to the final chunk on its own. OpenAI requires
            # stream_options={"include_usage": True} for the same thing, which is
            # why this cannot be shared with an OpenAI adapter verbatim.
            raw_usage = getattr(chunk, "usage", None)
            if raw_usage is not None:
                usage = Usage(
                    provider="groq",
                    model=getattr(chunk, "model", model),
                    prompt_tokens=raw_usage.prompt_tokens or 0,
                    completion_tokens=raw_usage.completion_tokens or 0,
                    extra={
                        k: v
                        for k in ("queue_time", "prompt_time", "completion_time", "total_time")
                        if (v := getattr(raw_usage, k, None)) is not None
                    },
                )

            if not chunk.choices:
                continue

            choice = chunk.choices[0]
            if choice.finish_reason and usage is not None:
                usage.finish_reason = choice.finish_reason

            delta = choice.delta.content
            if delta:
                yield delta

        yield usage or Usage(provider="groq", model=model, finish_reason="stop")

    except ProviderError:
        raise
    except Exception as exc:  # noqa: BLE001 — deliberately broad, then classified
        raise ProviderError(str(exc), kind=_classify_groq(exc)) from exc


# --------------------------------------------------------------------------- #
# cerebras (open-weight models, OpenAI-wire compatible)
# --------------------------------------------------------------------------- #

_cerebras_client = None


def _get_cerebras():
    """Reached through the `openai` package pointed at Cerebras' base_url.

    Same wire shape as OpenAI, but every model behind it is open-weight
    (Llama, Qwen) — this is not a route to closed-weight OpenAI models. Reusing
    the `openai` SDK rather than a bespoke client also means P2's instrument
    patch on `openai.resources.chat.completions` already covers it for free.
    """
    global _cerebras_client
    if _cerebras_client is None:
        from openai import AsyncOpenAI

        api_key = os.getenv("CEREBRAS_API_KEY")
        if not api_key:
            raise ProviderError("CEREBRAS_API_KEY is not set", kind="error")
        _cerebras_client = AsyncOpenAI(
            api_key=api_key, base_url=CEREBRAS_BASE_URL, max_retries=2, timeout=60.0
        )
    return _cerebras_client


def _classify_openai_wire(exc: Exception) -> str:
    import openai

    if isinstance(exc, openai.RateLimitError):
        return "rate_limited"
    if isinstance(exc, openai.APITimeoutError):
        return "timeout"
    return "error"


async def _stream_cerebras(messages: list[dict[str, str]], model: str) -> AsyncIterator[str | Usage]:
    client = _get_cerebras()

    try:
        stream = await client.chat.completions.create(
            model=model,
            messages=messages,
            stream=True,
            # Unlike Groq, OpenAI-wire servers only attach usage to the final
            # chunk when explicitly asked.
            stream_options={"include_usage": True},
        )

        usage: Usage | None = None

        async for chunk in stream:
            raw_usage = getattr(chunk, "usage", None)
            if raw_usage is not None:
                usage = Usage(
                    provider="cerebras",
                    model=getattr(chunk, "model", model),
                    prompt_tokens=raw_usage.prompt_tokens or 0,
                    completion_tokens=raw_usage.completion_tokens or 0,
                )

            if not chunk.choices:
                continue

            choice = chunk.choices[0]
            if choice.finish_reason and usage is not None:
                usage.finish_reason = choice.finish_reason

            delta = choice.delta.content
            if delta:
                yield delta

        yield usage or Usage(provider="cerebras", model=model, finish_reason="stop")

    except ProviderError:
        raise
    except Exception as exc:  # noqa: BLE001 — deliberately broad, then classified
        raise ProviderError(str(exc), kind=_classify_openai_wire(exc)) from exc


# --------------------------------------------------------------------------- #
# dispatch
# --------------------------------------------------------------------------- #


async def stream_chat(
    messages: list[dict[str, str]],
    *,
    provider: str | None = None,
    model: str | None = None,
) -> AsyncIterator[str | Usage]:
    """Stream a reply.

    Yields `str` deltas, then exactly one `Usage` as the final item. Callers
    distinguish by type — a sentinel rather than a callback, because usage is
    only knowable once the stream ends and this keeps it on one channel.
    """
    provider = provider or PROVIDER

    if provider == "mock":
        async for chunk in _stream_mock(messages):
            yield chunk
        return

    if provider == "groq":
        async for chunk in _stream_groq(messages, model or ANSWER_MODEL):
            yield chunk
        return

    if provider == "cerebras":
        async for chunk in _stream_cerebras(messages, model or CEREBRAS_MODEL):
            yield chunk
        return

    raise ProviderError(f"provider '{provider}' is not wired yet", kind="error")

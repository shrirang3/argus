"""Response normalisation.

Groq, OpenAI and Cerebras all speak the OpenAI wire shape, so one extractor
covers them. Anthropic differs and gets its own branch when it is wired.

Everything here uses `getattr` rather than attribute access. These are response
models from a dependency we do not control: fields appear on some chunks and not
others, and shapes shift between SDK versions. An instrumentation layer that
raises because a vendor added a field is worse than one that records slightly
less.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class Extraction:
    """What could be read out of a provider response."""

    text: str | None = None
    prompt_tokens: int | None = None
    completion_tokens: int | None = None
    total_tokens: int | None = None
    response_model: str | None = None
    finish_reason: str | None = None
    extra: dict = field(default_factory=dict)


# Timing fields Groq reports and OpenAI does not. queue_time is the interesting
# one: time spent waiting before inference began, which is invisible in
# end-to-end latency alone.
_TIMING_FIELDS = ("queue_time", "prompt_time", "completion_time", "total_time")


def _read_usage(usage) -> dict:
    if usage is None:
        return {}
    out = {
        "prompt_tokens": getattr(usage, "prompt_tokens", None),
        "completion_tokens": getattr(usage, "completion_tokens", None),
        "total_tokens": getattr(usage, "total_tokens", None),
    }
    extra = {
        name: value for name in _TIMING_FIELDS if (value := getattr(usage, name, None)) is not None
    }
    if extra:
        out["extra"] = extra
    return {k: v for k, v in out.items() if v is not None}


def extract_from_response(response) -> Extraction:
    """Non-streaming completion."""
    result = Extraction(response_model=getattr(response, "model", None))

    usage = _read_usage(getattr(response, "usage", None))
    result.prompt_tokens = usage.get("prompt_tokens")
    result.completion_tokens = usage.get("completion_tokens")
    result.total_tokens = usage.get("total_tokens")
    result.extra = usage.get("extra", {})

    choices = getattr(response, "choices", None) or []
    if choices:
        choice = choices[0]
        result.finish_reason = getattr(choice, "finish_reason", None)
        message = getattr(choice, "message", None)
        result.text = getattr(message, "content", None) if message else None

    return result


def extract_from_chunk(chunk) -> Extraction:
    """One streamed chunk.

    Usage arrives on the final chunk for Groq automatically; OpenAI only sends
    it when asked via stream_options. Either way it appears on exactly one
    chunk, so the caller accumulates rather than overwrites.
    """
    result = Extraction(response_model=getattr(chunk, "model", None))

    usage = _read_usage(getattr(chunk, "usage", None))
    result.prompt_tokens = usage.get("prompt_tokens")
    result.completion_tokens = usage.get("completion_tokens")
    result.total_tokens = usage.get("total_tokens")
    result.extra = usage.get("extra", {})

    choices = getattr(chunk, "choices", None) or []
    if choices:
        choice = choices[0]
        result.finish_reason = getattr(choice, "finish_reason", None)
        delta = getattr(choice, "delta", None)
        result.text = getattr(delta, "content", None) if delta else None

    return result

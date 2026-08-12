"""argus — every LLM call, watched.

Auto-instrumenting observability SDK for LLM inference. One call at startup;
every provider call in the process is captured after that, including calls made
by code you did not write.

    import argus
    argus.init(endpoint="http://ingestion:8001/v1/events", service="chat-app")

    with argus.conversation(conversation_id):
        resp = client.chat.completions.create(...)   # logged, unchanged
"""

from __future__ import annotations

import os
from dataclasses import dataclass

from argus.context import (
    conversation,
    current_conversation_id,
    current_message_id,
    current_session_id,
    session,
)
from argus.schema import EventBatch, InferenceEvent, Status

__version__ = "0.1.0"

__all__ = [
    "init",
    "shutdown",
    "stats",
    "conversation",
    "session",
    "current_conversation_id",
    "current_message_id",
    "current_session_id",
    "InferenceEvent",
    "EventBatch",
    "Status",
    "Config",
    "__version__",
]


@dataclass
class Config:
    """Resolved SDK configuration.

    Every field is overridable by environment variable so the SDK can be tuned
    in a deployed container without a code change — which matters because the
    knobs that need turning (buffer size, flush interval) are exactly the ones
    you discover under load.
    """

    endpoint: str
    service: str
    enabled: bool = True
    queue_maxsize: int = 10_000
    batch_size: int = 50
    flush_interval_s: float = 0.5
    timeout_s: float = 5.0
    max_retries: int = 3
    spill_path: str = "/tmp/argus-spill.jsonl"


_config: Config | None = None


def init(
    endpoint: str | None = None,
    service: str | None = None,
    **overrides,
) -> Config:
    """Configure the SDK and install provider instrumentation.

    Idempotent: calling twice reconfigures without double-patching, which
    matters under reload-based dev servers that re-import the module.
    """
    global _config

    _config = Config(
        endpoint=endpoint or os.getenv("ARGUS_ENDPOINT", "http://localhost:8001/v1/events"),
        service=service or os.getenv("ARGUS_SERVICE", "unknown"),
        enabled=os.getenv("ARGUS_ENABLED", "true").lower() != "false",
        queue_maxsize=int(os.getenv("ARGUS_QUEUE_MAXSIZE", "10000")),
        batch_size=int(os.getenv("ARGUS_BATCH_SIZE", "50")),
        flush_interval_s=float(os.getenv("ARGUS_FLUSH_INTERVAL", "0.5")),
        timeout_s=float(os.getenv("ARGUS_TIMEOUT", "5.0")),
        max_retries=int(os.getenv("ARGUS_MAX_RETRIES", "3")),
        spill_path=os.getenv("ARGUS_SPILL_PATH", "/tmp/argus-spill.jsonl"),
    )
    for key, value in overrides.items():
        setattr(_config, key, value)

    from argus import emitter, instrument

    emitter.configure(_config)
    instrument.install(_config)
    return _config


async def shutdown(timeout_s: float = 5.0) -> None:
    """Flush buffered events. Call from the application's shutdown hook."""
    from argus import emitter

    await emitter.aclose(timeout_s)


def stats() -> dict[str, int]:
    """Emitter counters — emitted, sent, dropped, spilled, send_failures.

    Exposed so the dashboard can show data loss. A half-empty chart looks the
    same as a quiet system; these numbers tell them apart.
    """
    from argus import emitter

    return emitter.stats()


def get_config() -> Config | None:
    return _config

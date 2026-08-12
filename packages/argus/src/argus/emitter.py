"""Non-blocking event transport.

The invariant this module exists to protect: **the application's request path
never waits on the logging path.** Observability that can take the product down
is worse than no observability, because it converts a monitoring outage into a
customer-facing one.

So `emit()` does nothing but append to an in-memory buffer and return. A
background task drains it in batches. When things go wrong the failure mode
degrades in stages rather than propagating:

    buffer (bounded) → batched POST → retry with backoff → spill to disk

and only then, if the buffer itself overflows, are events dropped — oldest
first, and counted.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from collections import deque
from pathlib import Path

import httpx

from argus.schema import EventBatch, InferenceEvent

log = logging.getLogger("argus.emitter")


class Stats:
    """Counters the dashboard reads back, so loss is visible rather than silent.

    A half-empty dashboard looks identical to a quiet system. These make the
    difference observable.
    """

    def __init__(self) -> None:
        self.emitted = 0
        self.sent = 0
        self.dropped = 0
        self.spilled = 0
        self.send_failures = 0

    def as_dict(self) -> dict[str, int]:
        return {
            "emitted": self.emitted,
            "sent": self.sent,
            "dropped": self.dropped,
            "spilled": self.spilled,
            "send_failures": self.send_failures,
        }


class Emitter:
    def __init__(self, config) -> None:
        self.config = config
        self.stats = Stats()
        # maxlen gives us drop-oldest for free. Dropping the oldest is
        # deliberate: during an incident the newest events describe the
        # incident, the oldest describe the calm before it.
        self._buffer: deque[InferenceEvent] = deque(maxlen=config.queue_maxsize)
        self._task: asyncio.Task | None = None
        self._client: httpx.AsyncClient | None = None
        self._stopping = False

    # ---------------------------------------------------------------- emit

    def emit(self, event: InferenceEvent) -> None:
        """Record an event. Never blocks, never raises, never awaits."""
        if not self.config.enabled:
            return

        self.stats.emitted += 1
        if len(self._buffer) == self._buffer.maxlen:
            self.stats.dropped += 1
        self._buffer.append(event)
        self._ensure_running()

    def _ensure_running(self) -> None:
        """Start the drain task lazily, on the loop that is actually running.

        init() is typically called at import time, before any event loop
        exists. Creating the task here rather than there avoids binding to the
        wrong loop — a classic source of "task attached to a different loop"
        errors under test runners.
        """
        if self._task is not None and not self._task.done():
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return  # no loop yet; the next emit from async code will start it
        self._task = loop.create_task(self._run())

    # --------------------------------------------------------------- drain

    async def _run(self) -> None:
        while not self._stopping:
            await asyncio.sleep(self.config.flush_interval_s)
            await self._flush_once()

    async def _flush_once(self) -> None:
        if not self._buffer:
            return

        batch: list[InferenceEvent] = []
        while self._buffer and len(batch) < self.config.batch_size:
            batch.append(self._buffer.popleft())

        try:
            await self._send(batch)
        except Exception:  # noqa: BLE001 — the emitter must never raise upward
            log.debug("argus: batch send failed, spilling", exc_info=True)
            self._spill(batch)

    async def _send(self, batch: list[InferenceEvent]) -> None:
        if self._client is None:
            self._client = httpx.AsyncClient(timeout=self.config.timeout_s)

        payload = EventBatch(events=batch).model_dump(mode="json")

        delay = 0.25
        last_error: Exception | None = None
        for attempt in range(self.config.max_retries + 1):
            try:
                response = await self._client.post(self.config.endpoint, json=payload)
                if response.status_code < 400:
                    self.stats.sent += len(batch)
                    return
                # 4xx means the payload is wrong; retrying will not fix it.
                if response.status_code < 500:
                    raise RuntimeError(f"ingest rejected batch: {response.status_code}")
                last_error = RuntimeError(f"ingest {response.status_code}")
            except Exception as exc:  # noqa: BLE001
                last_error = exc
                if isinstance(exc, RuntimeError) and "rejected" in str(exc):
                    break

            if attempt < self.config.max_retries:
                await asyncio.sleep(delay)
                delay *= 2  # exponential backoff — a struggling collector is
                #             not helped by being retried harder

        self.stats.send_failures += 1
        raise last_error or RuntimeError("send failed")

    def _spill(self, batch: list[InferenceEvent]) -> None:
        """Last resort: append to disk so the batch can be replayed later.

        Disk is slower than the network but it is not lossy, and an emitter that
        silently discards data during exactly the incident you want to
        investigate is worse than useless.
        """
        try:
            path = Path(self.config.spill_path)
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open("a", encoding="utf-8") as fh:
                for event in batch:
                    fh.write(json.dumps(event.model_dump(mode="json")) + "\n")
            self.stats.spilled += len(batch)
        except Exception:  # noqa: BLE001
            self.stats.dropped += len(batch)
            log.debug("argus: spill failed, events dropped", exc_info=True)

    # ------------------------------------------------------------ shutdown

    async def aclose(self, timeout_s: float = 5.0) -> None:
        """Flush what is buffered, then stop. Called on application shutdown."""
        self._stopping = True
        deadline = time.monotonic() + timeout_s
        while self._buffer and time.monotonic() < deadline:
            await self._flush_once()
        if self._task is not None:
            self._task.cancel()
        if self._client is not None:
            await self._client.aclose()
            self._client = None


_emitter: Emitter | None = None


def configure(config) -> Emitter:
    global _emitter
    _emitter = Emitter(config)
    return _emitter


def get() -> Emitter | None:
    return _emitter


def emit(event: InferenceEvent) -> None:
    if _emitter is not None:
        _emitter.emit(event)


def stats() -> dict[str, int]:
    return _emitter.stats.as_dict() if _emitter else {}


async def aclose(timeout_s: float = 5.0) -> None:
    if _emitter is not None:
        await _emitter.aclose(timeout_s)


def spill_path_default() -> str:
    return os.getenv("ARGUS_SPILL_PATH", "/tmp/argus-spill.jsonl")

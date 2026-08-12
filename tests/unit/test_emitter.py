"""Emitter behaviour under failure.

The happy path is uninteresting. What matters is that every degradation step
behaves as documented: bounded buffer, drop-oldest, retry, spill to disk, and
never raising into the caller.
"""

import json
from datetime import UTC, datetime

import pytest
from argus import Config
from argus.emitter import Emitter
from argus.schema import InferenceEvent


def make_event(model: str = "m") -> InferenceEvent:
    return InferenceEvent(
        service="test",
        provider="groq",
        model=model,
        started_at=datetime.now(UTC),
    )


def config(**overrides) -> Config:
    base = {
        "endpoint": "http://127.0.0.1:1/v1/events",  # nothing listens here
        "service": "test",
        "queue_maxsize": 5,
        "batch_size": 3,
        "flush_interval_s": 0.05,
        "timeout_s": 0.2,
        "max_retries": 0,
    }
    base.update(overrides)
    return Config(**base)


def test_emit_never_blocks_or_raises():
    em = Emitter(config())
    em.emit(make_event())
    assert em.stats.emitted == 1


def test_disabled_emitter_records_nothing():
    em = Emitter(config(enabled=False))
    em.emit(make_event())
    assert em.stats.emitted == 0


def test_buffer_is_bounded_and_drops_oldest(tmp_path):
    """Overflow must drop the OLDEST — during an incident the newest events
    describe the incident."""
    em = Emitter(config(queue_maxsize=3, spill_path=str(tmp_path / "s.jsonl")))
    for i in range(5):
        em.emit(make_event(model=f"m{i}"))

    assert len(em._buffer) == 3
    assert em.stats.dropped == 2
    assert [e.model for e in em._buffer] == ["m2", "m3", "m4"]


async def test_unreachable_endpoint_spills_to_disk(tmp_path):
    spill = tmp_path / "spill.jsonl"
    em = Emitter(config(spill_path=str(spill)))

    em.emit(make_event(model="spilled"))
    await em._flush_once()

    assert spill.exists()
    rows = [json.loads(line) for line in spill.read_text().splitlines()]
    assert rows[0]["model"] == "spilled"
    assert em.stats.spilled == 1
    assert em.stats.sent == 0


async def test_flush_does_not_raise_when_everything_fails(tmp_path):
    """The emitter must absorb its own failures — it is not the app's problem."""
    em = Emitter(config(spill_path="/proc/nonexistent/spill.jsonl"))
    em.emit(make_event())
    await em._flush_once()  # must not raise
    assert em.stats.dropped >= 1


async def test_successful_send_marks_sent(tmp_path):
    sent_batches = []

    class FakeResponse:
        status_code = 200

    class FakeClient:
        async def post(self, url, json):
            sent_batches.append(json)
            return FakeResponse()

        async def aclose(self):
            pass

    em = Emitter(config())
    em._client = FakeClient()

    em.emit(make_event(model="ok"))
    await em._flush_once()

    assert em.stats.sent == 1
    assert em.stats.spilled == 0
    assert sent_batches[0]["events"][0]["model"] == "ok"


async def test_batch_size_is_respected():
    seen = []

    class FakeResponse:
        status_code = 200

    class FakeClient:
        async def post(self, url, json):
            seen.append(len(json["events"]))
            return FakeResponse()

        async def aclose(self):
            pass

    em = Emitter(config(queue_maxsize=100, batch_size=3))
    em._client = FakeClient()

    for _ in range(7):
        em.emit(make_event())
    await em._flush_once()
    await em._flush_once()

    assert seen == [3, 3]


@pytest.mark.parametrize("status_code", [400, 422])
async def test_client_errors_are_not_retried(status_code, tmp_path):
    """A 4xx means the payload is wrong; retrying cannot fix it."""
    attempts = []

    class FakeResponse:
        def __init__(self, code):
            self.status_code = code

    class FakeClient:
        async def post(self, url, json):
            attempts.append(1)
            return FakeResponse(status_code)

        async def aclose(self):
            pass

    em = Emitter(config(max_retries=3, spill_path=str(tmp_path / "s.jsonl")))
    em._client = FakeClient()
    em.emit(make_event())
    await em._flush_once()

    assert len(attempts) == 1

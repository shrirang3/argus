"""Auto-instrumentation.

The three rules the wrapper must never break, tested directly:

1. call through — the original method still runs
2. re-raise the original exception — never swallowed, never re-wrapped
3. return the response untouched — the application cannot tell we were here

Patching is exercised against stand-in classes rather than the real Groq SDK, so
these tests need no network and no API key. The live path is verified separately
against the running stack.
"""

import asyncio
from types import SimpleNamespace

import pytest
from argus import Config, context, instrument
from argus.schema import Status


@pytest.fixture
def captured(monkeypatch):
    """Collect emitted events instead of sending them."""
    events = []
    monkeypatch.setattr("argus.emitter.emit", events.append)
    monkeypatch.setattr("argus.instrument.emitter.emit", events.append)
    return events


@pytest.fixture
def config():
    return Config(endpoint="http://unused", service="test-svc")


def usage(prompt=10, completion=5):
    return SimpleNamespace(
        prompt_tokens=prompt, completion_tokens=completion, total_tokens=prompt + completion
    )


def response(text="hello"):
    return SimpleNamespace(
        model="llama-3.3-70b-versatile",
        usage=usage(),
        choices=[SimpleNamespace(finish_reason="stop", message=SimpleNamespace(content=text))],
    )


def chunk(text=None, with_usage=False, finish=None):
    return SimpleNamespace(
        model="llama-3.3-70b-versatile",
        usage=usage() if with_usage else None,
        choices=[SimpleNamespace(finish_reason=finish, delta=SimpleNamespace(content=text))],
    )


# --------------------------------------------------------------------- sync


def test_sync_call_is_captured_and_response_unchanged(captured, config):
    sentinel = response("the answer")

    class Completions:
        def create(self, **kwargs):
            return sentinel

    instrument._patch(Completions, "create", "groq", config, is_async=False)
    try:
        returned = Completions().create(model="m", messages=[{"role": "user", "content": "hi"}])
    finally:
        instrument.uninstall()

    assert returned is sentinel  # rule 3: untouched, same object
    assert len(captured) == 1
    event = captured[0]
    assert event.provider == "groq"
    assert event.service == "test-svc"
    assert event.status is Status.SUCCESS
    assert event.prompt_tokens == 10
    assert event.completion_tokens == 5
    assert event.latency_ms is not None
    assert event.output_preview == "the answer"
    assert event.input_preview == "hi"


def test_original_exception_is_reraised(captured, config):
    class RateLimitError(Exception):
        pass

    class Completions:
        def create(self, **kwargs):
            raise RateLimitError("slow down")

    instrument._patch(Completions, "create", "groq", config, is_async=False)
    try:
        with pytest.raises(RateLimitError, match="slow down"):
            Completions().create(model="m", messages=[])
    finally:
        instrument.uninstall()

    assert captured[0].status is Status.RATE_LIMITED
    assert captured[0].error_type == "RateLimitError"


def test_timeout_is_classified_separately(captured, config):
    class APITimeoutError(Exception):
        pass

    class Completions:
        def create(self, **kwargs):
            raise APITimeoutError("took too long")

    instrument._patch(Completions, "create", "groq", config, is_async=False)
    try:
        with pytest.raises(APITimeoutError):
            Completions().create(model="m", messages=[])
    finally:
        instrument.uninstall()

    assert captured[0].status is Status.TIMEOUT


def test_prompt_preview_is_redacted(captured, config):
    class Completions:
        def create(self, **kwargs):
            return response()

    instrument._patch(Completions, "create", "groq", config, is_async=False)
    try:
        Completions().create(
            model="m", messages=[{"role": "user", "content": "email me at a@b.com"}]
        )
    finally:
        instrument.uninstall()

    assert "a@b.com" not in captured[0].input_preview
    assert captured[0].redaction_hits.get("email") == 1


# -------------------------------------------------------------------- async


class FakeStream:
    def __init__(self, chunks, raise_at=None):
        self._chunks = list(chunks)
        self._raise_at = raise_at
        self._i = 0

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self._raise_at is not None and self._i == self._raise_at:
            raise asyncio.CancelledError()
        if self._i >= len(self._chunks):
            raise StopAsyncIteration
        item = self._chunks[self._i]
        self._i += 1
        await asyncio.sleep(0.005)
        return item


async def test_streamed_call_records_ttft_tokens_and_output(captured, config):
    chunks = [chunk("hel"), chunk("lo "), chunk("world"), chunk(with_usage=True, finish="stop")]

    class AsyncCompletions:
        async def create(self, **kwargs):
            return FakeStream(chunks)

    instrument._patch(AsyncCompletions, "create", "groq", config, is_async=True)
    try:
        stream = await AsyncCompletions().create(
            model="m", messages=[{"role": "user", "content": "hi"}], stream=True
        )
        received = [c async for c in stream]
    finally:
        instrument.uninstall()

    assert len(received) == 4  # every chunk passed through
    assert len(captured) == 1
    event = captured[0]
    assert event.streamed is True
    assert event.status is Status.SUCCESS
    assert event.output_preview == "hello world"
    assert event.completion_tokens == 5
    assert event.finish_reason == "stop"
    # TTFT is measured at the first chunk carrying text, so it must be at or
    # below total latency.
    assert event.ttft_ms is not None
    assert event.ttft_ms <= event.latency_ms


async def test_cancelled_stream_emits_partial(captured, config):
    chunks = [chunk("par"), chunk("tial")]

    class AsyncCompletions:
        async def create(self, **kwargs):
            return FakeStream(chunks, raise_at=2)

    instrument._patch(AsyncCompletions, "create", "groq", config, is_async=True)
    try:
        stream = await AsyncCompletions().create(model="m", messages=[], stream=True)
        with pytest.raises(asyncio.CancelledError):
            async for _ in stream:
                pass
    finally:
        instrument.uninstall()

    event = captured[0]
    assert event.status is Status.CANCELLED
    assert event.output_preview == "partial"  # what was produced is kept


async def test_conversation_id_flows_through_contextvars(captured, config):
    cid = "11111111-2222-3333-4444-555555555555"

    class AsyncCompletions:
        async def create(self, **kwargs):
            return response()

    instrument._patch(AsyncCompletions, "create", "groq", config, is_async=True)
    try:
        with context.conversation(cid):
            await AsyncCompletions().create(model="m", messages=[])
    finally:
        instrument.uninstall()

    assert str(captured[0].conversation_id) == cid


async def test_no_conversation_context_is_not_an_error(captured, config):
    class AsyncCompletions:
        async def create(self, **kwargs):
            return response()

    instrument._patch(AsyncCompletions, "create", "groq", config, is_async=True)
    try:
        await AsyncCompletions().create(model="m", messages=[])
    finally:
        instrument.uninstall()

    assert captured[0].conversation_id is None


async def test_concurrent_conversations_do_not_leak_ids(captured, config):
    """contextvars must isolate concurrent tasks — the reason for token/reset."""

    class AsyncCompletions:
        async def create(self, **kwargs):
            await asyncio.sleep(0.01)
            return response()

    instrument._patch(AsyncCompletions, "create", "groq", config, is_async=True)

    async def call(cid):
        with context.conversation(cid):
            await AsyncCompletions().create(model="m", messages=[])

    ids = [f"{i:08d}-0000-0000-0000-000000000000" for i in range(1, 6)]
    try:
        await asyncio.gather(*(call(cid) for cid in ids))
    finally:
        instrument.uninstall()

    assert sorted(str(e.conversation_id) for e in captured) == sorted(ids)


async def test_openai_wire_provider_is_resolved_by_base_url(captured, config):
    """Cerebras and (were it wired) OpenAI share the same patched classes —
    only `base_url` at call time tells them apart.
    """

    class FakeClient:
        def __init__(self, base_url):
            self.base_url = base_url

    class AsyncCompletions:
        def __init__(self, base_url):
            self._client = FakeClient(base_url)

        async def create(self, **kwargs):
            return response()

    instrument._patch(
        AsyncCompletions, "create", instrument._resolve_openai_wire_provider, config, is_async=True
    )
    try:
        await AsyncCompletions("https://api.cerebras.ai/v1").create(model="m", messages=[])
        await AsyncCompletions("https://api.openai.com/v1").create(model="m", messages=[])
    finally:
        instrument.uninstall()

    assert [e.provider for e in captured] == ["cerebras", "openai"]


def test_install_is_idempotent(config):
    first = instrument.install(config)
    second = instrument.install(config)
    try:
        assert second == []  # second call patches nothing
    finally:
        instrument.uninstall()
    assert isinstance(first, list)

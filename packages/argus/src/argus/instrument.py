"""Auto-instrumentation.

Python classes are mutable at runtime, so a method can be replaced after import
and every existing and future instance picks up the replacement — attribute
lookup goes through the class, not a copy held by the object.

That is what makes this work on code we do not own. An explicit `wrap(client)`
helper only sees clients we construct ourselves; when LangGraph, a library or a
background job builds its own client and calls it, a wrapper captures nothing.
Patching the class captures all of it.

Three rules the wrapper never breaks:

1. **Call through, don't reimplement.** We are a decorator, not a replacement.
2. **Re-raise the original exception.** Never swallow, never re-wrap.
3. **Return the response untouched.** The application must not be able to tell
   we were here.
"""

from __future__ import annotations

import logging
import time
from datetime import UTC, datetime
from uuid import uuid4

from argus import context, emitter, redact
from argus.adapters import extract_from_chunk, extract_from_response
from argus.schema import InferenceEvent, Status

log = logging.getLogger("argus.instrument")

_installed = False
_originals: list[tuple[type, str, object]] = []


# --------------------------------------------------------------------------- #
# event construction
# --------------------------------------------------------------------------- #


def _messages_preview(kwargs) -> tuple[str | None, dict[str, int]]:
    """Preview of the prompt — the last user message, redacted and truncated.

    The last user message rather than the whole prompt: it is what actually
    distinguishes one call from another, and the rest is conversation history
    already stored elsewhere.
    """
    messages = kwargs.get("messages") or []
    for message in reversed(messages):
        role = message.get("role") if isinstance(message, dict) else getattr(message, "role", None)
        if role == "user":
            content = (
                message.get("content")
                if isinstance(message, dict)
                else getattr(message, "content", None)
            )
            if isinstance(content, str):
                return redact.preview(content)
            break
    return None, {}


def _new_event(config, provider: str, kwargs: dict) -> InferenceEvent:
    preview, hits = _messages_preview(kwargs)

    # Sampled params only. Recording `messages` here would duplicate the entire
    # prompt into the log table, which is the thing previews exist to avoid.
    params = {
        key: kwargs[key]
        for key in ("temperature", "max_tokens", "top_p", "tools", "tool_choice")
        if key in kwargs and kwargs[key] is not None
    }
    if "tools" in params:
        params["tools"] = len(params["tools"])  # count, not the schemas

    from argus import __version__

    return InferenceEvent(
        event_id=uuid4(),
        conversation_id=context.current_conversation_id(),
        message_id=context.current_message_id(),
        session_id=context.current_session_id(),
        service=config.service,
        provider=provider,
        model=str(kwargs.get("model") or "unknown"),
        streamed=bool(kwargs.get("stream")),
        started_at=datetime.now(UTC),
        input_preview=preview,
        redaction_hits=hits,
        request_params=params,
        sdk_version=__version__,
    )


def _classify(exc: Exception) -> tuple[Status, str]:
    """Map an SDK exception onto our status vocabulary.

    Matching on class *name* rather than importing each provider's exception
    types keeps this file free of provider imports — the SDK must not require
    every vendor's package to be installed in order to instrument one of them.
    """
    name = type(exc).__name__
    if "RateLimit" in name:
        return Status.RATE_LIMITED, name
    if "Timeout" in name:
        return Status.TIMEOUT, name
    return Status.ERROR, name


def _finish(event: InferenceEvent, started: float) -> None:
    event.ended_at = datetime.now(UTC)
    event.latency_ms = int((time.perf_counter() - started) * 1000)


def _apply(event: InferenceEvent, extraction, output: str | None) -> None:
    if extraction is not None:
        event.response_model = extraction.response_model or event.response_model
        event.finish_reason = extraction.finish_reason or event.finish_reason
        if extraction.prompt_tokens is not None:
            event.prompt_tokens = extraction.prompt_tokens
        if extraction.completion_tokens is not None:
            event.completion_tokens = extraction.completion_tokens
        if extraction.total_tokens is not None:
            event.total_tokens = extraction.total_tokens
        if extraction.extra:
            event.request_params = {**event.request_params, **extraction.extra}

    if output:
        preview, hits = redact.preview(output)
        event.output_preview = preview
        event.redaction_hits = redact.merge_hits(event.redaction_hits, hits)

    if event.total_tokens is None and event.prompt_tokens and event.completion_tokens:
        event.total_tokens = event.prompt_tokens + event.completion_tokens


# --------------------------------------------------------------------------- #
# stream proxies
# --------------------------------------------------------------------------- #


class _AsyncStreamProxy:
    """Transparent wrapper around a provider's async stream.

    Delegates every attribute to the wrapped object so the application cannot
    tell the difference — `.response`, `.close()`, `async with`, all still work.

    Emission happens in `finally` because a stream has three exits: it finishes,
    the caller stops consuming it, or the task is cancelled mid-flight. A
    partial record must survive all three.
    """

    def __init__(self, stream, event: InferenceEvent, started: float) -> None:
        self._stream = stream
        self._event = event
        self._started = started
        self._chunks: list[str] = []
        self._last = None
        self._emitted = False

    def __getattr__(self, name):
        return getattr(self._stream, name)

    async def __aenter__(self):
        await self._stream.__aenter__()
        return self

    async def __aexit__(self, *exc_info):
        return await self._stream.__aexit__(*exc_info)

    def __aiter__(self):
        return self

    async def __anext__(self):
        try:
            chunk = await self._stream.__anext__()
        except StopAsyncIteration:
            self._emit(Status.SUCCESS)
            raise
        except BaseException as exc:  # includes CancelledError
            self._on_error(exc)
            raise

        extraction = extract_from_chunk(chunk)
        if extraction.text:
            if self._event.ttft_ms is None:
                # First token out. For a streamed call this is what a user
                # experiences as speed — total latency is not.
                self._event.ttft_ms = int((time.perf_counter() - self._started) * 1000)
            self._chunks.append(extraction.text)
        self._last = _merge(self._last, extraction)
        return chunk

    def _on_error(self, exc: BaseException) -> None:
        import asyncio

        if isinstance(exc, asyncio.CancelledError | GeneratorExit):
            self._emit(Status.CANCELLED)
        else:
            status, error_type = _classify(exc)  # type: ignore[arg-type]
            self._emit(status, error_type=error_type, error_message=str(exc)[:500])

    def _emit(self, status: Status, **error) -> None:
        if self._emitted:
            return
        self._emitted = True
        self._event.status = status
        self._event.error_type = error.get("error_type")
        self._event.error_message = error.get("error_message")
        _finish(self._event, self._started)
        _apply(self._event, self._last, "".join(self._chunks))
        emitter.emit(self._event)

    def __del__(self) -> None:
        # A stream abandoned without being exhausted still happened and still
        # cost tokens. Without this it would leave no trace at all.
        if not self._emitted:
            try:
                self._emit(Status.CANCELLED)
            except Exception:  # noqa: BLE001 — never raise from __del__
                pass


def _merge(previous, extraction):
    """Usage arrives on one chunk; text arrives on many. Keep both."""
    if previous is None:
        return extraction
    for name in (
        "prompt_tokens",
        "completion_tokens",
        "total_tokens",
        "finish_reason",
        "response_model",
    ):
        value = getattr(extraction, name, None)
        if value is not None:
            setattr(previous, name, value)
    if extraction.extra:
        previous.extra = {**previous.extra, **extraction.extra}
    return previous


# --------------------------------------------------------------------------- #
# patching
# --------------------------------------------------------------------------- #


def _resolve_openai_wire_provider(self) -> str:
    """Cerebras is reached through the `openai` package pointed at a different
    `base_url` — same classes, different vendor. Tag by base_url at call time
    rather than at patch time, since one patched class serves both.
    """
    base_url = str(getattr(self, "_client", self).base_url or "")
    if "cerebras" in base_url:
        return "cerebras"
    return "openai"


def _patch(cls, method_name: str, provider, config, is_async: bool) -> None:
    original = getattr(cls, method_name)
    _originals.append((cls, method_name, original))
    resolve = provider if callable(provider) else (lambda self: provider)

    if is_async:

        async def patched(self, *args, **kwargs):
            started = time.perf_counter()
            event = _new_event(config, resolve(self), kwargs)
            try:
                result = await original(self, *args, **kwargs)
            except Exception as exc:
                status, error_type = _classify(exc)
                event.status = status
                event.error_type = error_type
                event.error_message = str(exc)[:500]
                _finish(event, started)
                emitter.emit(event)
                raise  # the application sees the original exception

            if kwargs.get("stream"):
                return _AsyncStreamProxy(result, event, started)

            _finish(event, started)
            extraction = extract_from_response(result)
            _apply(event, extraction, extraction.text)
            emitter.emit(event)
            return result  # untouched

    else:

        def patched(self, *args, **kwargs):
            started = time.perf_counter()
            event = _new_event(config, resolve(self), kwargs)
            try:
                result = original(self, *args, **kwargs)
            except Exception as exc:
                status, error_type = _classify(exc)
                event.status = status
                event.error_type = error_type
                event.error_message = str(exc)[:500]
                _finish(event, started)
                emitter.emit(event)
                raise

            if kwargs.get("stream"):
                # Sync streaming is not proxied yet; the event is emitted at
                # call time without output or usage rather than not at all.
                _finish(event, started)
                emitter.emit(event)
                return result

            _finish(event, started)
            extraction = extract_from_response(result)
            _apply(event, extraction, extraction.text)
            emitter.emit(event)
            return result

    patched.__argus_patched__ = True  # type: ignore[attr-defined]
    setattr(cls, method_name, patched)


# (module path, sync class, async class, provider name or resolver)
# Cerebras is reached via the `openai` package pointed at a different base_url
# (open-weight models only — no closed OpenAI models in this stack), so the
# patched openai.* classes resolve their provider tag dynamically per call.
_TARGETS = [
    ("groq.resources.chat.completions", "Completions", "AsyncCompletions", "groq"),
    (
        "openai.resources.chat.completions",
        "Completions",
        "AsyncCompletions",
        _resolve_openai_wire_provider,
    ),
]


def install(config) -> list[str]:
    """Patch every provider SDK that is importable. Idempotent.

    A missing provider is a no-op, not an error — instrumenting Groq must not
    require OpenAI's package to be installed.
    """
    global _installed
    if _installed:
        return []

    patched: list[str] = []
    for module_path, sync_cls, async_cls, provider in _TARGETS:
        try:
            module = __import__(module_path, fromlist=[sync_cls, async_cls])
        except ImportError:
            continue

        for cls_name, is_async in ((sync_cls, False), (async_cls, True)):
            cls = getattr(module, cls_name, None)
            if cls is None:
                continue
            if getattr(getattr(cls, "create", None), "__argus_patched__", False):
                continue
            try:
                _patch(cls, "create", provider, config, is_async)
                label = provider if isinstance(provider, str) else "openai-wire"
                patched.append(f"{label}.{cls_name}.create")
            except Exception:  # noqa: BLE001
                log.debug("argus: could not patch %s.%s", module_path, cls_name, exc_info=True)

    _installed = True
    log.info("argus: instrumented %s", ", ".join(patched) or "nothing")
    return patched


def uninstall() -> None:
    """Restore the original methods. Exists so tests can run in isolation."""
    global _installed
    for cls, method_name, original in reversed(_originals):
        setattr(cls, method_name, original)
    _originals.clear()
    _installed = False

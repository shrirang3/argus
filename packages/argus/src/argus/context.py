"""Ambient call context.

Every log line needs a conversation_id, but the code that makes the LLM call is
often not ours — it is inside LangGraph, a library, a background job. Threading
an id through every signature would not reach those places and would poison the
API of the ones it could reach.

`contextvars` is the mechanism designed for exactly this: values scoped to the
current logical flow of execution, inherited across `await` boundaries and into
tasks spawned from the current one. Set it once per request; anything downstream
in that task can read it without being told.
"""

from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from uuid import UUID

_conversation_id: ContextVar[UUID | None] = ContextVar("argus_conversation_id", default=None)
_session_id: ContextVar[str | None] = ContextVar("argus_session_id", default=None)
_message_id: ContextVar[UUID | None] = ContextVar("argus_message_id", default=None)


@contextmanager
def conversation(conversation_id: UUID | str | None, *, message_id: UUID | str | None = None):
    """Tag every LLM call made inside this block.

    The token/reset dance matters: plain assignment would leak the value to
    whatever runs next on the same task. Resetting restores the previous value,
    so nesting works and concurrent requests never see each other's ids.
    """
    cid = UUID(str(conversation_id)) if conversation_id is not None else None
    mid = UUID(str(message_id)) if message_id is not None else None

    token = _conversation_id.set(cid)
    mtoken = _message_id.set(mid)
    try:
        yield
    finally:
        _conversation_id.reset(token)
        _message_id.reset(mtoken)


@contextmanager
def session(session_id: str | None):
    """Tag calls with a session id, for grouping across conversations."""
    token = _session_id.set(session_id)
    try:
        yield
    finally:
        _session_id.reset(token)


def current_conversation_id() -> UUID | None:
    return _conversation_id.get()


def current_message_id() -> UUID | None:
    return _message_id.get()


def current_session_id() -> str | None:
    return _session_id.get()

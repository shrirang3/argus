"""Conversation routes.

Replaces the in-memory stub with Postgres persistence. The response shapes are
unchanged, which is the point of having built the frontend against them first —
no JavaScript changed when the implementation swapped underneath.
"""

from __future__ import annotations

import asyncio
import json
import uuid
from collections.abc import AsyncIterator

import argus
import repo
from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse
from llm import ProviderError, Usage, stream_chat
from models import Conversation, Message
from pydantic import BaseModel, Field

from db import SessionDep, SessionLocal

router = APIRouter(prefix="/api")

# Conversations with a cancel requested from elsewhere — another tab, another
# device. Process-local, which is correct only while chat runs as one replica;
# scaling out moves this to a Redis key, and P8 is where that bites.
_CANCELLED: set[uuid.UUID] = set()


class NewMessage(BaseModel):
    content: str = Field(min_length=1, max_length=32_000)


def _conv_summary(conv: Conversation) -> dict:
    return {
        "id": str(conv.id),
        "title": conv.title,
        "created_at": conv.created_at.isoformat(),
        "message_count": conv.message_count,
    }


def _message_json(msg: Message) -> dict:
    return {
        "role": msg.role,
        "content": msg.content,
        "created_at": msg.created_at.isoformat(),
        "truncated": msg.truncated,
    }


def _sse(event: str, **payload) -> str:
    return f"event: {event}\ndata: {json.dumps(payload)}\n\n"


def _parse_id(conversation_id: str) -> uuid.UUID:
    try:
        return uuid.UUID(conversation_id)
    except ValueError:
        # A malformed id is a missing conversation as far as a client is
        # concerned; 422 would leak that ids are UUIDs and nothing more.
        raise HTTPException(status_code=404, detail="conversation not found") from None


@router.post("/conversations")
async def create_conversation(session: SessionDep) -> dict:
    conv = await repo.create_conversation(session)
    return _conv_summary(conv)


@router.get("/conversations")
async def list_conversations(session: SessionDep) -> list[dict]:
    return [_conv_summary(c) for c in await repo.list_conversations(session)]


@router.get("/conversations/{conversation_id}")
async def get_conversation(conversation_id: str, session: SessionDep) -> dict:
    cid = _parse_id(conversation_id)
    conv = await repo.get_conversation(session, cid)
    if conv is None:
        raise HTTPException(status_code=404, detail="conversation not found")

    messages = await repo.get_messages(session, cid)
    return _conv_summary(conv) | {"messages": [_message_json(m) for m in messages]}


@router.delete("/conversations/{conversation_id}", status_code=204)
async def delete_conversation(conversation_id: str, session: SessionDep) -> None:
    await repo.soft_delete_conversation(session, _parse_id(conversation_id))


@router.post("/conversations/{conversation_id}/cancel", status_code=202)
async def cancel_conversation(conversation_id: str) -> dict:
    """202, not 200 — this sets a flag the generator reads on its next token."""
    cid = _parse_id(conversation_id)
    _CANCELLED.add(cid)
    return {"cancelled": str(cid)}


@router.post("/conversations/{conversation_id}/messages")
async def send_message(
    conversation_id: str,
    body: NewMessage,
    session: SessionDep,
) -> StreamingResponse:
    cid = _parse_id(conversation_id)

    conv = await repo.get_conversation(session, cid)
    if conv is None:
        raise HTTPException(status_code=404, detail="conversation not found")

    text = body.content.strip()
    if not text:
        raise HTTPException(status_code=400, detail="empty message")

    # Persist the user turn and build the prompt before the response starts, so
    # a failure here is a clean 4xx/5xx rather than an error mid-stream.
    await repo.add_message(session, cid, "user", text)
    await repo.set_title_if_unset(session, cid, text)
    context = await repo.build_context(session, cid)

    _CANCELLED.discard(cid)

    return StreamingResponse(
        _generate(cid, context),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


async def _generate(cid: uuid.UUID, context: list[dict[str, str]]) -> AsyncIterator[str]:
    """Stream the reply and persist whatever was produced.

    This runs after the route handler has returned, so it opens its own session
    rather than borrowing the request-scoped one — that dependency's lifetime is
    tied to the handler, and reusing it here is a use-after-close waiting to
    happen.
    """
    acc: list[str] = []
    usage: Usage | None = None
    status = "success"

    # The only telemetry line the application writes. Every provider call made
    # inside this block is captured by the SDK and tagged with this id —
    # including calls made by code we do not own.
    with argus.conversation(cid):
        try:
            async for chunk in stream_chat(context):
                if isinstance(chunk, Usage):
                    usage = chunk
                    break
                if cid in _CANCELLED:
                    status = "cancelled"
                    break
                acc.append(chunk)
                yield _sse("token", text=chunk)

        except asyncio.CancelledError:
            # The client aborted its fetch. Record, then re-raise — swallowing
            # this tells the event loop the task refused to die.
            status = "cancelled"
            raise
        except ProviderError as exc:
            status = exc.kind
            yield _sse("error", message=str(exc))
        finally:
            _CANCELLED.discard(cid)
            # finally, not the success path: completion, server-side cancel and
            # client disconnect are three different exits and a partial reply
            # must survive all three.
            if acc:
                async with SessionLocal() as session:
                    await repo.add_message(
                        session,
                        cid,
                        "assistant",
                        "".join(acc),
                        truncated=status == "cancelled",
                        token_count=usage.completion_tokens if usage else None,
                    )

    yield _sse(
        "done",
        status=status,
        tokens=usage.completion_tokens if usage else len(acc),
        prompt_tokens=usage.prompt_tokens if usage else None,
        provider=usage.provider if usage else None,
        model=usage.model if usage else None,
    )

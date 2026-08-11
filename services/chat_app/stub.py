"""TEMPORARY in-memory backend, so the UI can be built and verified before P1.

Everything here is replaced in P1 by real Postgres persistence and a real Groq
call. Nothing in this module should outlive that phase — the routes and their
response shapes are the contract the frontend is written against, so swapping
the implementation underneath should require no JavaScript changes.
"""

from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime
from uuid import uuid4

from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

router = APIRouter(prefix="/api")

# conversation_id -> {"id", "title", "created_at", "messages": [...]}
_CONVERSATIONS: dict[str, dict] = {}

# conversation_ids with a cancel requested from another client
_CANCELLED: set[str] = set()

_CANNED = (
    "Streaming from the stub backend. In P1 this is a real Groq call, and every "
    "token you see arrives over the same fetch + ReadableStream path. The point of "
    "wiring it now is that cancel, resume and the sidebar are all exercised against "
    "the response shapes the real routes will return, so none of this JavaScript "
    "changes when the implementation swaps underneath it."
)


def _now() -> str:
    return datetime.now(UTC).isoformat()


def _title_from(text: str) -> str:
    text = " ".join(text.split())
    return text[:48] + ("…" if len(text) > 48 else "")


class NewMessage(BaseModel):
    content: str


@router.post("/conversations")
async def create_conversation() -> dict:
    conv = {"id": str(uuid4()), "title": "New conversation", "created_at": _now(), "messages": []}
    _CONVERSATIONS[conv["id"]] = conv
    return {k: v for k, v in conv.items() if k != "messages"} | {"message_count": 0}


@router.get("/conversations")
async def list_conversations() -> list[dict]:
    return [
        {
            "id": c["id"],
            "title": c["title"],
            "created_at": c["created_at"],
            "message_count": len(c["messages"]),
        }
        for c in sorted(_CONVERSATIONS.values(), key=lambda c: c["created_at"], reverse=True)
    ]


@router.get("/conversations/{conversation_id}")
async def get_conversation(conversation_id: str) -> dict:
    conv = _CONVERSATIONS.get(conversation_id)
    if conv is None:
        raise HTTPException(status_code=404, detail="conversation not found")
    return conv


@router.delete("/conversations/{conversation_id}", status_code=204)
async def delete_conversation(conversation_id: str) -> None:
    _CONVERSATIONS.pop(conversation_id, None)


@router.post("/conversations/{conversation_id}/cancel", status_code=202)
async def cancel_conversation(conversation_id: str) -> dict:
    """Server-side cancel, for stopping a stream from a different tab or device.

    The common path is the browser aborting its own fetch, which the generator
    below sees as a CancelledError.
    """
    _CANCELLED.add(conversation_id)
    return {"cancelled": conversation_id}


def _sse(event: str, **payload) -> str:
    return f"event: {event}\ndata: {json.dumps(payload)}\n\n"


@router.post("/conversations/{conversation_id}/messages")
async def send_message(conversation_id: str, body: NewMessage) -> StreamingResponse:
    conv = _CONVERSATIONS.get(conversation_id)
    if conv is None:
        raise HTTPException(status_code=404, detail="conversation not found")

    text = body.content.strip()
    if not text:
        raise HTTPException(status_code=400, detail="empty message")

    conv["messages"].append({"role": "user", "content": text, "created_at": _now()})
    if conv["title"] == "New conversation":
        conv["title"] = _title_from(text)

    _CANCELLED.discard(conversation_id)

    async def generate():
        acc: list[str] = []
        status = "success"
        try:
            for token in _CANNED.split(" "):
                if conversation_id in _CANCELLED:
                    status = "cancelled"
                    break
                acc.append(token)
                yield _sse("token", text=token + " ")
                await asyncio.sleep(0.04)
        except asyncio.CancelledError:
            # Client aborted the fetch. In P2 this is where the SDK emits a row
            # with status="cancelled" and whatever tokens were produced.
            status = "cancelled"
            raise
        finally:
            _CANCELLED.discard(conversation_id)
            if acc:
                conv["messages"].append(
                    {"role": "assistant", "content": " ".join(acc), "created_at": _now()}
                )

        yield _sse("done", status=status, tokens=len(acc))

    return StreamingResponse(
        generate(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )

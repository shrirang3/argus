"""Data access for conversations and messages.

Route handlers never write SQL. Everything the chat app needs from Postgres
goes through this module, so the query shapes stay in one place — which is also
where the P5 agent tools will look for prior art.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime

from models import Conversation, Message
from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

CONTEXT_WINDOW_TURNS = 10


async def create_conversation(session: AsyncSession) -> Conversation:
    conv = Conversation(id=uuid.uuid4())
    session.add(conv)
    await session.commit()
    return conv


async def get_conversation(
    session: AsyncSession, conversation_id: uuid.UUID
) -> Conversation | None:
    stmt = select(Conversation).where(
        Conversation.id == conversation_id, Conversation.status != "deleted"
    )
    return await session.scalar(stmt)


async def list_conversations(session: AsyncSession, limit: int = 100) -> list[Conversation]:
    """Sidebar query — hits ix_conversations_updated, the partial index."""
    stmt = (
        select(Conversation)
        .where(Conversation.status != "deleted")
        .order_by(Conversation.updated_at.desc())
        .limit(limit)
    )
    return list(await session.scalars(stmt))


async def get_messages(
    session: AsyncSession, conversation_id: uuid.UUID, limit: int | None = None
) -> list[Message]:
    """Messages in order. `limit` returns the most recent N, still oldest-first.

    The context builder wants the tail of the conversation; the UI wants all of
    it. Both are the same index scan, so one function serves both.
    """
    stmt = select(Message).where(Message.conversation_id == conversation_id)
    if limit is None:
        stmt = stmt.order_by(Message.seq)
        return list(await session.scalars(stmt))

    stmt = stmt.order_by(Message.seq.desc()).limit(limit)
    rows = list(await session.scalars(stmt))
    return list(reversed(rows))


async def soft_delete_conversation(session: AsyncSession, conversation_id: uuid.UUID) -> None:
    """Mark deleted rather than DELETE.

    Inference logs reference this conversation and must outlive it — analytics
    should not disappear because a user cleared their history.
    """
    await session.execute(
        update(Conversation)
        .where(Conversation.id == conversation_id)
        .values(status="deleted", updated_at=datetime.now(UTC))
    )
    await session.commit()


async def add_message(
    session: AsyncSession,
    conversation_id: uuid.UUID,
    role: str,
    content: str,
    *,
    truncated: bool = False,
    token_count: int | None = None,
    commit: bool = True,
) -> Message:
    """Append a message and keep the conversation's denormalised counters honest.

    `seq` is derived from the current MAX inside the same transaction. Two
    concurrent writers to one conversation could compute the same value; the
    unique constraint on (conversation_id, seq) turns that into a loud error
    rather than silent reordering. Acceptable because a single conversation is
    inherently serial — a user cannot be mid-turn twice at once.
    """
    next_seq = await session.scalar(
        select(func.coalesce(func.max(Message.seq), -1) + 1).where(
            Message.conversation_id == conversation_id
        )
    )

    msg = Message(
        id=uuid.uuid4(),
        conversation_id=conversation_id,
        seq=next_seq,
        role=role,
        content=content,
        truncated=truncated,
        token_count=token_count,
    )
    session.add(msg)

    await session.execute(
        update(Conversation)
        .where(Conversation.id == conversation_id)
        .values(
            message_count=Conversation.message_count + 1,
            updated_at=datetime.now(UTC),
        )
    )

    if commit:
        await session.commit()
    return msg


async def set_title_if_unset(session: AsyncSession, conversation_id: uuid.UUID, text: str) -> None:
    """Derive a title from the first user message, once."""
    title = " ".join(text.split())[:48]
    if len(" ".join(text.split())) > 48:
        title += "…"

    await session.execute(
        update(Conversation)
        .where(Conversation.id == conversation_id, Conversation.title == "New conversation")
        .values(title=title)
    )
    await session.commit()


async def build_context(
    session: AsyncSession, conversation_id: uuid.UUID, turns: int = CONTEXT_WINDOW_TURNS
) -> list[dict[str, str]]:
    """The prompt sent to the model: the last N turns, oldest first.

    Trimming rather than summarising is deliberate. A rolling summary would
    preserve long-range facts but costs an extra inference every N turns and
    adds a failure mode; the window is predictable and free. The system prompt
    is prepended by the caller and is never subject to trimming.
    """
    messages = await get_messages(session, conversation_id, limit=turns * 2)
    return [{"role": m.role, "content": m.content} for m in messages]

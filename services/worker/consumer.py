"""Redis Streams consumer group.

A consumer group lets N workers share one stream: each message is delivered to
exactly one member, and a message stays *pending* until it is acknowledged. That
pending list is the recovery mechanism — if a worker dies mid-batch its messages
are still there, and another worker can claim them once they have been idle long
enough.

The ordering that makes this safe: **acknowledge only after the database
commit.** Acknowledging first would mean a crash between the ack and the write
loses data permanently and invisibly.
"""

from __future__ import annotations

import logging

import redis.asyncio as redis

from .config import (
    CLAIM_IDLE_MS,
    CONSUMER_GROUP,
    CONSUMER_NAME,
    REDIS_URL,
    STREAM_NAME,
)

log = logging.getLogger("argus.worker.consumer")

_redis: redis.Redis | None = None


def get_redis() -> redis.Redis:
    global _redis
    if _redis is None:
        _redis = redis.from_url(REDIS_URL, decode_responses=True)
    return _redis


async def ensure_group() -> None:
    """Create the consumer group, tolerating the case where it already exists.

    MKSTREAM creates the stream too, so a worker started before any producer
    does not crash on a missing key. Starting at id "0" rather than "$" means a
    fresh group reads the backlog instead of only new arrivals — on a first
    deploy, "$" would silently skip everything already queued.
    """
    client = get_redis()
    try:
        await client.xgroup_create(STREAM_NAME, CONSUMER_GROUP, id="0", mkstream=True)
        log.info("created consumer group %s on %s", CONSUMER_GROUP, STREAM_NAME)
    except redis.ResponseError as exc:
        if "BUSYGROUP" not in str(exc):
            raise


async def read_new(count: int, block_ms: int) -> list[tuple[str, dict]]:
    """Block for new messages addressed to this consumer.

    ">" means "messages never delivered to anyone", as opposed to this
    consumer's own pending backlog, which is handled by claim_stale().
    """
    client = get_redis()
    response = await client.xreadgroup(
        groupname=CONSUMER_GROUP,
        consumername=CONSUMER_NAME,
        streams={STREAM_NAME: ">"},
        count=count,
        block=block_ms,
    )
    if not response:
        return []
    return [(entry_id, fields) for _stream, entries in response for entry_id, fields in entries]


async def read_own_pending(count: int) -> list[tuple[str, dict]]:
    """This consumer's own unacknowledged messages, redelivered immediately.

    Reading id "0" instead of ">" returns entries already delivered to *this*
    consumer and not yet acknowledged — the batch that was in flight when the
    database went away, or when the process was killed.

    Without this, those messages would sit until the XAUTOCLAIM idle timeout
    expired, because ">" only ever returns messages nobody has seen. That
    timeout is deliberately long, since it exists to detect a *dead* consumer;
    applying it to a worker's own backlog would turn a two-second database blip
    into a minute of delay for no reason.
    """
    client = get_redis()
    response = await client.xreadgroup(
        groupname=CONSUMER_GROUP,
        consumername=CONSUMER_NAME,
        streams={STREAM_NAME: "0"},
        count=count,
    )
    if not response:
        return []
    return [
        (entry_id, fields)
        for _stream, entries in response
        for entry_id, fields in entries
        if fields
    ]


async def claim_stale(count: int) -> list[tuple[str, dict]]:
    """Take over messages a dead consumer left pending.

    XAUTOCLAIM scans the group's pending list for entries idle longer than the
    threshold and reassigns them. This is what makes a worker crash a delay
    rather than a data loss — combined with the idempotent insert, the reclaimed
    message is simply processed again.
    """
    client = get_redis()
    try:
        _cursor, entries, _deleted = await client.xautoclaim(
            name=STREAM_NAME,
            groupname=CONSUMER_GROUP,
            consumername=CONSUMER_NAME,
            min_idle_time=CLAIM_IDLE_MS,
            count=count,
        )
    except redis.ResponseError:
        return []
    return [(entry_id, fields) for entry_id, fields in entries if fields]


async def delivery_counts(entry_ids: list[str]) -> dict[str, int]:
    """How many times each pending message has been delivered.

    Used to detect poison: a message that keeps failing would otherwise be
    reclaimed forever and block the stream behind it.
    """
    if not entry_ids:
        return {}
    client = get_redis()
    try:
        pending = await client.xpending_range(
            name=STREAM_NAME,
            groupname=CONSUMER_GROUP,
            min=min(entry_ids),
            max=max(entry_ids),
            count=len(entry_ids) * 2,
        )
    except redis.ResponseError:
        return {}
    return {p["message_id"]: p["times_delivered"] for p in pending}


async def ack(entry_ids: list[str]) -> int:
    """Acknowledge. Called only after the database transaction has committed."""
    if not entry_ids:
        return 0
    return await get_redis().xack(STREAM_NAME, CONSUMER_GROUP, *entry_ids)


async def redis_ok() -> bool:
    try:
        return bool(await get_redis().ping())
    except Exception:  # noqa: BLE001
        return False


async def aclose() -> None:
    if _redis is not None:
        await _redis.aclose()

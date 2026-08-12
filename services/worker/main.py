"""Stream consumer.

Drains `llm.inference.v1` into Postgres. One loop, in a deliberate order:

    1. re-read this consumer's own unacknowledged backlog
    2. reclaim messages a *dead* consumer abandoned
    3. read new messages
    4. write the batch and its rollups in one transaction
    5. only then acknowledge

Backlog before new work, so a restarted worker finishes what it started rather
than adding to the pile. Steps 1 and 2 are separate because they answer
different questions: step 1 is "what was I doing when I stopped", which needs no
delay, while step 2 is "has someone else died", which needs an idle timeout long
enough not to steal work from a healthy peer.

Acknowledgement comes last so a crash means redelivery, never loss — which is
safe because the insert deduplicates.

The worker has no HTTP surface. Liveness is a heartbeat file, which is the same
exec-probe shape Kubernetes uses in P8.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import signal
from pathlib import Path

from . import consumer, writer
from .config import BATCH_SIZE, BLOCK_MS, HEARTBEAT_PATH, LOG_LEVEL, MAX_DELIVERIES

logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s %(levelname)s %(name)s %(message)s")
log = logging.getLogger("argus.worker")

HEARTBEAT = Path(HEARTBEAT_PATH)
_stopping = asyncio.Event()

STATS = {"read": 0, "inserted": 0, "duplicates": 0, "poisoned": 0, "malformed": 0}


def _decode(entry_id: str, fields: dict) -> tuple[dict | None, str | None]:
    """Pull the event out of a stream entry.

    A message that cannot be decoded is not retryable — redelivering it produces
    the identical failure — so it goes straight to the dead-letter table.
    """
    raw = fields.get("data")
    if raw is None:
        return None, "entry has no 'data' field"
    try:
        event = json.loads(raw)
    except json.JSONDecodeError as exc:
        return None, f"invalid json: {exc}"
    if not isinstance(event, dict) or "event_id" not in event:
        return None, "not an inference event"
    return event, None


async def _process(entries: list[tuple[str, dict]], *, reclaimed: bool) -> None:
    if not entries:
        return

    STATS["read"] += len(entries)

    events: list[dict] = []
    ids: list[str] = []
    quarantine: list[tuple[dict, str, str]] = []
    ack_only: list[str] = []

    # Poison detection applies to reclaimed messages only: a message being
    # reclaimed means a previous attempt did not finish, and one that keeps
    # coming back would otherwise block the stream behind it forever.
    counts = await consumer.delivery_counts([i for i, _ in entries]) if reclaimed else {}

    for entry_id, fields in entries:
        if counts.get(entry_id, 0) > MAX_DELIVERIES:
            quarantine.append(
                (
                    {"entry_id": entry_id, "fields": fields},
                    f"exceeded {MAX_DELIVERIES} delivery attempts",
                    "worker-poison",
                )
            )
            ack_only.append(entry_id)
            STATS["poisoned"] += 1
            continue

        event, error = _decode(entry_id, fields)
        if error is not None:
            quarantine.append(({"entry_id": entry_id, "fields": fields}, error, "worker-decode"))
            ack_only.append(entry_id)
            STATS["malformed"] += 1
            continue

        events.append(event)
        ids.append(entry_id)

    if quarantine:
        await writer.dead_letter(quarantine)

    if events:
        inserted, duplicates = await writer.write_batch(events)
        STATS["inserted"] += inserted
        STATS["duplicates"] += duplicates
        if duplicates:
            log.info("absorbed %d duplicate event(s)", duplicates)

    # Ack after the commit, never before.
    await consumer.ack(ids + ack_only)


async def _run() -> None:
    await consumer.ensure_group()
    log.info("worker up — draining %s", ", ".join(f"{k}={v}" for k, v in STATS.items()))

    while not _stopping.is_set():
        HEARTBEAT.touch()

        try:
            # 1. Our own in-flight backlog, redelivered immediately — the batch
            #    that was open when the database went away or the process died.
            own = await consumer.read_own_pending(BATCH_SIZE)
            await _process(own, reclaimed=True)

            # 2. Another consumer's abandoned messages, once they are stale
            #    enough that it is safe to assume that consumer is gone.
            reclaimed = await consumer.claim_stale(BATCH_SIZE)
            await _process(reclaimed, reclaimed=True)

            # 3. Only then, new work.
            entries = await consumer.read_new(BATCH_SIZE, BLOCK_MS)
            await _process(entries, reclaimed=False)

        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001
            # Do not acknowledge, do not exit. Whatever was in flight stays
            # pending and will be reclaimed; a crash loop would only make an
            # outage worse.
            log.warning("batch failed, leaving messages pending", exc_info=True)
            await asyncio.sleep(2)


def _install_signal_handlers(loop: asyncio.AbstractEventLoop) -> None:
    """Finish the current batch on SIGTERM rather than dying mid-transaction."""
    for sig in (signal.SIGTERM, signal.SIGINT):
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(sig, _stopping.set)


async def main() -> None:
    _install_signal_handlers(asyncio.get_running_loop())
    try:
        await _run()
    finally:
        log.info("shutting down — %s", ", ".join(f"{k}={v}" for k, v in STATS.items()))
        await consumer.aclose()
        await writer.aclose()


if __name__ == "__main__":
    asyncio.run(main())

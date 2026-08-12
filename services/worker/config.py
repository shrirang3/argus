"""Worker configuration."""

from __future__ import annotations

import os
import socket

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql+asyncpg://argus:argus@localhost:5433/argus")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6380/0")

STREAM_NAME = os.getenv("STREAM_NAME", "llm.inference.v1")
CONSUMER_GROUP = os.getenv("CONSUMER_GROUP", "ingest-workers")

# Identity must be unique per replica, or two workers share a name and Redis
# treats their pending entries as one consumer's — so a live worker's messages
# could be claimed out from under it.
CONSUMER_NAME = os.getenv("CONSUMER_NAME") or f"{socket.gethostname()}-{os.getpid()}"

BATCH_SIZE = int(os.getenv("WORKER_BATCH_SIZE", "100"))
BLOCK_MS = int(os.getenv("WORKER_BLOCK_MS", "2000"))

# How long a message may sit unacknowledged before another worker may claim it.
# Must comfortably exceed the time a healthy batch takes, or workers will steal
# in-flight work from each other.
CLAIM_IDLE_MS = int(os.getenv("WORKER_CLAIM_IDLE_MS", "60000"))

# After this many deliveries a message is treated as poison: it is moved to the
# dead-letter table and acknowledged, so one bad row cannot block the stream
# forever.
MAX_DELIVERIES = int(os.getenv("WORKER_MAX_DELIVERIES", "5"))

HEARTBEAT_PATH = os.getenv("WORKER_HEARTBEAT", "/tmp/argus-worker.alive")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

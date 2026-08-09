"""Stream consumer entrypoint.

The Redis consumer group, idempotent writes and rollups land in P4. For now
the loop only maintains a heartbeat file, which is what the container (and
later the Kubernetes liveness probe) checks — the worker has no HTTP surface.
"""

import asyncio
import logging
import os
from pathlib import Path

HEARTBEAT = Path(os.getenv("WORKER_HEARTBEAT", "/tmp/argus-worker.alive"))
INTERVAL_S = 10

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
log = logging.getLogger("argus-worker")


async def main() -> None:
    log.info("worker up — consumer group logic lands in P4")
    while True:
        HEARTBEAT.touch()
        await asyncio.sleep(INTERVAL_S)


if __name__ == "__main__":
    asyncio.run(main())

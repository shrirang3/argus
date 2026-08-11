"""Database engine and session lifecycle.

One engine per process, one session per request. The session is handed to route
handlers by FastAPI's dependency system so that transaction boundaries line up
with request boundaries rather than being scattered through the code.
"""

from __future__ import annotations

import os
from collections.abc import AsyncGenerator
from typing import Annotated

from fastapi import Depends
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql+asyncpg://argus:argus@localhost:5433/argus")

# pool_pre_ping issues a cheap SELECT 1 before handing out a pooled connection.
# Without it, a connection killed server-side (restart, idle timeout, failover)
# surfaces as a confusing error on the next real query instead of being replaced.
engine = create_async_engine(
    DATABASE_URL,
    pool_size=5,
    max_overflow=10,
    pool_pre_ping=True,
    echo=False,
)

# expire_on_commit=False: after commit, attribute access on an ORM object would
# otherwise trigger a lazy refresh — which in async code raises rather than
# silently issuing IO. We read objects after committing, so this must be off.
SessionLocal = async_sessionmaker(engine, expire_on_commit=False, autoflush=False)


async def get_session() -> AsyncGenerator[AsyncSession]:
    """FastAPI dependency: one session per request, always closed."""
    async with SessionLocal() as session:
        yield session


# Annotated alias rather than `= Depends(...)` in each signature: the default-arg
# form is a mutable-default smell (ruff B008) and repeats the wiring at every
# call site. This declares the dependency once and reads as a type.
SessionDep = Annotated[AsyncSession, Depends(get_session)]

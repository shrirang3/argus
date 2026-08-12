"""Chat app entrypoint.

Routes are backed by Postgres. The provider behind them is selected by
DEFAULT_PROVIDER and defaults to a mock that needs no API key.

Note what instrumenting this app costs: one `argus.init()` call below. No route,
no adapter and no provider call site knows the SDK exists.
"""

from contextlib import asynccontextmanager

import argus
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from .routes import router as api_router

# Must run before any provider client is constructed — patching the class after
# a client exists still works (lookup goes through the class), but doing it at
# import time keeps the ordering obvious.
argus.init(service="chat-app")


@asynccontextmanager
async def lifespan(app: FastAPI):
    yield
    # Drain whatever is still buffered rather than losing the last few seconds
    # of telemetry on every deploy.
    await argus.shutdown()


app = FastAPI(title="argus-chat", version="0.1.0", lifespan=lifespan)
app.include_router(api_router)
app.mount("/static", StaticFiles(directory="chat_app/static"), name="static")

templates = Jinja2Templates(directory="chat_app/templates")


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok", "service": "chat-app"}


@app.get("/sdk-stats")
async def sdk_stats() -> dict:
    """Emitter counters, so buffered/dropped/spilled events are observable.

    The dashboard reads this in P6 — an observability tool that cannot report
    its own data loss is asking to be trusted on faith.
    """
    return argus.stats()


@app.get("/", response_class=HTMLResponse)
async def index(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(request, "index.html")

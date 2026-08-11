"""Chat app entrypoint.

Routes are backed by Postgres. The provider behind them is selected by
DEFAULT_PROVIDER and defaults to a mock that needs no API key.
"""

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from routes import router as api_router

app = FastAPI(title="argus-chat", version="0.1.0")
app.include_router(api_router)
app.mount("/static", StaticFiles(directory="static"), name="static")

templates = Jinja2Templates(directory="templates")


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok", "service": "chat-app"}


@app.get("/", response_class=HTMLResponse)
async def index(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(request, "index.html")

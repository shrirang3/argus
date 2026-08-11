"""Chat app entrypoint.

Routes currently come from the temporary in-memory stub; P1 swaps that module
for real Postgres persistence and a real Groq call without touching the UI.
"""

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from stub import router as api_router

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

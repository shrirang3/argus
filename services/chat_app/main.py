"""Chat app entrypoint.

Conversation routes, SSE streaming and the LangGraph agent land in P1/P5.
For now this exists so the container has something to boot and answer with.
"""

from fastapi import FastAPI

app = FastAPI(title="argus-chat", version="0.1.0")


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok", "service": "chat-app"}

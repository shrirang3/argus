"""Dashboard entrypoint.

Metrics queries and charts land in P6.
"""

from fastapi import FastAPI

app = FastAPI(title="argus-dash", version="0.1.0")


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok", "service": "dashboard"}

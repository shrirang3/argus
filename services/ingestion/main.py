"""Ingestion service entrypoint.

Validation, PII redaction and the Redis publish path land in P3.
"""

from fastapi import FastAPI

app = FastAPI(title="argus-ingest", version="0.1.0")


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok", "service": "ingestion"}

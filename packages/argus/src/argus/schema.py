"""The wire contract between the SDK and the ingestion service.

Field names are native snake_case because they map 1:1 onto typed Postgres
columns, which is what keeps the dashboard queryable. `to_otel_attributes()`
renders the same event under OpenTelemetry GenAI semantic conventions, which is
what makes "any OTel-emitting stack can feed this pipeline" a fact rather than a
claim — the OTLP receiver that consumes it lands in P3.
"""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from uuid import UUID, uuid4

from pydantic import BaseModel, Field


class Status(StrEnum):
    """Outcome vocabulary. Shared by the SDK, ingestion and the schema.

    Rate limiting and timeouts are separated from generic errors on purpose:
    they call for different responses (back off / retry-unsafe) and deserve
    their own series on the dashboard.
    """

    SUCCESS = "success"
    ERROR = "error"
    CANCELLED = "cancelled"
    TIMEOUT = "timeout"
    RATE_LIMITED = "rate_limited"


class InferenceEvent(BaseModel):
    """One LLM call. The unit of everything downstream."""

    # Generated client-side at call time. This is the idempotency key that makes
    # at-least-once delivery safe to dedupe at write.
    event_id: UUID = Field(default_factory=uuid4)

    conversation_id: UUID | None = None
    message_id: UUID | None = None
    session_id: str | None = None
    service: str

    provider: str
    model: str
    response_model: str | None = None
    operation: str = "chat"

    status: Status = Status.SUCCESS
    error_type: str | None = None
    error_message: str | None = None
    finish_reason: str | None = None

    started_at: datetime
    ended_at: datetime | None = None
    latency_ms: int | None = None
    # Time to first token. For a streamed call this is what a user perceives as
    # speed; total latency is not.
    ttft_ms: int | None = None
    streamed: bool = False

    prompt_tokens: int | None = None
    completion_tokens: int | None = None
    total_tokens: int | None = None

    # Previews only, already redacted. Full text lives in `messages`; bounding
    # this column is what keeps the log table from becoming the biggest thing in
    # the database.
    input_preview: str | None = None
    output_preview: str | None = None
    redaction_hits: dict[str, int] = Field(default_factory=dict)

    # Provider params and provider-specific extras (Groq's queue_time, etc).
    request_params: dict = Field(default_factory=dict)

    sdk_version: str | None = None

    def to_otel_attributes(self) -> dict:
        """Render as OpenTelemetry GenAI semantic-convention attributes.

        Only the fields with a standardised name are translated; the rest keep
        an `argus.` prefix, which is what the spec says to do with attributes
        that have no convention yet.
        """
        attrs: dict = {
            "gen_ai.system": self.provider,
            "gen_ai.operation.name": self.operation,
            "gen_ai.request.model": self.model,
            "service.name": self.service,
            "argus.event_id": str(self.event_id),
            "argus.status": str(self.status),
            "argus.streamed": self.streamed,
        }
        optional = {
            "gen_ai.response.model": self.response_model,
            "gen_ai.response.finish_reasons": self.finish_reason,
            "gen_ai.usage.input_tokens": self.prompt_tokens,
            "gen_ai.usage.output_tokens": self.completion_tokens,
            "gen_ai.conversation.id": str(self.conversation_id) if self.conversation_id else None,
            "error.type": self.error_type,
            "argus.latency_ms": self.latency_ms,
            "argus.ttft_ms": self.ttft_ms,
        }
        attrs.update({k: v for k, v in optional.items() if v is not None})
        return attrs


class EventBatch(BaseModel):
    """What the SDK POSTs to /v1/events."""

    events: list[InferenceEvent]


class IngestResult(BaseModel):
    accepted: int
    rejected: int = 0

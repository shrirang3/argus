"""OTLP/HTTP JSON → InferenceEvent.

Why this endpoint exists: it is what turns "any OTel-emitting stack can feed
this pipeline" from a claim into a fact. Pipecat, LangChain, LlamaIndex and
OpenTelemetry's own auto-instrumentation all emit GenAI spans; anything that
does can point at `/v1/traces` and appear on the same dashboard as our own SDK,
with no new code on either side.

Only the subset of OTLP that carries GenAI semantics is handled. A span without
`gen_ai.*` attributes is not an inference and is skipped rather than guessed at.
"""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import UUID, uuid4

from argus.schema import InferenceEvent, Status


def _attrs(raw: list[dict]) -> dict:
    """Flatten OTLP's AnyValue wrapper into plain Python.

    OTLP encodes every attribute as {"key": k, "value": {"<type>Value": v}},
    which is verbose but unambiguous — and needs unwrapping before it is usable.
    """
    out: dict = {}
    for item in raw or []:
        key = item.get("key")
        value = item.get("value") or {}
        for kind in ("stringValue", "boolValue", "arrayValue"):
            if kind in value:
                out[key] = value[kind]
                break
        else:
            if "intValue" in value:
                out[key] = int(value["intValue"])
            elif "doubleValue" in value:
                out[key] = float(value["doubleValue"])
    return out


def _ns_to_dt(nanos) -> datetime | None:
    if not nanos:
        return None
    return datetime.fromtimestamp(int(nanos) / 1_000_000_000, tz=UTC)


def _as_uuid(value) -> UUID | None:
    try:
        return UUID(str(value))
    except (ValueError, TypeError):
        return None


def _status(span: dict, attrs: dict) -> Status:
    if attrs.get("argus.status") in set(Status):
        return Status(attrs["argus.status"])
    # OTLP status codes: 0 unset, 1 ok, 2 error.
    return Status.ERROR if (span.get("status") or {}).get("code") == 2 else Status.SUCCESS


def parse(payload: dict, default_service: str = "otlp") -> tuple[list[InferenceEvent], list[dict]]:
    """Return (events, skipped_spans).

    Skipped spans are reported rather than silently dropped, so a sender whose
    attributes do not match the convention gets a signal instead of silence.
    """
    events: list[InferenceEvent] = []
    skipped: list[dict] = []

    for resource_span in payload.get("resourceSpans", []) or []:
        resource_attrs = _attrs((resource_span.get("resource") or {}).get("attributes", []))
        service = resource_attrs.get("service.name", default_service)

        for scope_span in resource_span.get("scopeSpans", []) or []:
            for span in scope_span.get("spans", []) or []:
                attrs = _attrs(span.get("attributes", []))

                if "gen_ai.system" not in attrs:
                    skipped.append({"name": span.get("name"), "reason": "no gen_ai.system"})
                    continue

                started = _ns_to_dt(span.get("startTimeUnixNano")) or datetime.now(UTC)
                ended = _ns_to_dt(span.get("endTimeUnixNano"))
                latency = int((ended - started).total_seconds() * 1000) if ended else None

                finish = attrs.get("gen_ai.response.finish_reasons")
                if isinstance(finish, list):
                    finish = finish[0] if finish else None

                events.append(
                    InferenceEvent(
                        event_id=_as_uuid(attrs.get("argus.event_id")) or uuid4(),
                        conversation_id=_as_uuid(attrs.get("gen_ai.conversation.id")),
                        service=str(attrs.get("service.name") or service),
                        provider=str(attrs["gen_ai.system"]),
                        model=str(attrs.get("gen_ai.request.model") or "unknown"),
                        response_model=attrs.get("gen_ai.response.model"),
                        operation=str(attrs.get("gen_ai.operation.name") or "chat"),
                        status=_status(span, attrs),
                        error_type=attrs.get("error.type"),
                        finish_reason=finish,
                        started_at=started,
                        ended_at=ended,
                        latency_ms=attrs.get("argus.latency_ms") or latency,
                        ttft_ms=attrs.get("argus.ttft_ms"),
                        streamed=bool(attrs.get("argus.streamed", False)),
                        prompt_tokens=attrs.get("gen_ai.usage.input_tokens"),
                        completion_tokens=attrs.get("gen_ai.usage.output_tokens"),
                    )
                )

    return events, skipped

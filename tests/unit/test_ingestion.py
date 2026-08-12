"""Ingestion logic: cost enrichment and OTLP parsing.

No Redis or Postgres here — these are the pure functions. The wired path is
verified against the running stack.
"""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "services" / "ingestion"))

import otlp  # noqa: E402
from config import cost_usd  # noqa: E402

# ------------------------------------------------------------------ pricing


def test_priced_model():
    # 1000 * 0.05 + 500 * 0.08, per million
    assert cost_usd("groq", "llama-3.1-8b-instant", 1000, 500) == pytest.approx(0.00009)


def test_unknown_model_is_none_not_zero():
    """Zero would be indistinguishable from a free call and understate spend."""
    assert cost_usd("groq", "no-such-model", 1000, 500) is None


def test_unknown_provider_is_none():
    assert cost_usd("nobody", "llama-3.1-8b-instant", 10, 10) is None


def test_missing_token_counts_is_none():
    """A cancelled call has no usage; guessing its cost would be fabrication."""
    assert cost_usd("groq", "llama-3.1-8b-instant", None, None) is None
    assert cost_usd("groq", "llama-3.1-8b-instant", 100, None) is None


def test_free_model_is_zero_not_none():
    """A known-free model is genuinely 0.0, which is different from unknown."""
    assert cost_usd("mock", "mock-1", 100, 100) == 0.0


# --------------------------------------------------------------------- OTLP


def span(attributes, start="1786550000000000000", end="1786550000900000000", status_code=1):
    return {
        "resourceSpans": [
            {
                "resource": {
                    "attributes": [{"key": "service.name", "value": {"stringValue": "foreign-app"}}]
                },
                "scopeSpans": [
                    {
                        "spans": [
                            {
                                "name": "chat",
                                "startTimeUnixNano": start,
                                "endTimeUnixNano": end,
                                "status": {"code": status_code},
                                "attributes": attributes,
                            }
                        ]
                    }
                ],
            }
        ]
    }


GENAI = [
    {"key": "gen_ai.system", "value": {"stringValue": "groq"}},
    {"key": "gen_ai.request.model", "value": {"stringValue": "llama-3.3-70b-versatile"}},
    {"key": "gen_ai.usage.input_tokens", "value": {"intValue": "120"}},
    {"key": "gen_ai.usage.output_tokens", "value": {"intValue": "60"}},
    {"key": "gen_ai.response.finish_reasons", "value": {"stringValue": "stop"}},
]


def test_genai_span_becomes_an_event():
    events, skipped = otlp.parse(span(GENAI))
    assert len(events) == 1
    assert not skipped

    event = events[0]
    assert event.provider == "groq"
    assert event.model == "llama-3.3-70b-versatile"
    assert event.prompt_tokens == 120
    assert event.completion_tokens == 60
    assert event.finish_reason == "stop"
    assert event.service == "foreign-app"


def test_latency_is_derived_from_span_timestamps():
    events, _ = otlp.parse(span(GENAI))
    assert events[0].latency_ms == 900


def test_non_genai_span_is_skipped_not_guessed():
    """A span without gen_ai.* is not an inference. Report it, do not invent one."""
    events, skipped = otlp.parse(span([{"key": "http.method", "value": {"stringValue": "GET"}}]))
    assert events == []
    assert len(skipped) == 1
    assert "gen_ai.system" in skipped[0]["reason"]


def test_error_status_code_maps_to_error():
    events, _ = otlp.parse(span(GENAI, status_code=2))
    assert events[0].status == "error"


def test_finish_reason_accepts_a_list():
    """The convention says finish_reasons is an array; senders differ."""
    attrs = [a for a in GENAI if a["key"] != "gen_ai.response.finish_reasons"]
    attrs.append(
        {
            "key": "gen_ai.response.finish_reasons",
            "value": {"arrayValue": ["length"]},
        }
    )
    events, _ = otlp.parse(span(attrs))
    assert events[0].finish_reason == "length"


def test_event_id_is_generated_when_absent():
    """Foreign senders have no argus.event_id; dedupe still needs a key."""
    events, _ = otlp.parse(span(GENAI))
    assert events[0].event_id is not None


def test_conversation_id_is_read_when_present():
    cid = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
    attrs = [*GENAI, {"key": "gen_ai.conversation.id", "value": {"stringValue": cid}}]
    events, _ = otlp.parse(span(attrs))
    assert str(events[0].conversation_id) == cid


def test_malformed_conversation_id_does_not_raise():
    attrs = [*GENAI, {"key": "gen_ai.conversation.id", "value": {"stringValue": "not-a-uuid"}}]
    events, _ = otlp.parse(span(attrs))
    assert events[0].conversation_id is None


def test_empty_payload_is_not_an_error():
    assert otlp.parse({}) == ([], [])

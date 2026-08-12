"""Worker logic: bucketing, row projection, entry decoding.

No Redis or Postgres — these are the pure functions. The delivery guarantees are
verified against the running stack, because they are properties of the system
rather than of any function.
"""

import json
import sys
from datetime import UTC, datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "services"))

from worker.main import _decode  # noqa: E402
from worker.writer import _aggregate, _row  # noqa: E402


def event(**overrides) -> dict:
    base = {
        "event_id": "11111111-1111-1111-1111-111111111111",
        "provider": "groq",
        "model": "llama-3.1-8b-instant",
        "status": "success",
        "started_at": "2026-08-12T10:30:15.500000Z",
        "latency_ms": 100,
        "ttft_ms": 40,
        "total_tokens": 150,
        "cost_usd": 0.0001,
    }
    base.update(overrides)
    return base


# ---------------------------------------------------------------- bucketing


def test_events_fold_into_one_minute_buckets():
    aggs = _aggregate([event(), event(started_at="2026-08-12T10:30:59.900000Z")])
    assert len(aggs) == 1
    assert aggs[0]["bucket"] == datetime(2026, 8, 12, 10, 30, tzinfo=UTC)
    assert aggs[0]["count"] == 2
    assert aggs[0]["sum_latency_ms"] == 200


def test_different_minutes_are_different_buckets():
    aggs = _aggregate([event(), event(started_at="2026-08-12T10:31:00Z")])
    assert len(aggs) == 2


def test_status_splits_the_bucket():
    """Errors must be countable separately or the error rate is unknowable."""
    aggs = _aggregate([event(), event(status="error", latency_ms=5)])
    assert len(aggs) == 2
    assert {a["status"] for a in aggs} == {"success", "error"}


def test_model_splits_the_bucket():
    aggs = _aggregate([event(), event(model="llama-3.3-70b-versatile")])
    assert len(aggs) == 2


def test_ttft_is_counted_separately_from_calls():
    """Only streamed calls have a TTFT. Dividing by `count` would understate it."""
    aggs = _aggregate([event(ttft_ms=40), event(ttft_ms=None)])
    agg = aggs[0]
    assert agg["count"] == 2
    assert agg["ttft_count"] == 1
    assert agg["sum_ttft_ms"] == 40


def test_max_latency_tracks_the_tail():
    """A crude tail proxy — percentiles are not aggregable from sums."""
    aggs = _aggregate([event(latency_ms=10), event(latency_ms=900), event(latency_ms=50)])
    assert aggs[0]["max_latency_ms"] == 900


def test_missing_numbers_do_not_break_the_fold():
    """A cancelled call has no usage and no cost; it still has to be counted."""
    aggs = _aggregate([event(latency_ms=None, total_tokens=None, cost_usd=None, ttft_ms=None)])
    agg = aggs[0]
    assert agg["count"] == 1
    assert agg["sum_latency_ms"] == 0
    assert agg["sum_cost_usd"] == 0.0
    assert agg["ttft_count"] == 0


def test_empty_batch_produces_no_buckets():
    assert _aggregate([]) == []


# --------------------------------------------------------------- projection


def test_unknown_fields_are_dropped_not_rejected():
    """A newer SDK adding a field must not break an older worker."""
    row = _row(event(some_future_field="whatever"))
    assert "some_future_field" not in row
    assert row["provider"] == "groq"


def test_timestamps_are_parsed_from_iso_strings():
    row = _row(event())
    assert isinstance(row["started_at"], datetime)
    assert row["started_at"].tzinfo is not None


def test_json_columns_are_serialised():
    row = _row(event(redaction_hits={"email": 2}, request_params={"queue_time": 0.1}))
    assert row["redaction_hits"] == '{"email": 2}'
    assert json.loads(row["request_params"])["queue_time"] == 0.1


def test_absent_optional_columns_become_none():
    row = _row(event())
    assert row["error_type"] is None
    assert row["session_id"] is None


# ------------------------------------------------------------------ decode


def test_valid_entry_decodes():
    parsed, error = _decode("1-0", {"data": json.dumps(event())})
    assert error is None
    assert parsed["provider"] == "groq"


def test_missing_data_field_is_an_error():
    parsed, error = _decode("1-0", {"other": "x"})
    assert parsed is None
    assert "no 'data' field" in error


def test_invalid_json_is_an_error_not_an_exception():
    """Undecodable messages are not retryable — redelivery reproduces them."""
    parsed, error = _decode("1-0", {"data": "not json"})
    assert parsed is None
    assert "invalid json" in error


def test_json_without_event_id_is_rejected():
    parsed, error = _decode("1-0", {"data": json.dumps({"hello": "world"})})
    assert parsed is None
    assert "not an inference event" in error

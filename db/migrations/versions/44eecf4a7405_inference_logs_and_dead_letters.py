"""inference logs and dead letters

Revision ID: 44eecf4a7405
Revises: 35dd8f15fad2
Create Date: 2026-08-12

The telemetry side of the schema. Written now, in one migration, because
`inference_logs` and its rollup are one design even though ingestion (P3) only
writes `dead_letter_events` and the worker (P4) writes the rest.

Note the unique key: `(event_id, started_at)`, not `event_id` alone. Postgres
requires every unique constraint on a partitioned table to include the partition
key. That is only safe here because `started_at` is generated client-side by the
SDK and travels in the payload, so a redelivered event carries an identical value
and dedupe still holds. Had we stamped it server-side on arrival, retries would
land in different partitions with different timestamps and the idempotency
guarantee would silently disappear.

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "44eecf4a7405"
down_revision: str | Sequence[str] | None = "35dd8f15fad2"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

# Partitions created up front. A production deployment would add these on a
# schedule (pg_partman or a cron); the DEFAULT partition below is the safety net
# that stops an insert from failing when a month has no partition yet.
_MONTHS = [
    ("2026_07", "2026-07-01", "2026-08-01"),
    ("2026_08", "2026-08-01", "2026-09-01"),
    ("2026_09", "2026-09-01", "2026-10-01"),
    ("2026_10", "2026-10-01", "2026-11-01"),
    ("2026_11", "2026-11-01", "2026-12-01"),
]


def upgrade() -> None:
    """Upgrade schema."""
    op.create_table(
        "inference_logs",
        sa.Column("id", sa.BigInteger(), sa.Identity(), nullable=False),
        # Client-generated idempotency key. Deduping happens at write, which is
        # what makes at-least-once delivery safe.
        sa.Column("event_id", postgresql.UUID(as_uuid=True), nullable=False),
        # Soft reference on purpose — no foreign key. Logs must survive the
        # deletion of the conversation they describe.
        sa.Column("conversation_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("message_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("session_id", sa.Text(), nullable=True),
        sa.Column("service", sa.Text(), nullable=False),
        sa.Column("provider", sa.Text(), nullable=False),
        sa.Column("model", sa.Text(), nullable=False),
        sa.Column("response_model", sa.Text(), nullable=True),
        sa.Column("operation", sa.Text(), nullable=False, server_default="chat"),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("error_type", sa.Text(), nullable=True),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("finish_reason", sa.Text(), nullable=True),
        sa.Column("latency_ms", sa.Integer(), nullable=True),
        sa.Column("ttft_ms", sa.Integer(), nullable=True),
        sa.Column("streamed", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("prompt_tokens", sa.Integer(), nullable=True),
        sa.Column("completion_tokens", sa.Integer(), nullable=True),
        sa.Column("total_tokens", sa.Integer(), nullable=True),
        # NULL rather than 0 when the model's price is unknown. A zero would be
        # indistinguishable from a free call and would quietly understate spend.
        sa.Column("cost_usd", sa.Numeric(12, 6), nullable=True),
        # Previews only, already redacted twice — in the SDK and at the edge.
        sa.Column("input_preview", sa.Text(), nullable=True),
        sa.Column("output_preview", sa.Text(), nullable=True),
        sa.Column("redaction_hits", postgresql.JSONB(), nullable=True),
        # Provider params and vendor extras. JSONB so a new provider quirk does
        # not need a migration.
        sa.Column("request_params", postgresql.JSONB(), nullable=True),
        sa.Column("sdk_version", sa.Text(), nullable=True),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("ended_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "ingested_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.PrimaryKeyConstraint("id", "started_at"),
        sa.UniqueConstraint("event_id", "started_at", name="uq_inference_logs_event"),
        sa.CheckConstraint(
            "status in ('success', 'error', 'cancelled', 'timeout', 'rate_limited')",
            name="ck_inference_logs_status",
        ),
        postgresql_partition_by="RANGE (started_at)",
    )

    for suffix, start, end in _MONTHS:
        op.execute(
            f"CREATE TABLE inference_logs_{suffix} PARTITION OF inference_logs "
            f"FOR VALUES FROM ('{start}') TO ('{end}')"
        )
    op.execute("CREATE TABLE inference_logs_default PARTITION OF inference_logs DEFAULT")

    # Each index serves one real query.
    op.create_index("ix_inference_logs_started", "inference_logs", [sa.text("started_at DESC")])
    op.create_index(
        "ix_inference_logs_conversation",
        "inference_logs",
        ["conversation_id", sa.text("started_at DESC")],
    )
    op.create_index(
        "ix_inference_logs_model",
        "inference_logs",
        ["provider", "model", sa.text("started_at DESC")],
    )
    # Partial: errors are a small fraction of traffic, so the index that serves
    # the error panel stays small and hot.
    op.create_index(
        "ix_inference_logs_errors",
        "inference_logs",
        ["status", sa.text("started_at DESC")],
        postgresql_where=sa.text("status <> 'success'"),
    )

    # Pre-aggregated one-minute buckets. The dashboard reads these rather than
    # scanning raw rows; the cost is ~1 minute of staleness, which is why the
    # agent's tools read raw for windows under 15 minutes.
    op.create_table(
        "inference_metrics_1m",
        sa.Column("bucket", sa.DateTime(timezone=True), nullable=False),
        sa.Column("provider", sa.Text(), nullable=False),
        sa.Column("model", sa.Text(), nullable=False),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("sum_latency_ms", sa.BigInteger(), nullable=False, server_default="0"),
        sa.Column("sum_ttft_ms", sa.BigInteger(), nullable=False, server_default="0"),
        sa.Column("ttft_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("sum_tokens", sa.BigInteger(), nullable=False, server_default="0"),
        sa.Column("sum_cost_usd", sa.Numeric(14, 6), nullable=False, server_default="0"),
        sa.Column("max_latency_ms", sa.Integer(), nullable=True),
        sa.PrimaryKeyConstraint("bucket", "provider", "model", "status"),
    )
    op.create_index("ix_inference_metrics_bucket", "inference_metrics_1m", [sa.text("bucket DESC")])

    # Quarantine, not a bin. A payload that fails validation is kept verbatim
    # alongside the reason, so a schema mismatch can be diagnosed and replayed
    # rather than guessed at.
    op.create_table(
        "dead_letter_events",
        sa.Column("id", sa.BigInteger(), sa.Identity(), primary_key=True),
        sa.Column("raw", postgresql.JSONB(), nullable=False),
        sa.Column("error", sa.Text(), nullable=False),
        sa.Column("source", sa.Text(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
    )
    op.create_index("ix_dead_letter_created", "dead_letter_events", [sa.text("created_at DESC")])


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_index("ix_dead_letter_created", table_name="dead_letter_events")
    op.drop_table("dead_letter_events")
    op.drop_index("ix_inference_metrics_bucket", table_name="inference_metrics_1m")
    op.drop_table("inference_metrics_1m")
    # Dropping the parent drops every partition with it.
    op.drop_table("inference_logs")

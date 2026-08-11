"""conversations and messages

Revision ID: 35dd8f15fad2
Revises: c041b02b1171
Create Date: 2026-08-11

The chat side of the schema. Inference logs are a separate concern and land in
P3/P4 — deliberately, because logs must outlive the conversations they describe.

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "35dd8f15fad2"
down_revision: str | Sequence[str] | None = "c041b02b1171"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_table(
        "conversations",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("title", sa.Text(), nullable=False, server_default="New conversation"),
        # Soft delete: inference logs still point at this row, and analytics
        # should survive a user clearing their history.
        sa.Column("status", sa.Text(), nullable=False, server_default="active"),
        sa.Column("provider", sa.Text(), nullable=True),
        sa.Column("model", sa.Text(), nullable=True),
        # Denormalised so the sidebar never needs a COUNT over messages.
        sa.Column("message_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.CheckConstraint(
            "status in ('active', 'archived', 'deleted')", name="ck_conversations_status"
        ),
    )
    # The sidebar query: most recently touched first, deleted rows excluded.
    # Partial index — it only carries rows the query can actually return.
    op.create_index(
        "ix_conversations_updated",
        "conversations",
        [sa.text("updated_at DESC")],
        postgresql_where=sa.text("status <> 'deleted'"),
    )

    op.create_table(
        "messages",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("conversation_id", postgresql.UUID(as_uuid=True), nullable=False),
        # Per-conversation ordinal. Ordering by created_at is ambiguous for rows
        # written inside the same transaction.
        sa.Column("seq", sa.Integer(), nullable=False),
        sa.Column("role", sa.Text(), nullable=False),
        sa.Column("content", sa.Text(), nullable=False),
        sa.Column("tool_call_id", sa.Text(), nullable=True),
        sa.Column("token_count", sa.Integer(), nullable=True),
        # Set when a stream was cut short, so the UI can label a partial reply.
        sa.Column("truncated", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.ForeignKeyConstraint(["conversation_id"], ["conversations.id"], ondelete="CASCADE"),
        sa.CheckConstraint(
            "role in ('user', 'assistant', 'system', 'tool')", name="ck_messages_role"
        ),
        # Makes gaps and duplicate ordinals impossible rather than merely unlikely.
        sa.UniqueConstraint("conversation_id", "seq", name="uq_messages_conversation_seq"),
    )
    # Covers the only read path that matters: replay one conversation in order.
    op.create_index("ix_messages_conversation_seq", "messages", ["conversation_id", "seq"])


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_index("ix_messages_conversation_seq", table_name="messages")
    op.drop_table("messages")
    op.drop_index("ix_conversations_updated", table_name="conversations")
    op.drop_table("conversations")

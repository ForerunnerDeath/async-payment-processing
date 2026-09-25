from typing import cast

from sqlalchemy import Table
from sqlalchemy.dialects import postgresql
from sqlalchemy.schema import CreateIndex

from app.models.outbox_event import OutboxEvent


def test_outbox_event_table_has_expected_columns() -> None:
    columns = OutboxEvent.__table__.columns

    assert "id" in columns
    assert "event_type" in columns
    assert "payment_id" in columns
    assert "payload" in columns
    assert "created_at" in columns
    assert "published_at" in columns


def test_outbox_event_has_unpublished_partial_index() -> None:
    table = cast(Table, OutboxEvent.__table__)

    index = next(
        candidate
        for candidate in table.indexes
        if candidate.name == "ix_outbox_events_unpublished_created_at_id"
    )

    assert [column.name for column in index.columns] == [
        "created_at",
        "id",
    ]

    compiled_index = str(
        CreateIndex(index).compile(
            dialect=postgresql.dialect(),
        )
    )

    assert "WHERE published_at IS NULL" in compiled_index

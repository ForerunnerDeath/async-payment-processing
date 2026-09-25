import json
from collections.abc import Iterator

import pytest
import structlog

from app.core.logging import configure_logging


@pytest.fixture(autouse=True)
def reset_structlog() -> Iterator[None]:
    structlog.reset_defaults()
    yield
    structlog.reset_defaults()


def test_configure_logging_outputs_structured_json(
    capsys: pytest.CaptureFixture[str],
) -> None:
    configure_logging("INFO")

    logger = structlog.get_logger()
    logger.info("payment_created", payment_id="payment-123")

    output = capsys.readouterr().out.strip()
    event = json.loads(output)

    assert event["event"] == "payment_created"
    assert event["payment_id"] == "payment-123"
    assert event["level"] == "info"
    assert "timestamp" in event


def test_configure_logging_filters_messages_below_configured_level(
    capsys: pytest.CaptureFixture[str],
) -> None:
    configure_logging("WARNING")

    logger = structlog.get_logger()
    logger.info("should_not_be_logged")
    logger.warning("should_be_logged")

    output = capsys.readouterr().out.strip().splitlines()

    assert len(output) == 1

    event = json.loads(output[0])

    assert event["event"] == "should_be_logged"
    assert event["level"] == "warning"

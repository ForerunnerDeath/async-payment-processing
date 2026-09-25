from typing import cast
from uuid import UUID

import pytest
from fastapi import Request, Response
from starlette.types import Scope
from structlog.contextvars import get_contextvars

from app.core import middleware as middleware_module


def make_request(
    headers: list[tuple[bytes, bytes]] | None = None,
) -> Request:
    scope = cast(
        Scope,
        {
            "type": "http",
            "asgi": {
                "version": "3.0",
                "spec_version": "2.3",
            },
            "http_version": "1.1",
            "method": "GET",
            "scheme": "http",
            "path": "/test",
            "raw_path": b"/test",
            "query_string": b"",
            "root_path": "",
            "headers": headers or [],
            "client": ("127.0.0.1", 12345),
            "server": ("testserver", 80),
            "state": {},
        },
    )

    return Request(scope)


async def test_request_id_middleware_preserves_client_request_id() -> None:
    request = make_request(
        headers=[
            (b"x-request-id", b"client-request-id"),
        ]
    )

    async def call_next(_: Request) -> Response:
        context = get_contextvars()

        assert context["request_id"] == "client-request-id"
        assert context["method"] == "GET"
        assert context["path"] == "/test"

        return Response(status_code=204)

    response = await middleware_module.request_id_middleware(
        request,
        call_next,
    )

    assert response.headers["X-Request-ID"] == "client-request-id"
    assert request.state.request_id == "client-request-id"
    assert get_contextvars() == {}


async def test_request_id_middleware_generates_request_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    generated_request_id = UUID("12345678-1234-5678-1234-567812345678")

    monkeypatch.setattr(
        middleware_module,
        "uuid4",
        lambda: generated_request_id,
    )

    request = make_request()

    async def call_next(_: Request) -> Response:
        return Response(status_code=200)

    response = await middleware_module.request_id_middleware(
        request,
        call_next,
    )

    expected_request_id = str(generated_request_id)

    assert response.headers["X-Request-ID"] == expected_request_id
    assert request.state.request_id == expected_request_id
    assert get_contextvars() == {}


async def test_request_id_middleware_clears_context_after_exception() -> None:
    request = make_request(
        headers=[
            (b"x-request-id", b"failed-request"),
        ]
    )

    async def call_next(_: Request) -> Response:
        context = get_contextvars()

        assert context["request_id"] == "failed-request"

        raise RuntimeError("request failed")

    with pytest.raises(RuntimeError, match="request failed"):
        await middleware_module.request_id_middleware(
            request,
            call_next,
        )

    assert get_contextvars() == {}

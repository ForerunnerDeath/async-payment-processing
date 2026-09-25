FROM python:3.14-slim AS builder

ENV POETRY_NO_INTERACTION=1 \
    POETRY_VIRTUALENVS_CREATE=false \
    VIRTUAL_ENV=/opt/venv \
    PATH="/opt/venv/bin:/opt/poetry/bin:$PATH"

RUN python -m venv /opt/venv \
    && python -m venv /opt/poetry \
    && /opt/poetry/bin/pip install --no-cache-dir "poetry==2.2.1"

WORKDIR /build

COPY pyproject.toml poetry.lock ./

RUN poetry install --only main --no-root

FROM python:3.14-slim AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    VIRTUAL_ENV=/opt/venv \
    PATH="/opt/venv/bin:$PATH"

WORKDIR /service

COPY --from=builder /opt/venv /opt/venv
COPY app ./app

CMD ["python", "-m", "uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8001"]

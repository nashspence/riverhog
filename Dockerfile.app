FROM python:3.11.15-slim-bookworm

ENV PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app:/app/src

WORKDIR /app

COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install --no-cache-dir -e .[db]

COPY . .

ENTRYPOINT ["uvicorn", "arc_api.app:create_app", "--factory", "--host", "0.0.0.0", "--port", "8000"]

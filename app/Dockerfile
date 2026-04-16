FROM golang:1.24-bookworm AS age-builder
RUN go install filippo.io/age/cmd/...@latest

FROM python:3.12-slim
WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN apt-get update && apt-get install -y --no-install-recommends \
    xorriso \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=age-builder /go/bin/age /usr/local/bin/age
COPY --from=age-builder /go/bin/age-keygen /usr/local/bin/age-keygen
COPY --from=age-builder /go/bin/age-plugin-batchpass /usr/local/bin/age-plugin-batchpass

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app.py .

CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]

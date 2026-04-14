FROM python:3.11-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends build-essential libpq-dev curl \
    && rm -rf /var/lib/apt/lists/*

COPY libs/python-common /app/libs/python-common
RUN pip install -e /app/libs/python-common

COPY libs/x12-834 /app/libs/x12-834
RUN pip install -e /app/libs/x12-834

RUN pip install fastapi==0.111.0 'uvicorn[standard]==0.30.0' \
    sqlalchemy==2.0.29 asyncpg==0.29.0 'psycopg[binary]==3.1.19' \
    httpx==0.27.0 pydantic==2.7.1 pydantic-settings==2.2.1 \
    structlog==24.1.0 tenacity==8.2.3 cryptography==42.0.7 \
    google-cloud-pubsub==2.21.1 boto3==1.34.131 \
    opentelemetry-api==1.24.0 opentelemetry-sdk==1.24.0 \
    opentelemetry-exporter-otlp==1.24.0 \
    opentelemetry-instrumentation-fastapi==0.45b0 \
    opentelemetry-instrumentation-sqlalchemy==0.45b0
COPY ingestion /app/service
WORKDIR /app/service
ENV PYTHONPATH=/app/service:/app/libs/python-common/src:/app/libs/x12-834/src

EXPOSE 8000
HEALTHCHECK --interval=10s --timeout=3s --retries=20 CMD curl -fsS http://localhost:8000/livez || exit 1
CMD ["python", "-m", "app.main"]

# eligibility-workers

Three stateless consumers that back the CQRS + event-driven side of the platform.

| Worker | Purpose |
|---|---|
| `ingestion/` | Subscribes to `file.received`. Downloads uploaded 834/CSV from MinIO, parses, maps each INS loop to an atlas command, handles idempotency via `(trading_partner, ISA13, GS06, ST02, ins_pos)`. |
| `projector/` | Subscribes to domain events. Maintains the denormalized `eligibility_view` table and the OpenSearch `eligibility` index. Graceful fallback to pg-only on OS failure. |
| `outbox-relay/` | Polls `outbox` tables in each service DB, publishes to Pub/Sub with retry+exp-backoff, marks `published_at`. Guarantees at-least-once event delivery without 2PC. |

## Run

Each worker has its own `Dockerfile`:

```bash
docker build -t eligibility-ingestion:local    -f ingestion/Dockerfile .
docker build -t eligibility-projector:local    -f projector/Dockerfile .
docker build -t eligibility-outbox-relay:local -f outbox-relay/Dockerfile .
```

For local dev use the orchestration repo: [`eligibility-platform`](https://github.com/SamieZian/eligibility-platform).

## Test

```bash
python -m venv .venv && source .venv/bin/activate
pip install -e libs/python-common -e libs/x12-834
pip install fastapi 'uvicorn[standard]' sqlalchemy asyncpg 'psycopg[binary]' \
  httpx pydantic pydantic-settings structlog tenacity cryptography \
  google-cloud-pubsub boto3 pytest pytest-asyncio \
  opentelemetry-api opentelemetry-sdk opentelemetry-exporter-otlp

for w in ingestion projector outbox-relay; do
  echo "--- $w"
  (cd $w && PYTHONPATH=.:../libs/python-common/src:../libs/x12-834/src python -m pytest tests -q)
done
```

## Dependencies

- Pub/Sub (emulator locally, GCP Pub/Sub in prod)
- Postgres (one per service — atlas_db, member_db, group_db, plan_db)
- OpenSearch (for projector)
- MinIO / S3 (for ingestion)

## License

MIT.

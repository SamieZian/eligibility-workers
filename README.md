# eligibility-workers

Three stateless consumers that back the CQRS + event-driven side of the platform.

| Worker | Purpose |
|---|---|
| `ingestion/` | Subscribes to `file.received` Pub/Sub. Downloads uploaded 834/CSV from MinIO, parses with the streaming X12 parser, maps each INS loop to an atlas command, and POSTs. Idempotency via `(trading_partner, ISA13, GS06, ST02, INS_position)`. |
| `projector/` | Subscribes to all domain events (`MemberUpserted`, `PlanUpserted`, `EmployerUpserted`, `EnrollmentAdded/Changed/Terminated`). Maintains the denormalized `eligibility_view` table + OpenSearch `eligibility` index. Graceful fallback to pg-only on OpenSearch failure. |
| `outbox-relay/` | Polls the `outbox` table in each service DB (atlas / member / group / plan). Publishes unsent rows to Pub/Sub with retry + exponential backoff, marks `published_at` on success. Guarantees **at-least-once event delivery** without 2PC. |

## Prerequisites

| Tool | Version |
|---|---|
| Docker | 24+ |
| Docker Compose | v2 |
| Python | 3.11+ (standalone dev) |

## Run with the rest of the platform

```bash
git clone https://github.com/SamieZian/eligibility-platform
cd eligibility-platform
./bootstrap.sh
make up
```

## Run a worker standalone

Each worker has its own Dockerfile.

```bash
# Configure
cp .env.example .env

# Build a single worker image
docker build -t eligibility-ingestion:local    -f ingestion/Dockerfile    .
docker build -t eligibility-projector:local    -f projector/Dockerfile    .
docker build -t eligibility-outbox-relay:local -f outbox-relay/Dockerfile .

# Run (requires Pub/Sub emulator + Postgres + MinIO running)
docker run --rm --env-file .env eligibility-ingestion:local
```

## Develop locally

```bash
# Install Poetry 1.8.3 if you don't have it
pipx install poetry==1.8.3  # or: pip install --user poetry==1.8.3

# One pyproject.toml at the repo root covers all three workers + both vendored libs
poetry install
```

## Test

```bash
for w in ingestion projector outbox-relay; do
  echo "--- $w"
  (cd $w && PYTHONPATH=.:../libs/python-common/src:../libs/x12-834/src \
     poetry run pytest tests -q)
done
```

## Environment variables

See [`.env.example`](.env.example).

## License

MIT.

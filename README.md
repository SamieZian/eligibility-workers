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

## AI-assisted extraction

The ingestion worker optionally uses **Vertex AI Document AI** to parse scanned 834 PDFs and image-fax enrollments from small providers. The path is strictly opt-in: when `VERTEX_AI_DOCUMENT_PROCESSOR_ID` + `GOOGLE_CLOUD_PROJECT` are unset, scanned uploads surface as a structured `ingestion.pdf_scanned_without_docai` warning and the rest of the pipeline is unchanged. Enable by setting those two env vars (plus optional `VERTEX_AI_LOCATION`, default `us`) and installing the optional dependency with `poetry install --extras ai`. See [`docs/runbooks/anomaly-detection.md`](../eligibility-platform/docs/runbooks/anomaly-detection.md) for rollout + alert thresholds.

## Testing via curl

Workers are **async Pub/Sub consumers** — they have no direct HTTP endpoints. Exercise each via the BFF REST / GraphQL surface and watch the downstream effects in the DB / OS.

```bash
BFF=http://localhost:4000
T=11111111-1111-1111-1111-111111111111
```

**1. Ingestion (834 EDI + CSV)**

```bash
# 834 EDI — streaming parser, idempotent on ISA13:GS06:ST02:ins_sequence
curl -s -X POST $BFF/files/eligibility \
  -H "X-Tenant-Id: $T" -H "X-Correlation-Id: $(uuidgen)" \
  -F "file=@samples/834_demo.x12" | jq .

# CSV — row-hash idempotency
curl -s -X POST $BFF/files/eligibility \
  -H "X-Tenant-Id: $T" -H "X-Correlation-Id: $(uuidgen)" \
  -F "file=@samples/members_demo.csv" | jq .
# → {"file_id": "...", "job_id": "...", "status": "UPLOADED"}

# Watch the ingestion worker logs
docker compose logs -f --tail 30 ingestion
```

**Job status**

```bash
curl -s -X POST $BFF/graphql \
  -H "Content-Type: application/json" -H "X-Tenant-Id: $T" \
  -d "{\"query\":\"{ fileJob(fileId:\\\"$FILE_ID\\\"){ status totalRows successRows failedRows } }\"}" | jq .
```

**2. Projector (CQRS read-side)** — watch it fan out MemberUpserted / EnrollmentAdded into `eligibility_view` + OpenSearch + Redis pub/sub.

```bash
# Trigger: any mutation that emits events
curl -s -X POST $BFF/graphql \
  -H "Content-Type: application/json" -H "X-Tenant-Id: $T" \
  -d "{\"query\":\"mutation { updateMemberDemographics(memberId:\\\"$MID\\\", lastName:\\\"SHARMA-V2\\\") }\"}"

# See the projection
docker compose exec -T atlas_db psql -U postgres -d atlas_db \
  -c "SELECT member_name, status, effective_date FROM eligibility_view WHERE member_id='$MID';"

# See OpenSearch index
curl -s "http://localhost:9200/eligibility/_search?q=sharma" | jq .

# Watch projector logs
docker compose logs -f --tail 30 projector
```

**3. Outbox relay** — polls each service DB's `outbox` table, publishes to Pub/Sub, sets `published_at`.

```bash
# Count unpublished rows (should hover near 0 — relay publishes within seconds)
for db in atlas member group plan; do
  printf "%-8s " $db
  docker compose exec -T ${db}_db psql -U postgres -d ${db}_db \
    -t -c "SELECT COUNT(*) FROM outbox WHERE published_at IS NULL;" | tr -d ' '
done

docker compose logs -f --tail 30 outbox_relay
```

**4. Chaos test — kill projector, keep writing, restart, watch it catch up**

```bash
docker compose kill projector            # stop projections
# Fire 5 mutations via the BFF
for i in 1 2 3 4 5; do
  curl -s -X POST $BFF/graphql -H "Content-Type: application/json" -H "X-Tenant-Id: $T" \
    -d "{\"query\":\"mutation { updateMemberDemographics(memberId:\\\"$MID\\\", lastName:\\\"BURST-$i\\\") }\"}"
done
docker compose start projector           # it drains the backlog from Pub/Sub
sleep 4
# Final state should reflect the last write (BURST-5) — eventual consistency proven
```

## License

MIT.

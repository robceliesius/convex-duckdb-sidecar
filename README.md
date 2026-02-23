# convex-duckdb-sidecar

A lightweight HTTP service that runs DuckDB queries and writes Parquet files to S3. Companion to [`@robceliesius/convex-duckdb`](https://github.com/robceliesius/convex-duckdb).

## What It Does

The sidecar handles the heavy lifting that can't run inside Convex:

- **Snapshot** — Receives JSON data, converts it to Parquet (ZSTD-compressed), and uploads to S3/MinIO
- **Query** — Spins up an ephemeral DuckDB instance, attaches Parquet files from S3, executes SQL, and returns results

Each request gets a fresh `:memory:` DuckDB instance — no state leaks between requests.

## Quick Start

### Docker (recommended)

```bash
docker build -t duckdb-sidecar .
docker run --rm -p 3214:3214 duckdb-sidecar
```

Notes:
- DuckDB uses native bindings. Alpine-based images frequently fail to load DuckDB (`ERR_DLOPEN_FAILED` / missing `ld-linux-*`), so this sidecar uses a glibc-based base image (`node:22-bookworm-slim`).
- The sidecar is stateless. You can scale it horizontally.

### Standalone

```bash
npm install
npm run build
PORT=3214 node dist/server.js
```

### Development

```bash
npm run dev
```

## Example .env (for local dev scripts)

The sidecar reads runtime tuning from environment variables. S3/MinIO config is passed per-request in `s3_config`.

That said, having a `.env` is convenient for curl scripts and local tooling:

```bash
# .env
PORT=3214
DUCKDB_THREADS=12
DUCKDB_MEMORY_LIMIT=6GB
DUCKDB_ENABLE_OBJECT_CACHE=true
SIDECAR_MAX_CONCURRENT_QUERIES=12
SIDECAR_MAX_QUERY_QUEUE=200
SIDECAR_MAX_CONCURRENT_SNAPSHOTS=4
SIDECAR_MAX_SNAPSHOT_QUEUE=100

# Used by your shell scripts/curl examples (not read automatically by the sidecar).
S3_ENDPOINT_URL=http://localhost:9000
ANALYTICS_S3_BUCKET=analytics-parquet
AWS_REGION=us-east-1
AWS_ACCESS_KEY_ID=minioadmin
AWS_SECRET_ACCESS_KEY=minioadmin
```

## Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/health` | `GET` | Returns `{ "status": "ok", "service": "duckdb-sidecar" }` |
| `/snapshot` | `POST` | Convert data to Parquet and upload to S3 |
| `/query` | `POST` | Execute SQL over Parquet files in S3 |

### `POST /snapshot`

```json
{
  "table_name": "orders",
  "data": [{ "_id": "abc", "total": 99.99 }],
  "columns": [{ "source": "_id", "target": "_id", "type": "VARCHAR" }],
  "s3_key": "tenant/orders/1706140800000.parquet",
  "s3_config": {
    "endpoint": "http://minio:9000",
    "bucket": "analytics",
    "accessKeyId": "...",
    "secretAccessKey": "...",
    "forcePathStyle": true
  }
}
```

Curl example (using `.env` from above):

```bash
set -a
source .env
set +a

ts="$(date +%s%3N)"
key="smoke/${ts}.parquet"

curl -sS -X POST "http://localhost:${PORT}/snapshot" \
  -H "Content-Type: application/json" \
  -d "{
    \"table_name\": \"orders\",
    \"data\": [{\"id\":\"a\",\"total\":99.99}],
    \"columns\": [
      {\"source\":\"id\",\"target\":\"id\",\"type\":\"VARCHAR\"},
      {\"source\":\"total\",\"target\":\"total\",\"type\":\"DOUBLE\"}
    ],
    \"s3_key\": \"${key}\",
    \"s3_config\": {
      \"endpoint\": \"${S3_ENDPOINT_URL}\",
      \"bucket\": \"${ANALYTICS_S3_BUCKET}\",
      \"region\": \"${AWS_REGION}\",
      \"accessKeyId\": \"${AWS_ACCESS_KEY_ID}\",
      \"secretAccessKey\": \"${AWS_SECRET_ACCESS_KEY}\",
      \"forcePathStyle\": true
    }
  }"
echo
echo "wrote s3://${ANALYTICS_S3_BUCKET}/${key}"
```

### `POST /query`

```json
{
  "sql": "SELECT COUNT(*) as total FROM orders",
  "tables": [{ "name": "orders", "s3_path": "tenant/orders/1706140800000.parquet" }],
  "s3_config": {
    "endpoint": "http://minio:9000",
    "bucket": "analytics",
    "accessKeyId": "...",
    "secretAccessKey": "...",
    "forcePathStyle": true
  }
}
```

Curl example (query a single Parquet object):

```bash
set -a
source .env
set +a

key="smoke/1706140800000.parquet"

curl -sS -X POST "http://localhost:${PORT}/query" \
  -H "Content-Type: application/json" \
  -d "{
    \"sql\": \"SELECT COUNT(*)::BIGINT AS c FROM orders\",
    \"tables\": [{\"name\":\"orders\",\"s3_path\":\"${key}\"}],
    \"s3_config\": {
      \"endpoint\": \"${S3_ENDPOINT_URL}\",
      \"bucket\": \"${ANALYTICS_S3_BUCKET}\",
      \"region\": \"${AWS_REGION}\",
      \"accessKeyId\": \"${AWS_ACCESS_KEY_ID}\",
      \"secretAccessKey\": \"${AWS_SECRET_ACCESS_KEY}\",
      \"forcePathStyle\": true
    }
  }"
echo
```

## Configuration

| Environment Variable | Default | Description |
|---------------------|---------|-------------|
| `PORT` | `3214` | HTTP server port |
| `DUCKDB_THREADS` | unset | DuckDB worker thread count (`SET threads`) |
| `DUCKDB_MEMORY_LIMIT` | `6GB` | DuckDB memory limit (`SET memory_limit`) |
| `DUCKDB_ENABLE_OBJECT_CACHE` | `true` | DuckDB object cache (`SET enable_object_cache`) |
| `SIDECAR_MAX_CONCURRENT_QUERIES` | `12` | Max concurrent `/query` executions |
| `SIDECAR_MAX_QUERY_QUEUE` | `200` | Max queued `/query` requests before returning `503` |
| `SIDECAR_MAX_CONCURRENT_SNAPSHOTS` | `4` | Max concurrent `/snapshot` executions |
| `SIDECAR_MAX_SNAPSHOT_QUEUE` | `100` | Max queued `/snapshot` requests before returning `503` |

S3 credentials are passed per-request in the `s3_config` body field (not via environment variables), so a single sidecar instance can serve multiple tenants/buckets.

### Coolify Production Example

Set these in the DuckDB sidecar app environment:

```bash
PORT=3214
DUCKDB_THREADS=12
DUCKDB_MEMORY_LIMIT=6GB
DUCKDB_ENABLE_OBJECT_CACHE=true
SIDECAR_MAX_CONCURRENT_QUERIES=12
SIDECAR_MAX_QUERY_QUEUE=200
SIDECAR_MAX_CONCURRENT_SNAPSHOTS=4
SIDECAR_MAX_SNAPSHOT_QUEUE=100
```

## Docker Compose Example

```yaml
services:
  duckdb-sidecar:
    build: ./duckdb-sidecar
    ports:
      - "3214:3214"
    depends_on:
      - minio

  minio:
    image: minio/minio
    command: server /data --console-address ":9001"
    ports:
      - "9000:9000"
      - "9001:9001"
    environment:
      MINIO_ROOT_USER: minioadmin
      MINIO_ROOT_PASSWORD: minioadmin
```

## Architecture

```
Convex App  ──HTTP──▶  Sidecar  ──S3 API──▶  MinIO / S3
                         │
                    DuckDB (in-memory)
                    Parquet (ZSTD)
```

- **Express 5** HTTP server with 50MB request body limit
- **duckdb-async** for query execution
- **@aws-sdk/client-s3** for object storage
- **Node.js 22+** required
- Multi-stage Docker build

## Related

- [`@robceliesius/convex-duckdb`](https://github.com/robceliesius/convex-duckdb) — The Convex component (client API, metadata, schema)

## License

MIT

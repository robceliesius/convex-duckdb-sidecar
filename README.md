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
docker run -p 3214:3214 duckdb-sidecar
```

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

## Configuration

| Environment Variable | Default | Description |
|---------------------|---------|-------------|
| `PORT` | `3214` | HTTP server port |

S3 credentials are passed per-request in the `s3_config` body field (not via environment variables), so a single sidecar instance can serve multiple tenants/buckets.

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
- **Node.js 22+** required (Alpine Docker image)
- Multi-stage Docker build (~120MB image)

## Related

- [`@robceliesius/convex-duckdb`](https://github.com/robceliesius/convex-duckdb) — The Convex component (client API, metadata, schema)

## License

MIT

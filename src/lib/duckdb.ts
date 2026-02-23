import { Database } from "duckdb-async";
import { sqlStringLiteral } from "./sql.js";

export interface S3Config {
  endpoint: string;
  bucket: string;
  region?: string;
  accessKeyId: string;
  secretAccessKey: string;
  forcePathStyle?: boolean;
}

function parsePositiveIntEnv(name: string): number | null {
  const raw = process.env[name];
  if (!raw) return null;
  const parsed = Number.parseInt(raw, 10);
  if (!Number.isFinite(parsed) || parsed <= 0) return null;
  return parsed;
}

function parseBoolEnv(name: string, fallback: boolean): boolean {
  const raw = process.env[name];
  if (!raw) return fallback;
  const normalized = raw.trim().toLowerCase();
  if (normalized === "true" || normalized === "1" || normalized === "yes") return true;
  if (normalized === "false" || normalized === "0" || normalized === "no") return false;
  return fallback;
}

export async function createDuckDB(s3Config: S3Config): Promise<Database> {
  const db = await Database.create(":memory:");
  await db.run("INSTALL httpfs; LOAD httpfs;");

  // DuckDB `httpfs` wants `s3_endpoint` without scheme (host[:port]) plus flags
  // for SSL and URL style.
  const endpointUrl = new URL(s3Config.endpoint);
  if (endpointUrl.protocol !== "http:" && endpointUrl.protocol !== "https:") {
    throw new Error(`Unsupported S3 endpoint protocol: ${endpointUrl.protocol}`);
  }
  // Avoid surprising behavior if someone passes a path (DuckDB treats endpoint
  // as host[:port], not a full URL).
  if (endpointUrl.pathname !== "/" && endpointUrl.pathname !== "") {
    throw new Error(
      `S3 endpoint must not include a path: ${s3Config.endpoint}`,
    );
  }

  const endpointHost = endpointUrl.host; // includes port if present
  const useSsl = endpointUrl.protocol === "https:";

  await db.run(`SET s3_endpoint=${sqlStringLiteral(endpointHost)};`);
  await db.run(`SET s3_access_key_id=${sqlStringLiteral(s3Config.accessKeyId)};`);
  // DuckDB uses `s3_secret_access_key` (not `s3_secret_key`).
  await db.run(
    `SET s3_secret_access_key=${sqlStringLiteral(s3Config.secretAccessKey)};`,
  );
  await db.run(
    `SET s3_region=${sqlStringLiteral(s3Config.region ?? "us-east-1")};`,
  );
  await db.run(`SET s3_use_ssl=${useSsl ? "true" : "false"};`);

  const forcePathStyle = s3Config.forcePathStyle ?? true;
  const urlStyle = forcePathStyle ? "path" : "vhost";
  await db.run(`SET s3_url_style=${sqlStringLiteral(urlStyle)};`);

  // Runtime tuning (configurable via environment for production sidecar deploys).
  const duckDbThreads = parsePositiveIntEnv("DUCKDB_THREADS");
  if (duckDbThreads !== null) {
    await db.run(`SET threads=${duckDbThreads};`);
  }

  const memoryLimit = (process.env.DUCKDB_MEMORY_LIMIT ?? "6GB").trim();
  if (memoryLimit.length > 0) {
    await db.run(`SET memory_limit=${sqlStringLiteral(memoryLimit)};`);
  }

  const enableObjectCache = parseBoolEnv("DUCKDB_ENABLE_OBJECT_CACHE", true);
  await db.run(`SET enable_object_cache=${enableObjectCache ? "true" : "false"};`);

  return db;
}

export function getS3ConfigFromEnv(): S3Config {
  // Support both pool-specific names and existing sidecar env var names
  const endpoint = process.env.S3_ENDPOINT ?? process.env.S3_ENDPOINT_URL;
  const bucket = process.env.S3_BUCKET ?? process.env.ANALYTICS_S3_BUCKET;
  const accessKeyId = process.env.S3_ACCESS_KEY_ID ?? process.env.AWS_ACCESS_KEY_ID;
  const secretAccessKey = process.env.S3_SECRET_ACCESS_KEY ?? process.env.AWS_SECRET_ACCESS_KEY;
  if (!endpoint || !bucket || !accessKeyId || !secretAccessKey) {
    throw new Error(
      "Missing S3 pool env vars. Set S3_ENDPOINT (or S3_ENDPOINT_URL), S3_BUCKET (or ANALYTICS_S3_BUCKET), S3_ACCESS_KEY_ID (or AWS_ACCESS_KEY_ID), S3_SECRET_ACCESS_KEY (or AWS_SECRET_ACCESS_KEY)",
    );
  }
  return { endpoint, bucket, accessKeyId, secretAccessKey, forcePathStyle: true };
}

export async function createPooledDuckDB(): Promise<Database> {
  return createDuckDB(getS3ConfigFromEnv());
}

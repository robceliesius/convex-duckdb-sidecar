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

  return db;
}

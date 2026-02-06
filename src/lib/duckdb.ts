import { Database } from "duckdb-async";

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

  const endpoint = s3Config.endpoint.replace(/^https?:\/\//, "");
  await db.run(`SET s3_endpoint='${endpoint}';`);
  await db.run(`SET s3_access_key_id='${s3Config.accessKeyId}';`);
  await db.run(`SET s3_secret_key='${s3Config.secretAccessKey}';`);
  await db.run(`SET s3_region='${s3Config.region ?? "us-east-1"}';`);
  await db.run("SET s3_use_ssl=false;");
  await db.run("SET s3_url_style='path';");

  return db;
}

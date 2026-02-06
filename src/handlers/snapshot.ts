import { Request, Response } from "express";
import { Database } from "duckdb-async";
import { createS3Client, uploadToS3 } from "../lib/s3.js";
import type { S3Config } from "../lib/duckdb.js";
import fs from "fs/promises";
import path from "path";
import os from "os";

interface ColumnDef {
  source: string;
  target: string;
  type: string;
}

interface SnapshotRequest {
  table_name: string;
  data: Record<string, unknown>[];
  columns: ColumnDef[];
  s3_key: string;
  s3_config: S3Config;
}

const BATCH_SIZE = 1000;

export async function handleSnapshot(req: Request, res: Response) {
  const body = req.body as SnapshotRequest;

  if (!body.table_name || !body.data || !body.columns || !body.s3_key || !body.s3_config) {
    res.status(400).json({ error: "Missing required fields: table_name, data, columns, s3_key, s3_config" });
    return;
  }

  let db: Database | null = null;
  let tmpPath: string | null = null;

  try {
    db = await Database.create(":memory:");

    // Create table
    const colDefs = body.columns.map((c) => `"${c.target}" ${c.type}`).join(", ");
    await db.run(`CREATE TABLE export_data (${colDefs});`);

    // Batch insert
    if (body.data.length > 0) {
      const placeholders = body.columns.map(() => "?").join(", ");
      const stmt = await db.prepare(`INSERT INTO export_data VALUES (${placeholders})`);

      for (let i = 0; i < body.data.length; i += BATCH_SIZE) {
        const batch = body.data.slice(i, i + BATCH_SIZE);
        for (const row of batch) {
          const values = body.columns.map((col) => {
            const val = row[col.source];
            return val === undefined ? null : val;
          });
          await stmt.run(...values);
        }
      }
      await stmt.finalize();
    }

    // Export to Parquet
    tmpPath = path.join(os.tmpdir(), `duckdb_snap_${Date.now()}.parquet`);
    await db.run(`COPY export_data TO '${tmpPath}' (FORMAT PARQUET, COMPRESSION ZSTD);`);
    await db.close();
    db = null;

    // Upload to S3
    const buffer = await fs.readFile(tmpPath);
    const s3 = createS3Client(body.s3_config);
    await uploadToS3(s3, body.s3_config.bucket, body.s3_key, buffer);

    res.json({
      s3_key: body.s3_key,
      row_count: body.data.length,
      parquet_size_bytes: buffer.length,
    });
  } catch (error) {
    console.error("[snapshot] Error:", error);
    res.status(500).json({
      error: error instanceof Error ? error.message : String(error),
    });
  } finally {
    if (db) await db.close().catch(() => {});
    if (tmpPath) await fs.unlink(tmpPath).catch(() => {});
  }
}

import { Request, Response } from "express";
import { Database } from "duckdb-async";
import { createS3Client, uploadToS3 } from "../lib/s3.js";
import fs from "fs/promises";
import path from "path";
import os from "os";
import {
  normalizeDuckDbType,
  sqlIdentifier,
  sqlStringLiteral,
} from "../lib/sql.js";
import {
  assertArray,
  assertNonEmptyString,
  assertRecord,
  parseS3Config,
} from "../lib/validate.js";

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
  s3_config: unknown;
}

const BATCH_SIZE = 1000;

export async function handleSnapshot(req: Request, res: Response) {
  const DEBUG = process.env.LOG_LEVEL === "debug";

  let s3Key: string;
  let s3Config: ReturnType<typeof parseS3Config>;
  let data: Record<string, unknown>[];
  let columns: ColumnDef[];
  try {
    const body = assertRecord(req.body, "body") as unknown as SnapshotRequest;

    // Validate required fields early for clearer 400s.
    assertNonEmptyString(body.table_name, "table_name");
    s3Key = assertNonEmptyString(body.s3_key, "s3_key");
    s3Config = parseS3Config(body.s3_config);

    const rawData = assertArray(body.data, "data");
    data = rawData.map((row, i) => {
      // Snapshot rows must be objects.
      return assertRecord(row, `data[${i}]`) as Record<string, unknown>;
    });

    const rawColumns = assertArray(body.columns, "columns");
    columns = rawColumns.map((c, i) => {
      const obj = assertRecord(c, `columns[${i}]`);
      return {
        source: assertNonEmptyString(obj.source, `columns[${i}].source`),
        target: assertNonEmptyString(obj.target, `columns[${i}].target`),
        type: normalizeDuckDbType(assertNonEmptyString(obj.type, `columns[${i}].type`)),
      };
    });

    if (columns.length === 0) {
      throw new Error("columns must be non-empty");
    }
  } catch (error) {
    res.status(400).json({
      error: error instanceof Error ? error.message : String(error),
    });
    return;
  }

  let db: Database | null = null;
  let tmpPath: string | null = null;

  try {
    db = await Database.create(":memory:");

    // Create table
    const colDefs = columns
      .map((c) => `${sqlIdentifier(c.target)} ${c.type}`)
      .join(", ");
    await db.run(`CREATE TABLE export_data (${colDefs});`);

    // Batch insert
    if (data.length > 0) {
      const placeholders = columns.map(() => "?").join(", ");
      const stmt = await db.prepare(`INSERT INTO export_data VALUES (${placeholders})`);
      try {
        for (let i = 0; i < data.length; i += BATCH_SIZE) {
          const batch = data.slice(i, i + BATCH_SIZE);
          for (const row of batch) {
            const values = columns.map((col) => {
              const val = row[col.source];
              return val === undefined ? null : val;
            });
            await stmt.run(...values);
          }
        }
      } finally {
        await stmt.finalize().catch(() => {});
      }
    }

    // Export to Parquet
    tmpPath = path.join(os.tmpdir(), `duckdb_snap_${Date.now()}.parquet`);
    await db.run(
      `COPY export_data TO ${sqlStringLiteral(tmpPath)} (FORMAT PARQUET, COMPRESSION ZSTD);`,
    );
    await db.close();
    db = null;

    // Upload to S3
    const buffer = await fs.readFile(tmpPath);
    const s3 = createS3Client(s3Config);
    if (DEBUG) {
      console.log(
        `[snapshot] Uploading s3://${s3Config.bucket}/${s3Key} via ${s3Config.endpoint}`,
      );
    }
    await uploadToS3(s3, s3Config.bucket, s3Key, buffer);
    if (DEBUG) {
      console.log(
        `[snapshot] Uploaded s3://${s3Config.bucket}/${s3Key} (${buffer.length} bytes)`,
      );
    }

    res.json({
      s3_key: s3Key,
      row_count: data.length,
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

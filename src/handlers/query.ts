import { Request, Response } from "express";
import { createDuckDB, type S3Config } from "../lib/duckdb.js";
import type { Database } from "duckdb-async";

interface TableMapping {
  name: string;
  s3_path: string;
}

interface QueryRequest {
  sql: string;
  tables: TableMapping[];
  s3_config: S3Config;
}

export async function handleQuery(req: Request, res: Response) {
  const body = req.body as QueryRequest;

  if (!body.sql || !body.tables || !body.s3_config) {
    res.status(400).json({ error: "Missing required fields: sql, tables, s3_config" });
    return;
  }

  let db: Database | null = null;

  try {
    db = await createDuckDB(body.s3_config);

    // Create views for each table pointing to S3 Parquet files
    for (const table of body.tables) {
      const s3Uri = `s3://${body.s3_config.bucket}/${table.s3_path}`;
      await db.run(`CREATE VIEW "${table.name}" AS SELECT * FROM read_parquet('${s3Uri}');`);
    }

    // Execute SQL
    const result = await db.all(body.sql);
    await db.close();
    db = null;

    const columns = result.length > 0 ? Object.keys(result[0] as object) : [];
    const rows = result.map((row) => {
      const obj: Record<string, unknown> = {};
      for (const col of columns) {
        const val = (row as Record<string, unknown>)[col];
        obj[col] = typeof val === "bigint" ? Number(val) : val;
      }
      return obj;
    });

    res.json({ columns, rows, row_count: rows.length });
  } catch (error) {
    console.error("[query] Error:", error);
    res.status(500).json({
      error: error instanceof Error ? error.message : String(error),
    });
  } finally {
    if (db) await db.close().catch(() => {});
  }
}

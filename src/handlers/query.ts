import { Request, Response } from "express";
import { createDuckDB } from "../lib/duckdb.js";
import type { Database } from "duckdb-async";
import {
  getQueryQueueStats,
  runWithQueryQueue,
  SidecarQueueSaturatedError,
} from "../lib/query_queue.js";
import { sqlIdentifier, sqlStringLiteral, toJsonValue } from "../lib/sql.js";
import {
  assertArray,
  assertNonEmptyString,
  assertRecord,
  parseS3Config,
} from "../lib/validate.js";

interface TableMapping {
  name: string;
  s3_path: string;
}

interface QueryRequest {
  sql: string;
  tables: TableMapping[];
  s3_config: unknown;
}

export async function handleQuery(req: Request, res: Response) {
  const requestStartedAtMs = Date.now();
  let sql: string;
  let tables: TableMapping[];
  let s3Config: ReturnType<typeof parseS3Config>;
  try {
    const body = assertRecord(req.body, "body") as unknown as QueryRequest;

    sql = assertNonEmptyString(body.sql, "sql");
    s3Config = parseS3Config(body.s3_config);

    const rawTables = assertArray(body.tables, "tables");
    tables = rawTables.map((t, i) => {
      const obj = assertRecord(t, `tables[${i}]`);
      return {
        name: assertNonEmptyString(obj.name, `tables[${i}].name`),
        s3_path: assertNonEmptyString(obj.s3_path, `tables[${i}].s3_path`),
      };
    });
  } catch (error) {
    res.status(400).json({
      error: error instanceof Error ? error.message : String(error),
    });
    return;
  }

  let db: Database | null = null;

  try {
    const { value: payload, queueWaitMs, queueDepthOnEnqueue } = await runWithQueryQueue(
      async () => {
        db = await createDuckDB(s3Config);

        // Create views for each table pointing to S3 Parquet files
        for (const table of tables) {
          const s3Path = table.s3_path.replace(/^\/+/, "");
          const s3Uri = `s3://${s3Config.bucket}/${s3Path}`;
          await db.run(
            `CREATE OR REPLACE VIEW ${sqlIdentifier(table.name)} AS SELECT * FROM read_parquet(${sqlStringLiteral(s3Uri)});`,
          );
        }

        // Execute SQL
        const result = await db.all(sql);
        await db.close();
        db = null;

        const columns = result.length > 0 ? Object.keys(result[0] as object) : [];
        const rows = result.map((row) => {
          const obj: Record<string, unknown> = {};
          for (const col of columns) {
            const val = (row as Record<string, unknown>)[col];
            obj[col] = toJsonValue(val);
          }
          return obj;
        });

        return { columns, rows, row_count: rows.length };
      },
    );

    const durationMs = Date.now() - requestStartedAtMs;
    const queueStats = getQueryQueueStats();
    console.log(
      JSON.stringify({
        event: "sidecar_query",
        status: "ok",
        duration_ms: durationMs,
        queue_wait_ms: queueWaitMs,
        queue_depth_on_enqueue: queueDepthOnEnqueue,
        table_count: tables.length,
        row_count: payload.row_count,
        queue_running: queueStats.running,
        queue_queued: queueStats.queued,
      }),
    );

    res.json(payload);
  } catch (error) {
    const durationMs = Date.now() - requestStartedAtMs;
    if (error instanceof SidecarQueueSaturatedError) {
      const queueStats = getQueryQueueStats();
      res.status(error.statusCode).json({
        error: `${error.message}. Please retry in a few seconds.`,
      });
      console.warn(
        JSON.stringify({
          event: "sidecar_query",
          status: "busy",
          duration_ms: durationMs,
          table_count: tables.length,
          queue_running: queueStats.running,
          queue_queued: queueStats.queued,
        }),
      );
      return;
    }

    console.error("[query] Error:", error);
    console.error(
      JSON.stringify({
        event: "sidecar_query",
        status: "error",
        duration_ms: durationMs,
        table_count: tables.length,
        error: error instanceof Error ? error.message : String(error),
      }),
    );
    res.status(500).json({
      error: error instanceof Error ? error.message : String(error),
    });
  } finally {
    if (db !== null) {
      await (db as Database).close().catch(() => {});
    }
  }
}

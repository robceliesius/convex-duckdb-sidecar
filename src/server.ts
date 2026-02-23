import "./otel.js";
import express from "express";
import type { NextFunction, Request, Response } from "express";
import { handleSnapshot } from "./handlers/snapshot.js";
import { handleQuery } from "./handlers/query.js";
import { initPool, drainPool, getPoolStats } from "./lib/pool.js";

const app = express();
const PORT = Number.parseInt(process.env.PORT ?? "3214", 10);
if (!Number.isFinite(PORT)) {
  throw new Error(`Invalid PORT: ${process.env.PORT}`);
}

app.disable("x-powered-by");

// 50MB limit for large data payloads
app.use(express.json({ limit: "50mb" }));

// Return a clean 400 if JSON parsing fails.
app.use((err: unknown, _req: Request, res: Response, next: NextFunction) => {
  const anyErr = err as any;
  if (anyErr?.type === "entity.parse.failed") {
    res.status(400).json({ error: "Invalid JSON" });
    return;
  }
  next(err);
});

app.get("/health", (_req, res) => {
  res.json({ status: "ok", service: "duckdb-sidecar" });
});

app.get("/pool-stats", (_req, res) => {
  const stats = getPoolStats();
  if (!stats) {
    res.json({ enabled: false });
    return;
  }
  res.json({ enabled: true, ...stats });
});

app.post("/snapshot", handleSnapshot);
app.post("/query", handleQuery);

async function start() {
  const poolEnabled = await initPool();
  console.log(
    `[duckdb-sidecar] Pool: ${poolEnabled ? "enabled" : "disabled (per-request instances)"}`,
  );

  const server = app.listen(PORT, "0.0.0.0", () => {
    console.log(`[duckdb-sidecar] Listening on port ${PORT}`);
  });

  // Graceful shutdown: drain pool on SIGTERM
  const shutdown = async () => {
    console.log("[duckdb-sidecar] Shutting down...");
    server.close();
    await drainPool();
    process.exit(0);
  };
  process.on("SIGTERM", shutdown);
  process.on("SIGINT", shutdown);
}

start().catch((err) => {
  console.error("[duckdb-sidecar] Failed to start:", err);
  process.exit(1);
});

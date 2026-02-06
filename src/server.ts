import express from "express";
import type { NextFunction, Request, Response } from "express";
import { handleSnapshot } from "./handlers/snapshot.js";
import { handleQuery } from "./handlers/query.js";

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

app.post("/snapshot", handleSnapshot);
app.post("/query", handleQuery);

app.listen(PORT, "0.0.0.0", () => {
  console.log(`[duckdb-sidecar] Listening on port ${PORT}`);
});

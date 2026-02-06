import express from "express";
import { handleSnapshot } from "./handlers/snapshot.js";
import { handleQuery } from "./handlers/query.js";

const app = express();
const PORT = parseInt(process.env.PORT ?? "3214", 10);

// 50MB limit for large data payloads
app.use(express.json({ limit: "50mb" }));

app.get("/health", (_req, res) => {
  res.json({ status: "ok", service: "duckdb-sidecar" });
});

app.post("/snapshot", handleSnapshot);
app.post("/query", handleQuery);

app.listen(PORT, "0.0.0.0", () => {
  console.log(`[duckdb-sidecar] Listening on port ${PORT}`);
});

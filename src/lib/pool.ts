import { Database } from "duckdb-async";
import { createDuckDB, getS3ConfigFromEnv, type S3Config } from "./duckdb.js";

function parsePositiveIntEnv(name: string, fallback: number): number {
  const raw = process.env[name];
  if (!raw) return fallback;
  const parsed = Number.parseInt(raw, 10);
  if (!Number.isFinite(parsed) || parsed <= 0) return fallback;
  return parsed;
}

interface PooledInstance {
  db: Database;
  createdAt: number;
}

export class DuckDBPool {
  private idle: PooledInstance[] = [];
  private activeCount = 0;
  private creationTimes = new Map<Database, number>();
  private minSize: number;
  private maxSize: number;
  private recycleTTLMs: number;
  private s3Config: S3Config;
  private totalCreated = 0;
  private totalRecycled = 0;
  private closed = false;

  constructor(opts: {
    minSize: number;
    maxSize: number;
    recycleTTLMs: number;
    s3Config: S3Config;
  }) {
    this.minSize = opts.minSize;
    this.maxSize = opts.maxSize;
    this.recycleTTLMs = opts.recycleTTLMs;
    this.s3Config = opts.s3Config;
  }

  async warmup(): Promise<void> {
    const promises: Promise<void>[] = [];
    for (let i = 0; i < this.minSize; i++) {
      promises.push(
        this.createInstance().then((inst) => {
          this.idle.push(inst);
        }),
      );
    }
    await Promise.all(promises);
    console.log(`[pool] Warmed up ${this.idle.length} DuckDB instances`);
  }

  private async createInstance(): Promise<PooledInstance> {
    const db = await createDuckDB(this.s3Config);
    this.totalCreated++;
    return { db, createdAt: Date.now() };
  }

  private isExpired(inst: PooledInstance): boolean {
    return Date.now() - inst.createdAt > this.recycleTTLMs;
  }

  async acquire(): Promise<Database> {
    if (this.closed) throw new Error("Pool is closed");

    while (this.idle.length > 0) {
      const inst = this.idle.pop()!;
      if (this.isExpired(inst)) {
        this.totalRecycled++;
        inst.db.close().catch(() => {});
        continue;
      }
      this.activeCount++;
      this.creationTimes.set(inst.db, inst.createdAt);
      return inst.db;
    }

    if (this.activeCount < this.maxSize) {
      const inst = await this.createInstance();
      this.activeCount++;
      this.creationTimes.set(inst.db, inst.createdAt);
      return inst.db;
    }

    throw new Error(
      `Pool exhausted (active: ${this.activeCount}, max: ${this.maxSize})`,
    );
  }

  release(db: Database, healthy: boolean): void {
    this.activeCount--;
    const createdAt = this.creationTimes.get(db) ?? Date.now();
    this.creationTimes.delete(db);

    if (this.closed || !healthy) {
      if (!healthy) this.totalRecycled++;
      db.close().catch(() => {});
      return;
    }

    this.idle.push({ db, createdAt });

    // Trim idle pool to minSize
    while (this.idle.length > this.minSize) {
      const excess = this.idle.shift()!;
      this.totalRecycled++;
      excess.db.close().catch(() => {});
    }
  }

  getStats() {
    return {
      idle: this.idle.length,
      active: this.activeCount,
      total: this.idle.length + this.activeCount,
      minSize: this.minSize,
      maxSize: this.maxSize,
      totalCreated: this.totalCreated,
      totalRecycled: this.totalRecycled,
    };
  }

  async drain(): Promise<void> {
    this.closed = true;
    const promises = this.idle.map((inst) => inst.db.close().catch(() => {}));
    this.idle = [];
    await Promise.all(promises);
    console.log("[pool] Drained all idle DuckDB instances");
  }
}

// Module-level singleton
let pool: DuckDBPool | null = null;

export function getPool(): DuckDBPool | null {
  return pool;
}

export function getPoolStats(): ReturnType<DuckDBPool["getStats"]> | null {
  return pool?.getStats() ?? null;
}

export async function initPool(): Promise<boolean> {
  let s3Config: S3Config;
  try {
    s3Config = getS3ConfigFromEnv();
  } catch {
    console.log(
      "[pool] Disabled — set S3_ENDPOINT, S3_BUCKET, S3_ACCESS_KEY_ID, S3_SECRET_ACCESS_KEY to enable",
    );
    return false;
  }

  const minSize = parsePositiveIntEnv("DUCKDB_POOL_SIZE", 4);
  const maxSize = parsePositiveIntEnv("DUCKDB_POOL_MAX", 12);
  const recycleTTLMs = parsePositiveIntEnv("DUCKDB_POOL_RECYCLE_MS", 900_000);

  pool = new DuckDBPool({ minSize, maxSize, recycleTTLMs, s3Config });
  await pool.warmup();
  return true;
}

export async function drainPool(): Promise<void> {
  if (pool) {
    await pool.drain();
    pool = null;
  }
}

type QueueTask<T> = () => Promise<T>;

type QueueItem<T> = {
  task: QueueTask<T>;
  resolve: (value: QueueRunResult<T>) => void;
  reject: (error: unknown) => void;
  enqueuedAtMs: number;
};

export type QueueRunResult<T> = {
  value: T;
  queueWaitMs: number;
  queueDepthOnEnqueue: number;
};

function parsePositiveIntEnv(name: string, fallback: number): number {
  const raw = process.env[name];
  if (!raw) return fallback;
  const parsed = Number.parseInt(raw, 10);
  if (!Number.isFinite(parsed) || parsed <= 0) return fallback;
  return parsed;
}

const MAX_CONCURRENT = parsePositiveIntEnv("SIDECAR_MAX_CONCURRENT_QUERIES", 12);
const MAX_QUEUE = parsePositiveIntEnv("SIDECAR_MAX_QUERY_QUEUE", 200);

let running = 0;
const queue: Array<QueueItem<any>> = [];

export class SidecarQueueSaturatedError extends Error {
  statusCode: number;

  constructor(message: string) {
    super(message);
    this.name = "SidecarQueueSaturatedError";
    this.statusCode = 503;
  }
}

function pumpQueue(): void {
  while (running < MAX_CONCURRENT && queue.length > 0) {
    const item = queue.shift()!;
    const queueWaitMs = Math.max(0, Date.now() - item.enqueuedAtMs);
    running += 1;

    void item.task()
      .then((value) => {
        item.resolve({
          value,
          queueWaitMs,
          queueDepthOnEnqueue: 0,
        });
      })
      .catch((error) => item.reject(error))
      .finally(() => {
        running -= 1;
        pumpQueue();
      });
  }
}

export async function runWithQueryQueue<T>(task: QueueTask<T>): Promise<QueueRunResult<T>> {
  if (running < MAX_CONCURRENT) {
    running += 1;
    try {
      const value = await task();
      return {
        value,
        queueWaitMs: 0,
        queueDepthOnEnqueue: 0,
      };
    } finally {
      // Keep this in finally so queue drains even if task throws.
      running -= 1;
      pumpQueue();
    }
  }

  if (queue.length >= MAX_QUEUE) {
    throw new SidecarQueueSaturatedError(
      `sidecar is busy (queue full: ${queue.length}/${MAX_QUEUE}); retry shortly`,
    );
  }

  return await new Promise<QueueRunResult<T>>((resolve, reject) => {
    const queueDepthOnEnqueue = queue.length;
    queue.push({
      task: task as QueueTask<any>,
      enqueuedAtMs: Date.now(),
      resolve: (result) => {
        resolve({
          value: result.value as T,
          queueWaitMs: result.queueWaitMs,
          queueDepthOnEnqueue,
        });
      },
      reject,
    });
    pumpQueue();
  });
}

export function getQueryQueueStats(): {
  maxConcurrent: number;
  maxQueue: number;
  running: number;
  queued: number;
} {
  return {
    maxConcurrent: MAX_CONCURRENT,
    maxQueue: MAX_QUEUE,
    running,
    queued: queue.length,
  };
}

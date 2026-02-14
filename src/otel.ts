import { diag, DiagConsoleLogger, DiagLogLevel } from '@opentelemetry/api';
import { getNodeAutoInstrumentations } from '@opentelemetry/auto-instrumentations-node';
import type { ExportResult } from '@opentelemetry/core';
import { OTLPTraceExporter } from '@opentelemetry/exporter-trace-otlp-http';
import { resourceFromAttributes } from '@opentelemetry/resources';
import type { ReadableSpan, SpanExporter } from '@opentelemetry/sdk-trace-base';
import { ParentBasedSampler, TraceIdRatioBasedSampler } from '@opentelemetry/sdk-trace-base';
import { NodeSDK } from '@opentelemetry/sdk-node';
import { SemanticResourceAttributes } from '@opentelemetry/semantic-conventions';

function sanitizeString(input: string): string {
  let out = input;

  // Generic JWT pattern (base64url.header.base64url.payload.base64url.signature).
  out = out.replace(/[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+/g, '<jwt>');

  // Email addresses (PII).
  out = out.replace(/[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}/gi, '<email>');

  // Opaque IDs (hex, base64url-ish, long random strings).
  out = out.replace(/[a-f0-9]{32,}/gi, '<id>');
  out = out.replace(/[A-Za-z0-9_-]{40,}/g, '<id>');

  // Drop query strings.
  out = out.replace(/\?.*$/, '?<redacted>');

  return out;
}

function sanitizeSpan(span: ReadableSpan): void {
  const anySpan = span as any;

  if (typeof anySpan?.name === 'string') {
    anySpan.name = sanitizeString(anySpan.name);
  }

  const attrs: Record<string, unknown> | undefined = anySpan?.attributes;
  if (!attrs) return;

  for (const [k, v] of Object.entries(attrs)) {
    if (typeof v === 'string') {
      attrs[k] = sanitizeString(v);
      continue;
    }

    // Proactively remove high-risk attribute keys if they appear.
    const lk = String(k).toLowerCase();
    if (
      lk.includes('authorization') ||
      lk.includes('cookie') ||
      lk.includes('set-cookie') ||
      lk.includes('x-api-key') ||
      lk.includes('api_key') ||
      lk.includes('apikey')
    ) {
      delete attrs[k];
    }
  }
}

class RedactingSpanExporter implements SpanExporter {
  constructor(private readonly inner: SpanExporter) {}

  export(spans: ReadableSpan[], resultCallback: (result: ExportResult) => void): void {
    for (const span of spans) {
      try {
        sanitizeSpan(span);
      } catch {
        // Best-effort redaction: never block exports.
      }
    }
    this.inner.export(spans, resultCallback);
  }

  async shutdown(): Promise<void> {
    await this.inner.shutdown();
  }

  async forceFlush(): Promise<void> {
    if (typeof (this.inner as any).forceFlush === 'function') {
      await (this.inner as any).forceFlush();
    }
  }
}

const otlpBase =
  process.env.OTEL_EXPORTER_OTLP_ENDPOINT?.trim() ||
  process.env.OTEL_EXPORTER_OTLP_TRACES_ENDPOINT?.trim() ||
  '';

const enableTracing =
  (process.env.OTEL_TRACING_ENABLED ?? '').toLowerCase() === 'true' ||
  (process.env.NODE_ENV === 'production' && otlpBase.length > 0);

if (!enableTracing) {
  // eslint-disable-next-line no-console
  console.log('[otel] tracing disabled (set OTEL_EXPORTER_OTLP_ENDPOINT or OTEL_TRACING_ENABLED=true)');
} else {
  if ((process.env.OTEL_LOG_LEVEL ?? '').toUpperCase() === 'DEBUG') {
    diag.setLogger(new DiagConsoleLogger(), DiagLogLevel.DEBUG);
  }

  const base = otlpBase.replace(/\/$/, '');
  const tracesEndpoint =
    process.env.OTEL_EXPORTER_OTLP_TRACES_ENDPOINT?.trim() || `${base}/v1/traces`;

  const ratioRaw = process.env.OTEL_TRACES_SAMPLER_ARG?.trim();
  const ratio = ratioRaw ? Number(ratioRaw) : 0.2;
  const sampleRatio = Number.isFinite(ratio) ? Math.min(1, Math.max(0, ratio)) : 0.2;

  const serviceName = process.env.OTEL_SERVICE_NAME?.trim() || 'bulk_duckdb_sidecar';
  const environment =
    process.env.DEPLOYMENT_ENVIRONMENT?.trim() || process.env.NODE_ENV || 'production';

  const sdk = new NodeSDK({
    resource: resourceFromAttributes({
      [SemanticResourceAttributes.SERVICE_NAME]: serviceName,
      [SemanticResourceAttributes.SERVICE_NAMESPACE]: 'bulk',
      [SemanticResourceAttributes.DEPLOYMENT_ENVIRONMENT]: environment,
    }),
    sampler: new ParentBasedSampler({
      root: new TraceIdRatioBasedSampler(sampleRatio),
    }),
    traceExporter: new RedactingSpanExporter(new OTLPTraceExporter({ url: tracesEndpoint })),
    instrumentations: [getNodeAutoInstrumentations()],
  });

  await sdk.start();
  // eslint-disable-next-line no-console
  console.log('[otel] tracing enabled', { serviceName, sampleRatio, tracesEndpoint });

  const shutdown = async (signal: string) => {
    // eslint-disable-next-line no-console
    console.log(`[otel] shutting down (${signal})`);
    try {
      await sdk.shutdown();
    } catch (err) {
      // eslint-disable-next-line no-console
      console.error('[otel] shutdown error', err);
    }
  };

  process.once('SIGTERM', () => void shutdown('SIGTERM'));
  process.once('SIGINT', () => void shutdown('SIGINT'));
}


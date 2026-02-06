import type { S3Config } from "./duckdb.js";

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

export function assertNonEmptyString(value: unknown, name: string): string {
  if (typeof value !== "string" || !value.trim()) {
    throw new Error(`${name} must be a non-empty string`);
  }
  return value;
}

export function assertArray(value: unknown, name: string): unknown[] {
  if (!Array.isArray(value)) {
    throw new Error(`${name} must be an array`);
  }
  return value;
}

export function assertRecord(
  value: unknown,
  name: string,
): Record<string, unknown> {
  if (!isRecord(value)) {
    throw new Error(`${name} must be an object`);
  }
  return value;
}

export function parseS3Config(raw: unknown): S3Config {
  const obj = assertRecord(raw, "s3_config");

  const endpoint = assertNonEmptyString(obj.endpoint, "s3_config.endpoint");
  let endpointUrl: URL;
  try {
    endpointUrl = new URL(endpoint);
  } catch {
    throw new Error(
      's3_config.endpoint must be a valid URL (e.g. "http://minio:9000")',
    );
  }
  if (endpointUrl.protocol !== "http:" && endpointUrl.protocol !== "https:") {
    throw new Error(
      `s3_config.endpoint must be http(s), got: ${endpointUrl.protocol}`,
    );
  }
  if (endpointUrl.pathname !== "/" && endpointUrl.pathname !== "") {
    throw new Error(
      `s3_config.endpoint must not include a path: ${endpoint}`,
    );
  }

  const bucket = assertNonEmptyString(obj.bucket, "s3_config.bucket");
  const accessKeyId = assertNonEmptyString(
    obj.accessKeyId,
    "s3_config.accessKeyId",
  );
  const secretAccessKey = assertNonEmptyString(
    obj.secretAccessKey,
    "s3_config.secretAccessKey",
  );

  const region =
    obj.region === undefined ? undefined : assertNonEmptyString(obj.region, "s3_config.region");
  const forcePathStyle =
    obj.forcePathStyle === undefined
      ? undefined
      : (() => {
          if (typeof obj.forcePathStyle !== "boolean") {
            throw new Error("s3_config.forcePathStyle must be a boolean");
          }
          return obj.forcePathStyle;
        })();

  return {
    endpoint,
    bucket,
    region,
    accessKeyId,
    secretAccessKey,
    forcePathStyle,
  };
}

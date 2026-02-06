import { S3Client, PutObjectCommand } from "@aws-sdk/client-s3";
import type { S3Config } from "./duckdb.js";

export function createS3Client(config: S3Config): S3Client {
  return new S3Client({
    endpoint: config.endpoint,
    region: config.region ?? "us-east-1",
    credentials: {
      accessKeyId: config.accessKeyId,
      secretAccessKey: config.secretAccessKey,
    },
    forcePathStyle: config.forcePathStyle ?? true,
  });
}

export async function uploadToS3(
  client: S3Client,
  bucket: string,
  key: string,
  body: Buffer,
): Promise<void> {
  await client.send(
    new PutObjectCommand({
      Bucket: bucket,
      Key: key,
      Body: body,
      ContentType: "application/octet-stream",
    }),
  );
}

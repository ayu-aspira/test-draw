import type { Readable } from "node:stream";
import { GetObjectCommand, PutObjectCommand, S3Client } from "@aws-sdk/client-s3";
import { Upload } from "@aws-sdk/lib-storage";
import { getSignedUrl } from "@aws-sdk/s3-request-presigner";
import { parse } from "csv-parse";
import { Bucket } from "sst/node/bucket";

export const S3_CLIENT = new S3Client({});

const PRESIGNED_URL_EXPIRATION_SECONDS = 60;

/**
 * Function to lazy load the bucket name to avoid
 * SST global binding issues.
 */
export const getDrawDocumentBucketName = () => Bucket.DrawDocumentBucket.bucketName;

export const writeToS3 = async (input: { s3Key: string; contentType: string; bytes: Buffer }): Promise<void> => {
  const { s3Key, contentType, bytes } = input;

  try {
    await S3_CLIENT.send(
      new PutObjectCommand({
        Bucket: getDrawDocumentBucketName(),
        Key: s3Key,
        Body: bytes,
        ContentType: contentType,
      }),
    );
  } catch (err) {
    throw new Error("Failed to write asset to S3.", { cause: err });
  }
};

export const createS3ReadStream = async (s3Key: string): Promise<Readable> => {
  const result = await S3_CLIENT.send(
    new GetObjectCommand({
      Bucket: getDrawDocumentBucketName(),
      Key: s3Key,
    }),
  );

  if (!result.Body) {
    throw new Error(`Failed to read from S3: ${s3Key}`);
  }

  return result.Body as Readable;
};

export const readAllFromS3 = async (s3Key: string): Promise<Buffer> => {
  const rs = await createS3ReadStream(s3Key);
  const chunks = [];
  for await (const chunk of rs) {
    chunks.push(chunk);
  }
  return Buffer.concat(chunks);
};

export const createS3UploadStream = (input: {
  s3Key: string;
  contentType: string;
  readStream: Readable;
}): Upload => {
  const { s3Key, contentType, readStream } = input;

  return new Upload({
    client: S3_CLIENT,
    params: {
      Bucket: getDrawDocumentBucketName(),
      Key: s3Key,
      Body: readStream,
      ContentType: contentType,
    },
  });
};

export const readCSVFromS3 = async (s3Key: string): Promise<Record<string, string>[]> => {
  const result: Record<string, string>[] = [];

  const { Body } = await S3_CLIENT.send(
    new GetObjectCommand({
      Bucket: getDrawDocumentBucketName(),
      Key: s3Key,
    }),
  );

  if (!Body) {
    throw new Error("Invalid response from S3");
  }

  const parser = (Body as Readable).pipe(parse({ columns: true, skip_empty_lines: true, trim: true }));

  for await (const data of parser) {
    result.push(data as Record<string, string>);
  }

  return result;
};

export const generatePresignedUploadUrl = async (s3Key: string, contentType: string): Promise<string> => {
  const putCommand = new PutObjectCommand({
    Bucket: getDrawDocumentBucketName(),
    Key: s3Key,
    ContentType: contentType,
  });

  return await getSignedUrl(S3_CLIENT, putCommand, { expiresIn: PRESIGNED_URL_EXPIRATION_SECONDS });
};

export const generatePresignedDownloadUrl = async (s3Key: string): Promise<string> => {
  const getCommand = new GetObjectCommand({
    Bucket: getDrawDocumentBucketName(),
    Key: s3Key,
  });

  return await getSignedUrl(S3_CLIENT, getCommand, { expiresIn: PRESIGNED_URL_EXPIRATION_SECONDS });
};

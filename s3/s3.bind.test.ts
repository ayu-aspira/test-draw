import { createS3ReadStream, createS3UploadStream, readAllFromS3, readCSVFromS3, writeToS3 } from "@/s3/s3";
import { deleteAllS3TestDataByOrg } from "@/util/test-utils";
import { ulid } from "ulidx";
import { afterEach, describe, expect, it } from "vitest";

const ORG_ID = `org_${ulid()}`;

const CSV_TEST_DATA = "col1,col2,col3\n1,2,3\n4,5,6\n7,8,9\n";
const JSON_TEST_DATA = JSON.stringify([
  { col1: 1, col2: 2, col3: 3 },
  { col1: 4, col2: 5, col3: 6 },
  { col1: 7, col2: 8, col3: 9 },
]);

const writeTestDataToS3 = async (type: "csv" | "json"): Promise<string> => {
  const s3Key = `${ORG_ID}/test.${type}`;
  const contentType = type === "csv" ? "text/csv" : "application/json";
  const testData = type === "csv" ? CSV_TEST_DATA : JSON_TEST_DATA;
  const bytes = Buffer.from(testData);
  await writeToS3({ s3Key, contentType, bytes });
  return s3Key;
};

describe("S3 Utilities Tests", () => {
  afterEach(async () => {
    await deleteAllS3TestDataByOrg(ORG_ID);
  });

  it("Should write to S3", async () => {
    const s3Key = await writeTestDataToS3("csv");
    const data = await readAllFromS3(s3Key);
    expect(data.toString()).toBe(CSV_TEST_DATA);
  });

  it("Should create an S3 read stream", async () => {
    const s3Key = await writeTestDataToS3("json");
    const rs = await createS3ReadStream(s3Key);
    expect(rs).toBeDefined();
    const chunks = [];
    for await (const chunk of rs) {
      chunks.push(chunk);
    }
    expect(Buffer.concat(chunks).toString()).toBe(JSON_TEST_DATA);
  });

  it("Should read all from S3", async () => {
    const s3Key = await writeTestDataToS3("json");
    const data = await readAllFromS3(s3Key);
    expect(data.toString()).toBe(JSON_TEST_DATA);
  });

  it("Should create an S3 upload stream", async () => {
    const s3Key = await writeTestDataToS3("json");
    const rs = await createS3ReadStream(s3Key);

    const testUploadStreamS3Key = `${ORG_ID}/test-stream.json`;

    const upload = createS3UploadStream({
      s3Key: testUploadStreamS3Key,
      contentType: "application/json",
      readStream: rs,
    });

    expect(upload).toBeDefined();
    await upload.done();

    const data = await readAllFromS3(testUploadStreamS3Key);
    expect(data.toString()).toBe(JSON_TEST_DATA);
  });

  it("Should read a CSV from S3", async () => {
    const s3Key = await writeTestDataToS3("csv");
    const data = await readCSVFromS3(s3Key);
    expect(data).toEqual([
      { col1: "1", col2: "2", col3: "3" },
      { col1: "4", col2: "5", col3: "6" },
      { col1: "7", col2: "8", col3: "9" },
    ]);
  });
});

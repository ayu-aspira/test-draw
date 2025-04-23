import { Readable } from "node:stream";
import { S3_CLIENT, readCSVFromS3 } from "@/s3/s3";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

vi.mock("sst/node/bucket", () => ({
  Bucket: {
    DrawDocumentBucket: {
      bucketName: "draw-document-bucket",
    },
  },
}));

vi.mock("@aws-sdk/client-s3", () => ({
  S3Client: vi.fn(),
  GetObjectCommand: vi.fn(),
}));

describe("readCSVFromS3", () => {
  let sendMock = vi.fn();

  beforeEach(() => {
    sendMock = vi.fn();
    S3_CLIENT.send = sendMock;
  });

  afterEach(() => {
    vi.clearAllMocks();
  });

  it("should read a CSV file from S3 and parse it", async () => {
    const csvContent = "column1,column2\nvalue1,value2\n";
    const mockReadableStream = new Readable();
    mockReadableStream.push(csvContent);
    mockReadableStream.push(null);

    sendMock.mockResolvedValue({ Body: mockReadableStream });

    const expectedResult = [{ column1: "value1", column2: "value2" }];

    const result = await readCSVFromS3("someKey");

    expect(result).toEqual(expectedResult);
  });

  it("should correctly parse CSV with quotes and spaces", async () => {
    const csvContent = 'column1,column2\n"value1" , "value2" \n';
    const mockReadableStream = new Readable();
    mockReadableStream.push(csvContent);
    mockReadableStream.push(null);

    sendMock.mockResolvedValue({ Body: mockReadableStream });

    const expectedResult = [{ column1: "value1", column2: "value2" }];

    const result = await readCSVFromS3("someKey");

    expect(result).toEqual(expectedResult);
  });

  it("should handle an empty CSV file", async () => {
    const mockReadableStream = new Readable();
    mockReadableStream.push(""); // Empty content
    mockReadableStream.push(null);

    sendMock.mockResolvedValue({ Body: mockReadableStream });

    const result = await readCSVFromS3("someKey");

    expect(result).toEqual([]); // Expect empty array for empty CSV
  });

  it("should correctly parse CSV with Windows-style line endings", async () => {
    const csvContent = "column1,column2\r\nvalue1,value2\r\n";
    const mockReadableStream = new Readable();
    mockReadableStream.push(csvContent);
    mockReadableStream.push(null);

    sendMock.mockResolvedValue({ Body: mockReadableStream });

    const result = await readCSVFromS3("someKey");

    expect(result).toEqual([{ column1: "value1", column2: "value2" }]);
  });

  it("should correctly parse a large CSV file", async () => {
    const count = Math.floor(Math.random() * 100_000) + 100_000;
    const csvContent = Buffer.concat([
      Buffer.from("column1,column2\n"),
      ...Array(count).fill(Buffer.from("value1,value2\n")),
    ]).toString();
    const mockReadableStream = new Readable();
    mockReadableStream.push(csvContent);
    mockReadableStream.push(null);

    sendMock.mockResolvedValue({ Body: mockReadableStream });

    const result = await readCSVFromS3("someKey");

    expect(result.length).toBe(count);
  });

  it("should reject a file with missing CSV data", async () => {
    const csvContent = "column1,column2\nvalue1"; // Missing second column
    const mockReadableStream = new Readable();
    mockReadableStream.push(csvContent);
    mockReadableStream.push(null);

    sendMock.mockResolvedValue({ Body: mockReadableStream });

    await expect(readCSVFromS3("someKey")).rejects.toThrow();
  });

  it("should reject a file with extra columns in CSV", async () => {
    const csvContent = "column1,column2,column3\nvalue1,value2,value3,value4\n";
    const mockReadableStream = new Readable();
    mockReadableStream.push(csvContent);
    mockReadableStream.push(null);

    sendMock.mockResolvedValue({ Body: mockReadableStream });

    await expect(readCSVFromS3("someKey")).rejects.toThrow();
  });

  it("should throw an error if S3 response has no Body", async () => {
    sendMock.mockResolvedValue({ Body: null });

    await expect(readCSVFromS3("someKey")).rejects.toThrow("Invalid response from S3");
  });

  it("should handle S3 errors gracefully", async () => {
    sendMock.mockRejectedValue(new Error("S3 failure"));

    await expect(readCSVFromS3("someKey")).rejects.toThrow("S3 failure");
  });
});

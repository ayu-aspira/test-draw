import { DynamoDBPrefix } from "@/dynamo/dynamo";
import { getOrganizationAndDocumentIdsFromKey, isKeyFormatValid } from "@/s3/draw-document";
import { afterEach, describe, expect, it, vi } from "vitest";

// Mock the DynamoDB client to avoid sst bind issues.
vi.mock("@aspira-nextgen/core/dynamodb", async () => {
  const actual = await vi.importActual("@aspira-nextgen/core/dynamodb");
  return {
    ...actual,
    createDynamoDBClient: vi.fn(),
  };
});

vi.mock("sst/node/bucket", () => ({
  Bucket: {
    DrawDocumentBucket: {
      bucketName: "draw-document-bucket",
    },
  },
}));

vi.mock("@aws-sdk/client-s3", async () => {
  const actualModule = await vi.importActual("@aws-sdk/client-s3");

  return {
    ...actualModule,
    S3Client: vi.fn(() => ({
      send: vi.fn().mockImplementation((command) => {
        if (command.constructor.name === "HeadObjectCommand") {
          return { ContentLength: 1024 * 1024 * 5 }; // Mock file size (5 MB)
        }

        if (command.constructor.name === "SelectObjectContentCommand") {
          return {
            Payload: [
              {
                Records: {
                  Payload: Buffer.from(
                    `{"Start":${command.input.ScanRange?.Start}, "End":${command.input.ScanRange?.End}}\n`,
                  ),
                },
              },
            ],
          };
        }

        throw new Error("Unexpected command");
      }),
    })),
  };
});

describe("Draw-Document S3 Unit Tests", () => {
  afterEach(() => {
    vi.clearAllMocks();
  });

  it("Should validate an S3 key", () => {
    expect(isKeyFormatValid(`org_123/${DynamoDBPrefix.DRAW_DOCUMENT}_456/file.txt`)).toBe(true);
    expect(isKeyFormatValid("file.txt")).toBe(false);
  });

  it("Should throw if the key is invalid.", () => {
    expect(() => getOrganizationAndDocumentIdsFromKey("file.txt")).toThrowError("Invalid S3 key format: file.txt");
  });

  it("Should parse the organization and document ID from the s3 key.", () => {
    const { organizationId, documentId } = getOrganizationAndDocumentIdsFromKey(
      `org_123/${DynamoDBPrefix.DRAW_DOCUMENT}_456/file.txt`,
    );
    expect(organizationId).toBe("org_123");
    expect(documentId).toBe(`${DynamoDBPrefix.DRAW_DOCUMENT}_456`);
  });
});

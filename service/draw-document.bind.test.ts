import { getDrawDocumentEntities, listDrawDocumentEntitiesByWorkflowNodeIds } from "@/dynamo/draw-document";
import { DynamoDBPrefix, DynamoDBTypename, dynamoDBClient } from "@/dynamo/dynamo";
import {
  deleteDrawDocument,
  getDrawDocument,
  prepareDrawDocumentDownload,
  prepareDrawDocumentUpload,
} from "@/service/draw-document";
import { createTestDrawDocument, createTestWorkflowNode } from "@/util/test-utils";
import { buildTestToken } from "@aspira-nextgen/core/authn-test-utils";
import { deleteAllTableItemsByPk } from "@aspira-nextgen/core/dynamodb-test-utils";
import { DrawDocumentProcessingStatus, DrawDocumentType, type WorkflowNode } from "@aspira-nextgen/graphql/resolvers";
import { Table } from "sst/node/table";
import { ulid } from "ulidx";
import { afterEach, describe, expect, it, vi } from "vitest";

// Mocking out S3Client and getSignedUrl as we won't be consuming the pre-signed URLs.
vi.mock("@aws-sdk/s3-request-presigner", () => ({
  getSignedUrl: vi.fn().mockReturnValue("https://example.com"),
}));

vi.mock("@aws-sdk/client-s3", async () => {
  const actual = await vi.importActual("@aws-sdk/client-s3");

  return {
    ...actual,
    S3Client: vi.fn().mockImplementation(() => ({
      send: vi.fn(),
    })),
  };
});

const ORG_ID = `org_${ulid()}`;
const TOKEN = buildTestToken(ORG_ID);

const invokePrepareDrawDocumentUpload = async (input: {
  filename: string;
  name: string;
  type: DrawDocumentType;
  contentType: string;
  workflowNode?: WorkflowNode;
}) => {
  const workflowNode = input.workflowNode ?? (await createTestWorkflowNode(TOKEN));
  const drawDocumentUpload = await prepareDrawDocumentUpload(
    {
      filename: input.filename,
      name: input.name,
      type: input.type,
      contentType: input.contentType,
      workflowNodeId: workflowNode.id,
    },
    TOKEN,
  );
  return {
    workflowNode,
    drawDocumentUpload,
  };
};

describe("Draw-Document Tests", () => {
  afterEach(async () => await deleteAllTableItemsByPk(dynamoDBClient, Table.Draw.tableName, [ORG_ID]));

  it("Should generate a Draw Document entity and return a pre-signed PUT S3 URL", async () => {
    const {
      workflowNode,
      drawDocumentUpload: { url, document },
    } = await invokePrepareDrawDocumentUpload({
      filename: "test.csv",
      name: "test",
      type: DrawDocumentType.HuntCodes,
      contentType: "text/csv",
    });

    expect(url).toEqual("https://example.com");

    const drawDocumentIdPattern = `^${DynamoDBPrefix.DRAW_DOCUMENT}_[a-zA-Z0-9]+$`;

    expect(document).toMatchObject({
      id: expect.stringMatching(drawDocumentIdPattern),
      name: "test",
      type: DrawDocumentType.HuntCodes,
      processingStatus: DrawDocumentProcessingStatus.UrlGenerated,
      __typename: DynamoDBTypename.DRAW_DOCUMENT,
    });

    const [drawDocumentEntity] = await getDrawDocumentEntities([document.id], ORG_ID);

    expect(drawDocumentEntity).toMatchObject({
      pk: ORG_ID,
      sk: expect.stringMatching(drawDocumentIdPattern),
      id: expect.stringMatching(drawDocumentIdPattern),
      s3Key: `${ORG_ID}/${drawDocumentEntity.id}/test.csv`,
      filename: "test.csv",
      contentType: "text/csv",
      processingStatus: DrawDocumentProcessingStatus.UrlGenerated,
    });

    const [nodeLinkedDrawDocument] = await listDrawDocumentEntitiesByWorkflowNodeIds({
      organizationId: ORG_ID,
      workflowNodeIds: [workflowNode.id],
    });

    expect(nodeLinkedDrawDocument).toEqual({ ...drawDocumentEntity, workflowNodeId: workflowNode.id });
  });

  it("Should only allow CSV files to be uploaded.", async () => {
    await expect(
      invokePrepareDrawDocumentUpload({
        filename: "test.csv",
        name: "test",
        type: DrawDocumentType.HuntCodes,
        contentType: "text/plain",
      }),
    ).rejects.toThrowError("Only CSV files are supported.");
  });

  it("Should generate a pre-signed URL for a draw-document entity.", async () => {
    const {
      drawDocumentUpload: { document: uploadDoc },
    } = await invokePrepareDrawDocumentUpload({
      filename: "test.csv",
      name: "test",
      type: DrawDocumentType.HuntCodes,
      contentType: "text/csv",
    });

    const { url, document: downloadDoc } = await prepareDrawDocumentDownload(uploadDoc.id, TOKEN);

    expect(url).toEqual("https://example.com");

    expect(downloadDoc).toMatchObject({
      id: uploadDoc.id,
      name: "test",
      type: DrawDocumentType.HuntCodes,
      processingStatus: DrawDocumentProcessingStatus.UrlGenerated,
      __typename: DynamoDBTypename.DRAW_DOCUMENT,
    });
  });

  it("Should throw an error if the document does not exist upon download.", async () => {
    await expect(prepareDrawDocumentDownload("non-existent", TOKEN)).rejects.toThrowError("Document not found.");
  });

  it("Should get a draw document.", async () => {
    const {
      drawDocumentUpload: { document: uploadDoc },
    } = await invokePrepareDrawDocumentUpload({
      filename: "test.csv",
      name: "test",
      type: DrawDocumentType.HuntCodes,
      contentType: "text/csv",
    });

    const document = await getDrawDocument(uploadDoc.id, ORG_ID);

    expect(document).toMatchObject({
      id: uploadDoc.id,
      name: "test",
      type: DrawDocumentType.HuntCodes,
      processingStatus: DrawDocumentProcessingStatus.UrlGenerated,
      __typename: DynamoDBTypename.DRAW_DOCUMENT,
    });
  });

  it("Should throw an error if the document does not exist.", async () => {
    await expect(getDrawDocument("non-existent", ORG_ID)).rejects.toThrowError("Document not found.");
  });

  describe("Draw Document Delete", () => {
    it("Should delete a draw document", async () => {
      const document = await createTestDrawDocument(TOKEN);

      const [drawDocumentEntity] = await getDrawDocumentEntities([document.id], ORG_ID);
      expect(drawDocumentEntity).toBeDefined();

      await deleteDrawDocument({ id: document.id }, TOKEN);

      await expect(getDrawDocument(document.id, ORG_ID)).rejects.toThrow("Document not found");
    });
  });

  describe("Prepare Draw Document Upload", () => {
    it("Should delete existing document links when uploading a new document", async () => {
      const { workflowNode: node1 } = await invokePrepareDrawDocumentUpload({
        filename: "test.csv",
        name: "test",
        type: DrawDocumentType.HuntCodes,
        contentType: "text/csv",
      });

      const { drawDocumentUpload: newDocUpload } = await invokePrepareDrawDocumentUpload({
        filename: "new-test.csv",
        name: "new test",
        type: DrawDocumentType.HuntCodes,
        contentType: "text/csv",
        workflowNode: node1,
      });

      const documents = await listDrawDocumentEntitiesByWorkflowNodeIds({
        organizationId: ORG_ID,
        workflowNodeIds: [node1.id],
      });

      expect(documents).toEqual([expect.objectContaining(newDocUpload.document)]);
    });
  });
});

import { dynamoDBClient } from "@/dynamo/dynamo";
import { validateCsvHandler } from "@/queue/draw-document";
import { createTestDrawDocuments, deleteAllS3TestDataByOrg, uploadTestRecordsToS3AsCsv } from "@/util/test-utils";
import { buildTestToken } from "@aspira-nextgen/core/authn-test-utils";
import { deleteAllTableItemsByPk } from "@aspira-nextgen/core/dynamodb-test-utils";
import { DrawDocumentProcessingStatus } from "@aspira-nextgen/graphql/resolvers";
import type { SQSEvent } from "aws-lambda";
import { Table } from "sst/node/table";
import { ulid } from "ulidx";
import { afterEach, describe, expect, it, vi } from "vitest";
import { getDrawDocumentEntities } from "#dynamo/draw-document.ts";

// We won't be consuming the pre-signed URLs.
vi.mock("@aws-sdk/s3-request-presigner", () => ({
  getSignedUrl: vi.fn().mockReturnValue("https://example.com"),
}));

const ORG_ID = `org_${ulid()}`;

const TOKEN = buildTestToken(ORG_ID);

function buildJsonS3EventBody(docId: string): string {
  return JSON.stringify({
    Records: [
      {
        s3: {
          object: {
            key: `${ORG_ID}/${docId}/test.csv`,
          },
        },
      },
    ],
  });
}

describe("Draw Document Tests", () => {
  describe("Draw-Document Queue Tests", () => {
    afterEach(async () => {
      vi.clearAllMocks();
      await deleteAllTableItemsByPk(dynamoDBClient, Table.Draw.tableName, [ORG_ID]);
      await deleteAllS3TestDataByOrg(ORG_ID);
    });

    it("Should validate hunt-codes.", async () => {
      const { drawDataDocument: document } = await createTestDrawDocuments(
        TOKEN,
        DrawDocumentProcessingStatus.UrlGenerated,
      );

      let [drawDocumentEntity] = await getDrawDocumentEntities([document.id], ORG_ID);

      const testRecords = [
        {
          hunt_code: "AE000A1R",
          description1: "Special Restrictions",
          description2: "See Regulation #229B",
          total_quota: "10",
          is_valid: "Y",
          in_the_draw: "Y",
          nr_cap_chk: "Y",
          nrcap_prect: "10",
        },
        {
          hunt_code: "BE075U1A",
          description1: "Special Restrictions See Brochure",
          description2: "",
          total_quota: "",
          is_valid: "Y",
          in_the_draw: "N",
          nr_cap_chk: "N",
          nrcap_prect: "",
        },
        {
          hunt_code: "DE012P4R",
          description1: "Private Land Only",
          description2: "",
          total_quota: "100",
          is_valid: "N",
          in_the_draw: "Y",
          nr_cap_chk: "N",
          nrcap_prect: "",
        },
      ];

      await uploadTestRecordsToS3AsCsv({
        s3Key: drawDocumentEntity.s3Key,
        records: testRecords,
      });

      await validateCsvHandler({
        Records: [
          {
            body: buildJsonS3EventBody(drawDocumentEntity.id),
          },
        ],
      } as SQSEvent);

      drawDocumentEntity = (await getDrawDocumentEntities([document.id], ORG_ID))[0];

      expect(drawDocumentEntity.processingStatus).toBe(DrawDocumentProcessingStatus.ValidationFinished);
    });

    it("Should handle errors when importing the CSV.", async () => {
      const { drawDataDocument: document } = await createTestDrawDocuments(
        TOKEN,
        DrawDocumentProcessingStatus.UrlGenerated,
      );

      let [drawDocumentEntity] = await getDrawDocumentEntities([document.id], ORG_ID);

      await uploadTestRecordsToS3AsCsv({
        s3Key: drawDocumentEntity.s3Key,
        records: [
          {
            hunt_code: "", // Missing required field
            description1: "Special Restrictions",
            description2: "See Regulation #229B",
            total_quota: "10",
          },
        ],
      });

      const jsonS3EventBody = JSON.stringify({
        Records: [
          {
            s3: {
              object: {
                key: `${ORG_ID}/${drawDocumentEntity.id}/test.csv`,
              },
            },
          },
        ],
      });

      await validateCsvHandler({
        Records: [
          {
            body: jsonS3EventBody,
          },
        ],
      } as SQSEvent);

      drawDocumentEntity = (await getDrawDocumentEntities([document.id], ORG_ID))[0];

      expect(drawDocumentEntity.processingStatus).toBe(DrawDocumentProcessingStatus.ValidationFailed);
    });
  });
});

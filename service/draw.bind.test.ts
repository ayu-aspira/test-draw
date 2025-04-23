import { DynamoDBTypename, dynamoDBClient } from "@/dynamo/dynamo";
import { listDrawsForDrawCategory } from "@/service/draw";
import { createDraw, getDraw, updateDraw } from "@/service/draw";
import { prepareDrawDocumentUpload } from "@/service/draw-document";
import { GraphQLTypename } from "@/service/graphql";
import {
  createTestDraw,
  createTestDrawCategory,
  createTestDrawDocuments,
  deleteAllS3TestDataByOrg,
  generateSample,
} from "@/util/test-utils";
import { buildTestToken } from "@aspira-nextgen/core/authn-test-utils";
import { deleteAllTableItemsByPk, withConsistencyRetries } from "@aspira-nextgen/core/dynamodb-test-utils";
import { DrawDocumentType, DrawSortDirection } from "@aspira-nextgen/graphql/resolvers";
import { Table } from "sst/node/table";
import { ulid } from "ulidx";
import { afterEach, describe, expect, it, vi } from "vitest";

vi.mock("@aws-sdk/s3-request-presigner", () => ({
  getSignedUrl: vi.fn().mockReturnValue("https://example.com"),
}));

vi.mock("@aws-sdk/client-sfn", () => ({
  SFNClient: vi.fn().mockImplementation(() => ({
    send: vi.fn(),
  })),
  StartExecutionCommand: vi.fn(),
}));

const ORG_ID = `org_${ulid()}`;
const TOKEN = buildTestToken(ORG_ID);

// ccapps - these are all being phased out.
// TODO: remove once the backing code is removed
describe.skip("Draw Service Tests", () => {
  afterEach(async () => {
    await deleteAllTableItemsByPk(dynamoDBClient, Table.Draw.tableName, [ORG_ID]);
    await deleteAllS3TestDataByOrg(ORG_ID);
  });

  describe("Create Draw Tests", () => {
    it("Should create a draw", async () => {
      const draw = await createTestDraw(TOKEN);

      const { Item } = await dynamoDBClient.get({
        TableName: Table.Draw.tableName,
        Key: { pk: ORG_ID, sk: draw.id },
        ConsistentRead: true,
      });

      expect(Item).toEqual({
        pk: ORG_ID,
        sk: draw.id,
        id: draw.id,
        name: draw.name,
        drawDataDocumentId: draw.drawDataDocument.id,
        applicantDocumentId: draw.applicantDocument.id,
        sortRules: [],
        drawCategoryId: draw.drawCategoryId,
        oneToManyKey: `${draw.drawCategoryId}#${draw.id}`,
        createdBy: TOKEN.sub,
        createdAt: Item?.createdAt,
        __typename: DynamoDBTypename.DRAW,
      });
    });

    it("Should throw an error if a draw with the same name already exists.", async () => {
      const draw = await createTestDraw(TOKEN, { name: "duplicate test draw" });

      await expect(
        createDraw(
          {
            name: "duplicate test draw",
            drawDataDocumentId: draw.drawDataDocument.id,
            applicantDocumentId: draw.applicantDocument.id,
            drawCategoryId: draw.drawCategoryId,
          },
          TOKEN,
        ),
      ).rejects.toThrow("Draw with the same name already exists");
    });

    it("Should throw an error if the documentIds don't exist.", async () => {
      const drawCategory = await createTestDrawCategory(TOKEN);

      await expect(
        createDraw(
          {
            name: "test draw",
            drawDataDocumentId: "draw_document_123",
            applicantDocumentId: "draw_document_456",
            drawCategoryId: drawCategory.id,
          },
          TOKEN,
        ),
      ).rejects.toThrow("Provided draw data document either does not exist or has not finished importing.");

      const { document: drawDataDocument } = await prepareDrawDocumentUpload(
        {
          filename: "test.csv",
          name: "huntcodes",
          type: DrawDocumentType.HuntCodes,
          contentType: "text/csv",
          workflowNodeId: "",
        },
        TOKEN,
      );

      await expect(
        createDraw(
          {
            name: "test draw",
            drawDataDocumentId: drawDataDocument.id,
            applicantDocumentId: "draw_document_456",
            drawCategoryId: drawCategory.id,
          },
          TOKEN,
        ),
      ).rejects.toThrow("Provided applicant document either does not exist or has not finished importing.");
    });

    it("Should throw an error if the documents haven't finished processing.", async () => {
      const drawCategory = await createTestDrawCategory(TOKEN);

      // Not providing a processing status will default to UrlGenerated.
      const { drawDataDocument, applicantDocument } = await createTestDrawDocuments(TOKEN);

      await expect(
        createDraw(
          {
            name: "test draw",
            drawDataDocumentId: drawDataDocument.id,
            applicantDocumentId: applicantDocument.id,
            drawCategoryId: drawCategory.id,
          },
          TOKEN,
        ),
      ).rejects.toThrow("Provided draw data document either does not exist or has not finished importing.");

      await expect(
        createDraw(
          {
            name: "test draw",
            drawDataDocumentId: drawDataDocument.id,
            applicantDocumentId: applicantDocument.id,
            drawCategoryId: drawCategory.id,
          },
          TOKEN,
        ),
      ).rejects.toThrow("Provided applicant document either does not exist or has not finished importing.");
    });
  });

  describe("Get Draw Tests", () => {
    it("Should get a draw", async () => {
      const draw = await createTestDraw(TOKEN);

      const retrievedDraw = await getDraw(draw.id, TOKEN);

      expect(retrievedDraw).toMatchObject({
        id: draw.id,
        name: draw.name,
        drawDataDocument: draw.drawDataDocument,
        applicantDocument: draw.applicantDocument,
        drawCategoryId: draw.drawCategoryId,
        oneToManyKey: `${draw.drawCategoryId}#${draw.id}`,
        sortRules: [],
        __typename: DynamoDBTypename.DRAW,
      });
    });
  });

  describe("Update Draw Tests", () => {
    it("Should update a draw", async () => {
      const draw = await createTestDraw(TOKEN);

      const updatedDraw = await updateDraw(
        {
          id: draw.id,
          name: "updated draw",
          drawDataDocumentId: draw.drawDataDocument.id,
          applicantDocumentId: draw.applicantDocument.id,
          sortRules: [
            {
              field: "age",
              direction: DrawSortDirection.Asc,
            },
          ],
          drawCategoryId: draw.drawCategoryId,
        },
        TOKEN,
      );

      expect(updatedDraw).toMatchObject({
        id: draw.id,
        name: "updated draw",
        drawDataDocument: draw.drawDataDocument,
        applicantDocument: draw.applicantDocument,
        sortRules: [
          {
            field: "age",
            direction: DrawSortDirection.Asc,
            __typename: GraphQLTypename.DRAW_SORT_RULE,
          },
        ],
        __typename: DynamoDBTypename.DRAW,
      });
    });

    it("Should partially update a draw", async () => {
      const draw = await createTestDraw(TOKEN);

      let updatedDraw = await updateDraw(
        {
          id: draw.id,
          name: "updated draw",
        },
        TOKEN,
      );

      expect(updatedDraw).toMatchObject({
        id: draw.id,
        name: "updated draw",
        drawDataDocument: draw.drawDataDocument,
        applicantDocument: draw.applicantDocument,
        sortRules: [],
        __typename: DynamoDBTypename.DRAW,
      });

      updatedDraw = await updateDraw(
        {
          id: draw.id,
          sortRules: [
            {
              field: "age",
              direction: DrawSortDirection.Asc,
            },
          ],
        },
        TOKEN,
      );

      expect(updatedDraw).toMatchObject({
        id: draw.id,
        name: "updated draw",
        drawDataDocument: draw.drawDataDocument,
        applicantDocument: draw.applicantDocument,
        sortRules: [
          {
            field: "age",
            direction: DrawSortDirection.Asc,
            __typename: GraphQLTypename.DRAW_SORT_RULE,
          },
        ],
        __typename: DynamoDBTypename.DRAW,
      });
    });

    it("Should throw an error if the draw doesn't exist.", async () => {
      await expect(
        updateDraw(
          {
            id: "non-existent-draw",
            name: "updated draw",
            drawDataDocumentId: "draw_document_123",
            applicantDocumentId: "draw_document_456",
            sortRules: [],
            drawCategoryId: "draw_category_789",
          },
          TOKEN,
        ),
      ).rejects.toThrow("Draw not found");
    });

    it("Should throw an error if updating a draw with the same name as an existing draw.", async () => {
      const draw = await createTestDraw(TOKEN);
      const draw2 = await createTestDraw(TOKEN);

      await expect(
        updateDraw(
          {
            id: draw2.id,
            name: draw.name,
            drawDataDocumentId: draw.drawDataDocument.id,
            applicantDocumentId: draw.applicantDocument.id,
            sortRules: [],
            drawCategoryId: draw.drawCategoryId,
          },
          TOKEN,
        ),
      ).rejects.toThrow("Draw with the same name already exists");
    });

    it("Should throw an error if the documentIds don't exist.", async () => {
      const draw = await createTestDraw(TOKEN);

      await expect(
        updateDraw(
          {
            id: draw.id,
            name: "updated draw",
            drawDataDocumentId: "draw_document_123",
            applicantDocumentId: "draw_document_456",
            sortRules: [],
            drawCategoryId: draw.drawCategoryId,
          },
          TOKEN,
        ),
      ).rejects.toThrow("Provided draw data document either does not exist or has not finished importing.");
    });

    it("Should throw an error if a sort rule field is invalid.", async () => {
      const draw = await createTestDraw(TOKEN);

      await expect(
        updateDraw(
          {
            id: draw.id,
            name: "updated draw",
            drawDataDocumentId: draw.drawDataDocument.id,
            applicantDocumentId: draw.applicantDocument.id,
            sortRules: [
              {
                field: "invalid",
                direction: DrawSortDirection.Asc,
              },
            ],
            drawCategoryId: draw.drawCategoryId,
          },
          TOKEN,
        ),
      ).rejects.toThrow("Invalid sort rule.");

      await expect(
        updateDraw(
          {
            id: draw.id,
            name: "updated draw",
            drawDataDocumentId: draw.drawDataDocument.id,
            applicantDocumentId: draw.applicantDocument.id,
            sortRules: [
              {
                field: "age",
                direction: DrawSortDirection.Asc,
              },
              {
                field: "age",
                direction: DrawSortDirection.Asc,
              },
            ],
            drawCategoryId: draw.drawCategoryId,
          },
          TOKEN,
        ),
      ).rejects.toThrow("Sorting multiple times on the same field is not allowed.");
    });
  });

  const generateDrawCategoryWithDraws = async (n?: number) => {
    const drawCategory = await createTestDrawCategory(TOKEN);
    const draws = await generateSample(() => createTestDraw(TOKEN, { drawCategoryId: drawCategory.id }), n);

    return {
      drawCategory,
      draws,
      count: draws.length,
    };
  };

  describe("List Draws under a DrawCategory Test", () => {
    it("Should list all draws for a draw category", async () => {
      const { drawCategory, draws } = await generateDrawCategoryWithDraws();

      const drawNames = draws.map((draw) => draw.name).sort();

      const { items } = await listDrawsForDrawCategory(
        {
          drawCategoryId: drawCategory.id,
          listDrawsInput: {
            limit: draws.length,
          },
        },
        TOKEN,
      );

      const itemNames = items.map((draw) => draw.name).sort();

      expect(itemNames).toMatchObject(drawNames);
    });

    it("Should return limit items when limit < count", async () => {
      const n = Math.ceil(Math.random() * 10) + 1;
      const { drawCategory, count } = await generateDrawCategoryWithDraws(n);
      const limit = count - Math.ceil(Math.random() * (count - 1));

      // for this to test the desired behavior, we need to ensure that the
      // limit is greater than 0 and less than the count,
      // this ensures we do not regress
      expect(limit).toBeGreaterThan(0);
      expect(limit).toBeLessThan(count);

      await withConsistencyRetries(async () => {
        const { items } = await listDrawsForDrawCategory(
          { drawCategoryId: drawCategory.id, listDrawsInput: { limit } },
          TOKEN,
        );

        expect(items.length).toBe(limit);
      });
    });

    it("Should return count items when limit > count", async () => {
      const { drawCategory, count } = await generateDrawCategoryWithDraws();
      const limit = count + Math.ceil(Math.random() * 10);

      await withConsistencyRetries(async () => {
        const { items } = await listDrawsForDrawCategory(
          { drawCategoryId: drawCategory.id, listDrawsInput: { limit } },
          TOKEN,
        );

        expect(items.length).toBe(count);
      });
    });

    it("Should enforce limit > 0", async () => {
      const drawCategory = await createTestDrawCategory(TOKEN);
      await expect(
        listDrawsForDrawCategory({ drawCategoryId: drawCategory.id, listDrawsInput: { limit: -1 } }, TOKEN),
      ).rejects.toThrow("Limit must be greater than 0");
    });
  });
});

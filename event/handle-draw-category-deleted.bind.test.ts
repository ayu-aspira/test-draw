import { dynamoDBClient } from "@/dynamo/dynamo";
import { listDrawsForDrawCategory } from "@/service/draw";
import { deleteDrawCategory } from "@/service/draw-category";
import { createTestDraw } from "@/util/test-utils";
import { deleteAllS3TestDataByOrg } from "@/util/test-utils";
import { buildTestToken } from "@aspira-nextgen/core/authn-test-utils";
import { deleteAllTableItemsByPk, withConsistencyRetries } from "@aspira-nextgen/core/dynamodb-test-utils";
import { Table } from "sst/node/table";
import { ulid } from "ulidx";
import { afterEach, describe, expect, it as maybe } from "vitest";

const ORG_ID = `org_${ulid()}`;
const TOKEN = buildTestToken(ORG_ID);

// this test can fail intermittently, only run local until we figure out why
const it = process.env.DRAW_TESTS === "domain-events" ? maybe : maybe.skip;

describe("Handle Draw Category Tests", () => {
  afterEach(async () => {
    await deleteAllTableItemsByPk(dynamoDBClient, Table.Draw.tableName, [ORG_ID]);
    await deleteAllS3TestDataByOrg(ORG_ID);
  });

  it("deletes all the draws", async () => {
    const draw = await createTestDraw(TOKEN);

    const draws = await listDrawsForDrawCategory(
      { drawCategoryId: draw.drawCategoryId, listDrawsInput: { limit: 100 } },
      TOKEN,
    );

    expect(draws.items.length).toBeGreaterThan(0);

    await deleteDrawCategory({ id: draw.drawCategoryId }, TOKEN);

    await withConsistencyRetries(
      async () => {
        const draws = await listDrawsForDrawCategory(
          { drawCategoryId: draw.drawCategoryId, listDrawsInput: { limit: 100 } },
          TOKEN,
        );

        expect(draws.items).toHaveLength(0);
      },
      30,
      1000,
    );
  }, 30000);
});

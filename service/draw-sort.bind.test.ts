import { getDrawSortEntity } from "@/dynamo/draw-sort";
import { DynamoDBPrefix, dynamoDBClient } from "@/dynamo/dynamo";
import { ApplicantSortOptions } from "@/s3/applicant";
import {
  createDrawSort,
  deleteDrawSort,
  getApplicantDrawSortOptions,
  getDrawSort,
  listDrawSorts,
  updateDrawSort,
} from "@/service/draw-sort";
import { createTestDrawSort, deleteAllS3TestDataByOrg } from "@/util/test-utils";
import { buildTestToken } from "@aspira-nextgen/core/authn-test-utils";
import { deleteAllTableItemsByPk } from "@aspira-nextgen/core/dynamodb-test-utils";
import { DrawSortDirection } from "@aspira-nextgen/graphql/resolvers";
import { Table } from "sst/node/table";
import { ulid } from "ulidx";
import { afterEach, describe, expect, it } from "vitest";

const ORG_ID = `org_${ulid()}`;
const TOKEN = buildTestToken(ORG_ID);

const testSortName = "test sort";
const testSortRules = [{ field: "pointBalance", direction: DrawSortDirection.Asc }];

describe("Draw Sort Service Tests", () => {
  afterEach(async () => {
    await deleteAllTableItemsByPk(dynamoDBClient, Table.Draw.tableName, [ORG_ID]);
    await deleteAllS3TestDataByOrg(ORG_ID);
  });

  describe("Create Draw Sort", () => {
    it("Should create a draw sort", async () => {
      const drawSort = await createTestDrawSort({ name: testSortName, rules: testSortRules }, TOKEN);

      const drawSortEntity = await getDrawSortEntity({ organizationId: ORG_ID, id: drawSort.id });

      expect(drawSortEntity).toMatchObject({
        id: drawSort.id,
        name: testSortName,
        rules: testSortRules,
      });
    });

    it("Should create a draw sort with no rules", async () => {
      const drawSort = await createTestDrawSort({ name: testSortName, rules: testSortRules }, TOKEN);

      const drawSortEntity = await getDrawSortEntity({ organizationId: ORG_ID, id: drawSort.id });

      expect(drawSortEntity).toMatchObject({
        id: drawSort.id,
        name: testSortName,
        rules: testSortRules,
      });
    });

    it("Should disallow creating a draw sort with a duplicate name.", async () => {
      await createTestDrawSort({ name: testSortName }, TOKEN);
      await expect(createTestDrawSort({ name: testSortName }, TOKEN)).rejects.toThrow(
        `Draw sort entity with the same name (${testSortName}) already exists`,
      );
    });

    it("Should disallow creating a draw sort against a non-existant applicant field.", async () => {
      await expect(
        createDrawSort(
          {
            name: "test sort",
            rules: [{ field: "nonExistantField", direction: DrawSortDirection.Asc }],
          },
          TOKEN,
        ),
      ).rejects.toThrow(`Invalid input: {"formErrors":[],"fieldErrors":{"rules":["Invalid sort field."]}}`);
    });

    it("Should disallow creating a draw sort with multiple rules on the same field.", async () => {
      await expect(
        createDrawSort(
          {
            name: "test sort",
            rules: [
              { field: "pointBalance", direction: DrawSortDirection.Asc },
              { field: "pointBalance", direction: DrawSortDirection.Desc },
            ],
          },
          TOKEN,
        ),
      ).rejects.toThrow(
        `Invalid input: {"formErrors":[],"fieldErrors":{"rules":["Sorting multiple times on the same field is not allowed."]}}`,
      );
    });
  });

  describe("Get Draw Sort", () => {
    it("Should get a draw sort", async () => {
      const drawSort = await createTestDrawSort({ name: testSortName, rules: testSortRules }, TOKEN);

      const fetchedDrawSort = await getDrawSort({ id: drawSort.id, organizationId: ORG_ID });

      expect(fetchedDrawSort).toMatchObject({
        id: drawSort.id,
        name: testSortName,
        rules: testSortRules,
      });
    });

    it("Should throw an error when fetching a non-existant draw sort", async () => {
      await expect(
        getDrawSort({ id: `${DynamoDBPrefix.DRAW_SORT}_${ulid()}`, organizationId: ORG_ID }),
      ).rejects.toThrow("Draw sort not found");
    });

    it("Should throw an error if the ID is invalid", async () => {
      await expect(getDrawSort({ id: "invalidId", organizationId: ORG_ID })).rejects.toThrowError(
        `Invalid input: {"formErrors":["Invalid ID"],"fieldErrors":{}}`,
      );
    });
  });

  describe("List Draw Sorts", () => {
    it("Should list draw sorts", async () => {
      const drawSort1 = await createTestDrawSort({ name: "sort1" }, TOKEN);
      const drawSort2 = await createTestDrawSort({ name: "sort2" }, TOKEN);
      const drawSort3 = await createTestDrawSort({ name: "sort3" }, TOKEN);

      let connection = await listDrawSorts({ limit: 2 }, TOKEN);

      expect(connection).toMatchObject({
        items: expect.arrayContaining([
          expect.objectContaining({ id: drawSort1.id, name: "sort1" }),
          expect.objectContaining({ id: drawSort2.id, name: "sort2" }),
        ]),
      });

      connection = await listDrawSorts({ limit: 2, nextToken: connection.nextToken }, TOKEN);

      expect(connection).toMatchObject({
        items: expect.arrayContaining([expect.objectContaining({ id: drawSort3.id, name: "sort3" })]),
      });
    });

    it("Should return no draw sorts when there are none", async () => {
      const drawSorts = await listDrawSorts({}, TOKEN);

      expect(drawSorts).toMatchObject({
        items: [],
      });
    });
  });

  describe("Update Draw Sort", () => {
    it("Should update a draw", async () => {
      const drawSort = await createTestDrawSort({ name: testSortName, rules: testSortRules }, TOKEN);

      // Just changing the name.
      await updateDrawSort(
        {
          id: drawSort.id,
          name: "updated sort",
        },
        TOKEN,
      );

      let updatedDrawSort = await getDrawSort({ id: drawSort.id, organizationId: ORG_ID });
      expect(updatedDrawSort).toMatchObject({
        id: drawSort.id,
        name: "updated sort",
        rules: testSortRules,
      });

      await updateDrawSort(
        {
          id: drawSort.id,
          rules: [...testSortRules, { field: "age", direction: DrawSortDirection.Asc }],
        },
        TOKEN,
      );

      updatedDrawSort = await getDrawSort({ id: drawSort.id, organizationId: ORG_ID });
      expect(updatedDrawSort).toMatchObject({
        id: drawSort.id,
        name: "updated sort",
        rules: [...testSortRules, { field: "age", direction: DrawSortDirection.Asc }],
      });
    });

    it("Should disallow updating a draw sort with a duplicate name.", async () => {
      const drawSort1 = await createTestDrawSort({ name: "sort1" }, TOKEN);
      await createTestDrawSort({ name: "sort2" }, TOKEN);

      await expect(updateDrawSort({ id: drawSort1.id, name: "sort2" }, TOKEN)).rejects.toThrow(
        "Draw sort name sort2 already exists.",
      );
    });

    it("Should disallow updating a draw sort against a non-existant applicant field.", async () => {
      const drawSort = await createTestDrawSort({ name: testSortName, rules: testSortRules }, TOKEN);
      await expect(
        updateDrawSort(
          {
            id: drawSort.id,
            name: "test sort",
            rules: [{ field: "nonExistantField", direction: DrawSortDirection.Asc }],
          },
          TOKEN,
        ),
      ).rejects.toThrow(`Invalid input: {"formErrors":[],"fieldErrors":{"rules":["Invalid sort field."]}}`);
    });

    it("Should disallow updating a draw sort with multiple rules on the same field.", async () => {
      const drawSort = await createTestDrawSort({ name: testSortName, rules: testSortRules }, TOKEN);
      await expect(
        updateDrawSort(
          {
            id: drawSort.id,
            name: "test sort",
            rules: [
              { field: "pointBalance", direction: DrawSortDirection.Asc },
              { field: "pointBalance", direction: DrawSortDirection.Desc },
            ],
          },
          TOKEN,
        ),
      ).rejects.toThrow(
        `Invalid input: {"formErrors":[],"fieldErrors":{"rules":["Sorting multiple times on the same field is not allowed."]}}`,
      );
    });

    describe("Delete Draw Sort", () => {
      it("Should delete a draw sort", async () => {
        const drawSort = await createTestDrawSort({ name: testSortName, rules: testSortRules }, TOKEN);

        await deleteDrawSort({ id: drawSort.id }, TOKEN);

        await expect(getDrawSort({ id: drawSort.id, organizationId: ORG_ID })).rejects.toThrow("Draw sort not found");
      });
    });
  });

  describe("Applicant Sort Options Tests", () => {
    it("Should return applicant sort options", () => {
      expect(getApplicantDrawSortOptions()).toEqual(ApplicantSortOptions);
    });
  });
});

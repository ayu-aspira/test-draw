import { DynamoDBTypename, dynamoDBClient } from "@/dynamo/dynamo";
import {
  createDrawCategory,
  deleteDrawCategory,
  getDrawCategory,
  listDrawCategories,
  updateDrawCategory,
} from "@/service/draw-category";
import { buildTestToken } from "@aspira-nextgen/core/authn-test-utils";
import { deleteAllTableItemsByPk } from "@aspira-nextgen/core/dynamodb-test-utils";
import { Table } from "sst/node/table";
import { ulid } from "ulidx";
import { afterEach, describe, expect, it } from "vitest";

const ORG_ID = `org_${ulid()}`;
const TOKEN = buildTestToken(ORG_ID);

describe("DrawCategory Draws Test", () => {
  afterEach(async () => {
    await deleteAllTableItemsByPk(dynamoDBClient, Table.Draw.tableName, [ORG_ID]);
  });

  describe("Create DrawCategory Tests", () => {
    it("Should create a draw category", async () => {
      const drawCategory = await createDrawCategory(
        {
          name: "test category",
        },
        TOKEN,
      );

      const { Item } = await dynamoDBClient.get({
        TableName: Table.Draw.tableName,
        Key: { pk: ORG_ID, sk: drawCategory.id },
        ConsistentRead: true,
      });

      // NOTE: because we grab this directly from the database, there should be no draws: field
      expect(Item).toEqual({
        pk: ORG_ID,
        sk: drawCategory.id,
        id: drawCategory.id,
        name: "test category",
        createdBy: TOKEN.sub,
        createdAt: Item?.createdAt,
        __typename: DynamoDBTypename.DRAW_CATEGORY,
      });
    });

    // Skipping as we removed the unique constraint on draw category names
    it.skip("Should throw an error if a draw category with the same name already exists.", async () => {
      const categoryName = `test category ${ulid()}`;
      await createDrawCategory(
        {
          name: categoryName,
        },
        TOKEN,
      );

      await expect(
        createDrawCategory(
          {
            name: categoryName,
          },
          TOKEN,
        ),
      ).rejects.toThrow(`A draw category with the name "${categoryName}" already exists`);
    });
  });

  describe("Get DrawCategory Test", () => {
    it("Should get a draw category", async () => {
      const categoryName = `test category ${ulid()}`;
      const drawCategory = await createDrawCategory(
        {
          name: categoryName,
        },
        TOKEN,
      );

      const retrievedDrawCategory = await getDrawCategory(drawCategory.id, TOKEN);

      expect(retrievedDrawCategory).toMatchObject({
        id: drawCategory.id,
        name: categoryName,
        __typename: DynamoDBTypename.DRAW_CATEGORY,
      });
    });

    it("Should throw an error if a draw category does not exist.", async () => {
      await expect(getDrawCategory(ulid(), TOKEN)).rejects.toThrow("Draw category not found");
    });
  });

  const generateDrawCategories = async (n?: number) => {
    const count = n ?? Math.ceil(Math.random() * 10);

    return await Promise.all(
      [...Array(count).keys()].map(() => {
        const categoryName = `test category ${ulid()}`;
        return createDrawCategory({ name: categoryName }, TOKEN);
      }),
    );
  };

  describe("List DrawCategories Test", () => {
    it("Should list all draw categories for an org", async () => {
      const drawCategories = await generateDrawCategories();

      const names = drawCategories.map((category) => category.name).sort();

      const { items: retrievedDrawCategories } = await listDrawCategories({}, TOKEN);
      const retrievedNames = retrievedDrawCategories.map((category) => category.name).sort();

      expect(names).toMatchObject(retrievedNames);
    });

    it("Should return limit items when limit < count", async () => {
      const count = Math.ceil(Math.random() * 10) + 1;
      const limit = count - Math.ceil(Math.random() * (count - 1));

      await generateDrawCategories(count);

      const { items } = await listDrawCategories({ limit }, TOKEN);

      expect(items.length).toBe(limit);
    });

    it("Should return count items when limit > count", async () => {
      const count = Math.ceil(Math.random() * 10);
      const limit = count + Math.ceil(Math.random() * 10);

      await generateDrawCategories(count);

      const { items } = await listDrawCategories({ limit }, TOKEN);

      expect(items.length).toBe(count);
    });

    it("Should enforce limit > 0", async () => {
      await expect(listDrawCategories({ limit: -1 }, TOKEN)).rejects.toThrow("Limit must be greater than 0");
    });
  });

  describe("Delete DrawCategories Test", () => {
    it("Should delete a draw category", async () => {
      const drawCategories = await generateDrawCategories();
      const count = drawCategories.length;

      for (let idx = 0; idx < count; idx++) {
        await deleteDrawCategory({ id: drawCategories[idx].id }, TOKEN);
        const { items: retrievedDrawCategories } = await listDrawCategories({}, TOKEN);

        expect(retrievedDrawCategories.length).toBe(count - idx - 1);

        await expect(getDrawCategory(drawCategories[idx].id, TOKEN)).rejects.toThrow("Draw category not found");
      }
    });
  });

  describe("Update DrawCategory Tests", () => {
    it("Should update a draw category", async () => {
      const drawCategory = await createDrawCategory(
        {
          name: "test category",
        },
        TOKEN,
      );

      const categoryName = `test category ${ulid()}`;
      await updateDrawCategory(
        {
          id: drawCategory.id,
          name: categoryName,
        },
        TOKEN,
      );

      const updatedDrawCategory = await getDrawCategory(drawCategory.id, TOKEN);

      expect(updatedDrawCategory).toMatchObject({
        id: drawCategory.id,
        name: categoryName,
        __typename: DynamoDBTypename.DRAW_CATEGORY,
      });
    });

    // Skipping as we removed the unique constraint on draw category names
    it.skip("Should throw an error if a draw category with the same name already exists.", async () => {
      const categoryName = `test category ${ulid()}`;
      await createDrawCategory(
        {
          name: categoryName,
        },
        TOKEN,
      );

      const drawCategory = await createDrawCategory(
        {
          name: `test category ${ulid()}`,
        },
        TOKEN,
      );

      await expect(
        updateDrawCategory(
          {
            id: drawCategory.id,
            name: categoryName,
          },
          TOKEN,
        ),
      ).rejects.toThrow(`A draw category with the name "${categoryName}" already exists`);
    });
  });
});

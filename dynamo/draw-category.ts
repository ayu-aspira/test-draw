import { DEFAULT_LIMIT, DynamoDBPrefix, DynamoDBTypename, dynamoDBClient, getDrawTableName } from "@/dynamo/dynamo";
import type { Token } from "@aspira-nextgen/core/authn";
import {
  type BaseEntity,
  type CreateMetadata,
  type WithMetadata,
  generateUpdateExpression,
  getExclusiveStartKey,
  getNextToken,
} from "@aspira-nextgen/core/dynamodb";
import type { ListDrawCategoriesInput } from "@aspira-nextgen/graphql/resolvers";
import { ulid } from "ulidx";

export type DrawCategoryEntity = WithMetadata<
  BaseEntity & {
    id: string;
    name: string;
  }
>;

const buildDrawCategorySk = (id?: string) => `${DynamoDBPrefix.DRAW_CATEGORY}_${id ?? ""}`;

export const createDrawCategoryEntity = async (
  input: {
    name: string;
  } & CreateMetadata,
  token: Token,
): Promise<DrawCategoryEntity> => {
  const id = buildDrawCategorySk(ulid());
  const { organizationId } = token.claims;

  const drawEntity: DrawCategoryEntity = {
    pk: organizationId,
    sk: id,
    id,
    ...input,
    __typename: DynamoDBTypename.DRAW_CATEGORY,
  };

  const drawCategoryTable = getDrawTableName();

  try {
    await dynamoDBClient.put({
      TableName: drawCategoryTable,
      Item: drawEntity,
    });
  } catch (err) {
    throw new Error("Failed to create draw category.", { cause: err });
  }

  return drawEntity;
};

export const getDrawCategoryEntity = async (id: {
  drawCategoryId: string;
  organizationId: string;
}): Promise<DrawCategoryEntity> => {
  const { organizationId, drawCategoryId } = id;
  const { Item: item } = await dynamoDBClient.get({
    TableName: getDrawTableName(),
    Key: {
      pk: organizationId,
      sk: drawCategoryId,
    },
    ConsistentRead: true,
  });

  if (!item) {
    throw new Error("Draw category not found");
  }

  return item as DrawCategoryEntity;
};

export const listDrawCategoryEntities = async (
  organizationId: string,
  input: ListDrawCategoriesInput,
): Promise<{ items: DrawCategoryEntity[]; nextToken?: string }> => {
  const { limit: unsafeLimit, nextToken: token } = input;

  const limit = unsafeLimit ?? DEFAULT_LIMIT;

  if (limit < 0) {
    throw new Error("Limit must be greater than 0");
  }

  try {
    const { Items: items = [], LastEvaluatedKey: key } = await dynamoDBClient.query({
      TableName: getDrawTableName(),
      KeyConditionExpression: "#pk = :pk and begins_with(#sk, :skPath)",
      ExpressionAttributeNames: {
        "#pk": "pk",
        "#sk": "sk",
      },
      ExpressionAttributeValues: {
        ":pk": organizationId,
        ":skPath": buildDrawCategorySk(),
      },
      Limit: limit,
      ExclusiveStartKey: getExclusiveStartKey(token),
      ConsistentRead: true,
    });

    const nextToken = getNextToken(key) ?? undefined;

    return {
      items: items as DrawCategoryEntity[],
      nextToken,
    };
  } catch (err) {
    throw new Error("Failed to get draw categories.", { cause: err });
  }
};

export const deleteDrawCategoryEntity = async (id: {
  drawCategoryId: string;
  token: Token;
}): Promise<void> => {
  const { drawCategoryId, token } = id;
  const {
    claims: { organizationId },
  } = token;

  const drawCategoryTable = getDrawTableName();

  try {
    await dynamoDBClient.delete({
      TableName: drawCategoryTable,
      Key: {
        pk: organizationId,
        sk: drawCategoryId,
      },
    });
  } catch (err) {
    throw new Error("Failed to delete draw category.", { cause: err });
  }
};

export const updateDrawCategoryEntity = async (input: {
  id: {
    organizationId: string;
    drawCategoryId: string;
  };
  updates: {
    name?: string;
  };
  updatedBy: string;
}): Promise<DrawCategoryEntity> => {
  const {
    id: { organizationId, drawCategoryId },
    updates,
    updatedBy,
  } = input;

  const drawCategoryTable = getDrawTableName();

  const updateExpression = generateUpdateExpression({
    fields: updates,
    updatedBy,
    table: drawCategoryTable,
    pk: organizationId,
    sk: drawCategoryId,
  });

  try {
    await dynamoDBClient.update(updateExpression);
  } catch (err) {
    throw new Error("Failed to update draw category entity", { cause: err });
  }

  return await getDrawCategoryEntity({ drawCategoryId, organizationId });
};

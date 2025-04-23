import { DEFAULT_LIMIT, DynamoDBPrefix, DynamoDBTypename, dynamoDBClient, getDrawTableName } from "@/dynamo/dynamo";
import { addErrorIntervalByEndExclusive, scrubUpdateFields } from "@/dynamo/util";
import {
  type BaseEntity,
  type ErrorIntervals,
  type WithMetadata,
  buildCreateMetadata,
  findAppropriateErrorMessage,
  findConditionalCheckFailedIndexes,
  generateUpdateExpression,
  getExclusiveStartKey,
  getNextToken,
  isTransactionConditionalCheckFailed,
} from "@aspira-nextgen/core/dynamodb";
import type { DrawSortDirection, ListDrawSortsInput } from "@aspira-nextgen/graphql/resolvers";
import { ulid } from "ulidx";

export type DrawSortEntity = WithMetadata<
  BaseEntity<DynamoDBTypename.DRAW_SORT> & {
    id: string;
    name: string;
    rules: {
      field: string;
      direction: DrawSortDirection;
    }[];
  }
>;

export const createDrawSortEntity = async (input: {
  organizationId: string;
  name: string;
  rules: {
    field: string;
    direction: DrawSortDirection;
  }[];
  createdBy: string;
}) => {
  const { organizationId, name, rules, createdBy } = input;

  const id = `${DynamoDBPrefix.DRAW_SORT}_${ulid()}`;

  const entity: DrawSortEntity = {
    pk: organizationId,
    sk: id,
    id,
    name,
    rules,
    ...buildCreateMetadata(createdBy),
    __typename: DynamoDBTypename.DRAW_SORT,
  };

  try {
    await dynamoDBClient.transactWrite({
      TransactItems: [
        {
          Put: {
            TableName: getDrawTableName(),
            Item: entity,
            ConditionExpression: "attribute_not_exists(pk) AND attribute_not_exists(sk)",
          },
        },
        putDrawSortUniqueNameConstraint({ organizationId, name }),
      ],
    });
  } catch (err) {
    if (!isTransactionConditionalCheckFailed(err)) {
      throw new Error("Failed to create draw sort entity", { cause: err });
    }

    throw new Error(`Draw sort entity with the same name (${name}) already exists`);
  }

  return entity;
};

export const updateDrawSortEntity = async (input: {
  id: {
    organizationId: string;
    drawSortId: string;
  };
  updates: {
    name?: string;
    rules?: {
      field: string;
      direction: DrawSortDirection;
    }[];
  };
  updatedBy: string;
}): Promise<DrawSortEntity> => {
  const { organizationId, drawSortId } = input.id;
  const updateFields = scrubUpdateFields(input.updates);

  const transactItems = [];
  let errorIntervals: ErrorIntervals = {};

  if (updateFields.name) {
    const drawSort = await getDrawSortEntity({ organizationId, id: drawSortId });

    if (!drawSort) {
      throw new Error("Draw sort not found");
    }

    if (updateFields.name !== drawSort.name) {
      const end = transactItems.push(
        deleteDrawSortUniqueNameConstraint({ organizationId, name: drawSort.name }),
        putDrawSortUniqueNameConstraint({ organizationId, name: updateFields.name }),
      );

      errorIntervals = addErrorIntervalByEndExclusive({
        intervals: errorIntervals,
        message: `Draw sort name ${updateFields.name} already exists.`,
        endExclusive: end,
      });
    }
  }

  const end = transactItems.push({
    Update: generateUpdateExpression({
      table: getDrawTableName(),
      pk: organizationId,
      sk: drawSortId,
      fields: input.updates,
      updatedBy: input.updatedBy,
    }),
  });

  errorIntervals = addErrorIntervalByEndExclusive({
    intervals: errorIntervals,
    message: "Draw sort not found.",
    endExclusive: end,
  });

  try {
    await dynamoDBClient.transactWrite({
      TransactItems: transactItems,
    });
  } catch (err) {
    if (!isTransactionConditionalCheckFailed(err)) {
      throw new Error("Failed to update draw sort entity", { cause: err });
    }

    const failedIndexes = findConditionalCheckFailedIndexes(err);

    if (!failedIndexes || failedIndexes.length === 0) {
      throw new Error("Failed to update draw sort", { cause: err });
    }

    const errorMessage = findAppropriateErrorMessage(failedIndexes, errorIntervals);

    throw new Error(errorMessage, { cause: err });
  }

  // Casting because it has to exist at this point.
  return (await getDrawSortEntity({ organizationId, id: drawSortId })) as DrawSortEntity;
};

export const getDrawSortEntity = async (input: { organizationId: string; id: string }): Promise<
  DrawSortEntity | undefined
> => {
  const { organizationId, id } = input;

  const { Item } = await dynamoDBClient.get({
    TableName: getDrawTableName(),
    Key: {
      pk: organizationId,
      sk: id,
    },
    ConsistentRead: true,
  });

  return Item as DrawSortEntity | undefined;
};

export const listDrawSortEntities = async (input: {
  organizationId: string;
  paginationRequest: ListDrawSortsInput;
}): Promise<{ nextToken?: string; items: DrawSortEntity[] }> => {
  const { limit, nextToken } = input.paginationRequest;

  const { Items = [], LastEvaluatedKey } = await dynamoDBClient.query({
    TableName: getDrawTableName(),
    KeyConditionExpression: "pk = :pk and begins_with(sk, :sk)",
    ExpressionAttributeValues: {
      ":pk": input.organizationId,
      ":sk": DynamoDBPrefix.DRAW_SORT,
    },
    Limit: limit ?? DEFAULT_LIMIT,
    ExclusiveStartKey: getExclusiveStartKey(nextToken),
  });

  return {
    items: Items as DrawSortEntity[],
    nextToken: getNextToken(LastEvaluatedKey) ?? undefined,
  };
};

export const deleteDrawSortEntity = async (input: {
  organizationId: string;
  drawSortId: string;
}): Promise<boolean> => {
  const { organizationId, drawSortId } = input;

  const drawSort = await getDrawSortEntity({ organizationId, id: drawSortId });

  if (!drawSort) {
    return false;
  }

  const { name } = drawSort;

  try {
    await dynamoDBClient.transactWrite({
      TransactItems: [
        deleteDrawSortUniqueNameConstraint({ organizationId, name: name }),
        {
          Delete: {
            TableName: getDrawTableName(),
            Key: {
              pk: organizationId,
              sk: drawSortId,
            },
          },
        },
      ],
    });
  } catch (err) {
    throw new Error("Failed to delete draw sort", { cause: err });
  }

  return true;
};

const generateDrawSortUniqueNameConstraint = (name: string) =>
  `${DynamoDBPrefix.DRAW_SORT_NAME_UNIQUE_CONSTRAINT}#${name.trim().toLowerCase()}`;

const deleteDrawSortUniqueNameConstraint = (input: { organizationId: string; name: string }) => ({
  Delete: {
    TableName: getDrawTableName(),
    Key: {
      pk: input.organizationId,
      sk: generateDrawSortUniqueNameConstraint(input.name),
    },
  },
});

const putDrawSortUniqueNameConstraint = (input: { organizationId: string; name: string }) => ({
  Put: {
    TableName: getDrawTableName(),
    Item: { pk: input.organizationId, sk: generateDrawSortUniqueNameConstraint(input.name) },
    ConditionExpression: "attribute_not_exists(pk) AND attribute_not_exists(sk)",
  },
});

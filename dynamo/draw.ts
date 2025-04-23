import {
  DEFAULT_LIMIT,
  DynamoDBIndex,
  DynamoDBPrefix,
  DynamoDBTypename,
  dynamoDBClient,
  getDrawTableName,
} from "@/dynamo/dynamo";
import { addErrorIntervalByEndExclusive } from "@/dynamo/util";
import type { Token } from "@aspira-nextgen/core/authn";
import {
  type BaseEntity,
  type CreateMetadata,
  type ErrorIntervals,
  type WithMetadata,
  findAppropriateErrorMessage,
  findConditionalCheckFailedIndexes,
  generateUpdateExpression,
  getExclusiveStartKey,
  getNextToken,
  isTransactionCanceledException,
} from "@aspira-nextgen/core/dynamodb";
import {
  DrawDocumentProcessingStatus,
  DrawDocumentType,
  type DrawSortDirection,
  type ListDrawsInput,
} from "@aspira-nextgen/graphql/resolvers";
import { ulid } from "ulidx";

export type DrawEntity = WithMetadata<
  BaseEntity & {
    id: string;
    name: string;
    drawDataDocumentId: string;
    applicantDocumentId: string;
    sortRules: {
      field: string;
      direction: DrawSortDirection;
    }[];
    drawCategoryId: string;
    oneToManyKey: string;
  }
>;

export const createDrawEntity = async (
  input: {
    name: string;
    drawDataDocumentId: string;
    applicantDocumentId: string;
    drawCategoryId: string;
  } & CreateMetadata,
  token: Token,
): Promise<DrawEntity> => {
  const id = `${DynamoDBPrefix.DRAW}_${ulid()}`;
  const { organizationId } = token.claims;

  const oneToManyKey = `${input.drawCategoryId}#${id}`;

  const conditionChecks = [
    buildDocumentIdConditionCheck(input.drawDataDocumentId, DrawDocumentType.HuntCodes, organizationId),
    buildDocumentIdConditionCheck(input.applicantDocumentId, DrawDocumentType.Applicants, organizationId),
    buildDrawCategoryIdConditionCheck(input.drawCategoryId, organizationId),
  ];

  const drawEntity: DrawEntity = {
    pk: organizationId,
    sk: id,
    id,
    ...input,
    sortRules: [], // Draws are created w/out sort rules by default.
    oneToManyKey,
    __typename: DynamoDBTypename.DRAW,
  };

  try {
    await dynamoDBClient.transactWrite({
      TransactItems: [
        ...conditionChecks,
        putDrawUniqueNameConstraint({ organizationId, name: input.name }),
        {
          Put: {
            TableName: getDrawTableName(),
            Item: drawEntity,
          },
        },
      ],
    });
  } catch (err) {
    if (!isTransactionCanceledException(err)) {
      throw new Error("Failed to create draw entity", { cause: err });
    }

    const failedIndexes = findConditionalCheckFailedIndexes(err);

    if (!failedIndexes || failedIndexes.length === 0) {
      throw new Error("Failed to create draw entity", { cause: err });
    }

    const errorMessage = findAppropriateErrorMessage(failedIndexes, {
      "Provided draw data document either does not exist or has not finished importing.": {
        start: 0,
        end: 1,
      },
      "Provided applicant document either does not exist or has not finished importing.": {
        start: 1,
        end: 2,
      },
      "Provided draw category ID does not exist.": {
        start: 2,
        end: 3,
      },
      "Draw with the same name already exists.": {
        start: 3,
        end: 4,
      },
    });

    throw new Error(errorMessage, { cause: err });
  }

  return drawEntity;
};

export const updateDrawEntity = async (input: {
  id: {
    organizationId: string;
    drawId: string;
  };
  updates: {
    name?: string;
    drawDataDocumentId?: string;
    applicantDocumentId?: string;
    sortRules?: {
      field: string;
      direction: DrawSortDirection;
    }[];
    drawCategoryId?: string;
  };
  updatedBy: string;
}): Promise<DrawEntity> => {
  const { id, updates, updatedBy } = input;
  const { drawId, organizationId } = id;
  const { name, drawDataDocumentId, applicantDocumentId, sortRules, drawCategoryId } = updates;

  const fields: (typeof input)["updates"] & { oneToManyKey?: string } = {};

  const transactItems = [];

  let errorMessageIndices: ErrorIntervals = {};

  if (drawDataDocumentId) {
    const end = transactItems.push(
      buildDocumentIdConditionCheck(drawDataDocumentId, DrawDocumentType.HuntCodes, organizationId),
    );
    fields.drawDataDocumentId = drawDataDocumentId;
    errorMessageIndices = addErrorIntervalByEndExclusive({
      intervals: errorMessageIndices,
      message: "Provided draw data document either does not exist or has not finished importing.",
      endExclusive: end,
    });
  }

  if (applicantDocumentId) {
    const end = transactItems.push(
      buildDocumentIdConditionCheck(applicantDocumentId, DrawDocumentType.Applicants, organizationId),
    );
    fields.applicantDocumentId = applicantDocumentId;
    errorMessageIndices = addErrorIntervalByEndExclusive({
      intervals: errorMessageIndices,
      message: "Provided applicant document either does not exist or has not finished importing.",
      endExclusive: end,
    });
  }

  if (drawCategoryId) {
    const end = transactItems.push(buildDrawCategoryIdConditionCheck(drawCategoryId, organizationId));
    fields.oneToManyKey = buildDrawCategoryOtmPath(drawCategoryId);
    errorMessageIndices = addErrorIntervalByEndExclusive({
      intervals: errorMessageIndices,
      message: "Provided draw category ID does not exist.",
      endExclusive: end,
    });
  }

  if (sortRules) {
    fields.sortRules = sortRules;
  }

  let isNameDifferent = false;

  if (name) {
    const currDrawEntity = await getDrawEntity({ drawId, organizationId });
    isNameDifferent = currDrawEntity.name !== name;

    if (isNameDifferent) {
      const end = transactItems.push(
        deleteDrawUniqueNameConstraint({ organizationId, name: currDrawEntity.name }),
        putDrawUniqueNameConstraint({ organizationId, name }),
      );

      errorMessageIndices = addErrorIntervalByEndExclusive({
        intervals: errorMessageIndices,
        message: "Draw with the same name already exists.",
        endExclusive: end,
      });
    }

    fields.name = name;
  }

  transactItems.push({
    Update: generateUpdateExpression({
      table: getDrawTableName(),
      pk: organizationId,
      sk: drawId,
      fields,
      updatedBy,
    }),
  });

  try {
    await dynamoDBClient.transactWrite({
      TransactItems: transactItems,
    });

    return await getDrawEntity({ drawId, organizationId });
  } catch (err) {
    if (!isTransactionCanceledException(err)) {
      throw new Error("Failed to update draw entity", { cause: err });
    }

    const failedIndexes = findConditionalCheckFailedIndexes(err);

    if (!failedIndexes || failedIndexes.length === 0) {
      throw new Error("Failed to update draw entity", { cause: err });
    }

    const errorMessage = findAppropriateErrorMessage(failedIndexes, errorMessageIndices);

    throw new Error(errorMessage, { cause: err });
  }
};

export const deleteDrawEntity = async (
  input: {
    drawId: string;
  },
  token: Token,
): Promise<void> => {
  const { drawId } = input;
  const {
    claims: { organizationId },
  } = token;

  const drawTable = getDrawTableName();

  const drawEntityDelete = {
    Delete: {
      TableName: drawTable,
      Key: {
        pk: organizationId,
        sk: drawId,
      },
    },
  };

  const drawEntity = await getDrawEntity({ drawId, organizationId });
  const drawEntityNameDelete = {
    Delete: {
      TableName: drawTable,
      Key: {
        pk: organizationId,
        sk: generateDrawUniqueConstraintKey(drawEntity.name),
      },
    },
  };

  try {
    await dynamoDBClient.transactWrite({
      TransactItems: [drawEntityDelete, drawEntityNameDelete],
    });
  } catch (err) {
    throw new Error("Failed to delete draw entity", { cause: err });
  }
};

export const getDrawEntity = async (input: { drawId: string; organizationId: string }): Promise<DrawEntity> => {
  const { drawId, organizationId } = input;

  const { Item } = await dynamoDBClient.get({
    TableName: getDrawTableName(),
    Key: {
      pk: organizationId,
      sk: drawId,
    },
    ConsistentRead: true,
  });

  if (!Item) {
    throw new Error("Draw not found");
  }

  return Item as DrawEntity;
};

export const listDrawEntitiesForDrawCategory = async (input: {
  drawCategoryId: string;
  organizationId: string;
  paginationRequest: ListDrawsInput;
}): Promise<{
  items: DrawEntity[];
  nextToken?: string;
}> => {
  const { drawCategoryId, organizationId, paginationRequest } = input;
  const { limit: unsafeLimit, nextToken: token } = paginationRequest;
  const limit = unsafeLimit ?? DEFAULT_LIMIT;

  if (limit < 0) {
    throw new Error("Limit must be greater than 0");
  }

  const { Items: items = [], LastEvaluatedKey: key } = await dynamoDBClient.query({
    TableName: getDrawTableName(),
    IndexName: DynamoDBIndex.ONE_TO_MANY,
    KeyConditionExpression: "pk = :pk and begins_with(oneToManyKey, :key)",
    ExpressionAttributeValues: {
      ":pk": organizationId,
      ":key": buildDrawCategoryOtmPath(drawCategoryId),
    },
    ScanIndexForward: false,
    Limit: limit,
    ExclusiveStartKey: getExclusiveStartKey(token),
  });

  const nextToken = getNextToken(key) ?? undefined;

  return {
    items: items as DrawEntity[],
    nextToken,
  };
};

export const listAllDrawEntitiesForDrawCategory = async (input: {
  drawCategoryId: string;
  organizationId: string;
}): Promise<DrawEntity[]> => {
  const { drawCategoryId, organizationId } = input;

  const { Items: items = [] } = await dynamoDBClient.query({
    TableName: getDrawTableName(),
    IndexName: DynamoDBIndex.ONE_TO_MANY,
    KeyConditionExpression: "pk = :pk and begins_with(oneToManyKey, :key)",
    ExpressionAttributeValues: {
      ":pk": organizationId,
      ":key": buildDrawCategoryOtmPath(drawCategoryId),
    },
    ScanIndexForward: false,
  });

  return items as DrawEntity[];
};

const buildDocumentIdConditionCheck = (documentId: string, type: DrawDocumentType, organizationId: string) => {
  return {
    ConditionCheck: {
      TableName: getDrawTableName(),
      Key: {
        pk: organizationId,
        sk: documentId,
      },
      ConditionExpression:
        "attribute_exists(pk) AND attribute_exists(sk) AND processingStatus = :processingStatus AND #type = :type",
      ExpressionAttributeValues: {
        ":processingStatus": DrawDocumentProcessingStatus.ValidationFinished,
        ":type": type,
      },
      ExpressionAttributeNames: {
        "#type": "type",
      },
    },
  };
};

const buildDrawCategoryIdConditionCheck = (drawCategoryId: string, organizationId: string) => {
  return {
    ConditionCheck: {
      TableName: getDrawTableName(),
      Key: {
        pk: organizationId,
        sk: drawCategoryId,
      },
      ConditionExpression: "attribute_exists(pk) AND attribute_exists(sk)",
    },
  };
};

const generateDrawUniqueConstraintKey = (name: string): string => {
  return `${DynamoDBPrefix.DRAW_NAME_UNIQUE_CONSTRAINT}_${name}`;
};

const buildDrawCategoryOtmPath = (drawCategoryId: string): string => {
  return `${drawCategoryId}#${DynamoDBPrefix.DRAW}`;
};

const deleteDrawUniqueNameConstraint = (input: { organizationId: string; name: string }) => ({
  Delete: {
    TableName: getDrawTableName(),
    Key: {
      pk: input.organizationId,
      sk: generateDrawUniqueConstraintKey(input.name),
    },
  },
});

const putDrawUniqueNameConstraint = (input: { organizationId: string; name: string }) => ({
  Put: {
    TableName: getDrawTableName(),
    Item: { pk: input.organizationId, sk: generateDrawUniqueConstraintKey(input.name) },
    ConditionExpression: "attribute_not_exists(pk) AND attribute_not_exists(sk)",
  },
});

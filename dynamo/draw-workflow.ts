import {
  DynamoDBPrefix,
  DynamoDBTypename,
  deleteEntities,
  dynamoDBClient,
  entityExistsConditionCheck,
  getWorkflowTableName,
  otmPathBuilder,
  queryAllOneToManyEntities,
  queryOneToManyEntitiesWithPagination,
} from "@/dynamo/dynamo";
import { addErrorIntervalByEndExclusive, scrubUpdateFields } from "@/dynamo/util";
import {
  type WorkflowEntity,
  WorkflowInstanceStatus,
  buildWorkflowEntity,
  buildWorkflowInstanceEntity,
} from "@/dynamo/workflow";
import {
  type BaseEntity,
  type ErrorIntervals,
  type WithMetadata,
  buildCreateMetadata,
  findAppropriateErrorMessage,
  findConditionalCheckFailedIndexes,
  generateUpdateExpression,
  isTransactionCanceledException,
} from "@aspira-nextgen/core/dynamodb";
import type { ListDrawWorkflowsInput } from "@aspira-nextgen/graphql/resolvers";
import { ulid } from "ulidx";

type DrawWorkflowEntity = WithMetadata<
  BaseEntity & {
    id: string;
    name: string;
    workflowId: string;
    drawCategoryId: string;
    oneToManyKey: string;
  }
>;

const generateDrawWorkflowDrawCategoryExistsCondition = (input: {
  drawCategoryId: string;
  organizationId: string;
}) =>
  entityExistsConditionCheck({ pk: input.organizationId, sk: input.drawCategoryId, tableName: getWorkflowTableName() });

const buildDrawCategoryWorkflowsOtmPath = otmPathBuilder(DynamoDBPrefix.DRAW_WORKFLOW);

export const createDrawWorkflowEntity = async (input: {
  organizationId: string;
  name: string;
  drawCategoryId: string;
  createdBy: string;
}): Promise<{
  drawWorkflowEntity: DrawWorkflowEntity;
  workflowEntity: WorkflowEntity;
}> => {
  const organizationId = input.organizationId;
  const metadata = buildCreateMetadata(input.createdBy);

  const workflowEntity = buildWorkflowEntity(organizationId, metadata);
  const { id: workflowId } = workflowEntity;

  const drawWorkflowId = `${DynamoDBPrefix.DRAW_WORKFLOW}_${ulid()}`;

  const workflowInstanceEntity = buildWorkflowInstanceEntity({
    organizationId,
    workflowId: workflowId,
    createdBy: input.createdBy,
    status: WorkflowInstanceStatus.BuildNeeded,
  });

  const drawWorkflowEntity: DrawWorkflowEntity = {
    pk: organizationId,
    sk: drawWorkflowId,
    id: drawWorkflowId,
    workflowId: workflowEntity.id,
    ...metadata,
    __typename: DynamoDBTypename.DRAW_WORKFLOW,
    name: input.name,
    drawCategoryId: input.drawCategoryId,
    oneToManyKey: buildDrawCategoryWorkflowsOtmPath({ parentId: input.drawCategoryId, childId: drawWorkflowId }),
  };

  const tableName = getWorkflowTableName();

  try {
    await dynamoDBClient.transactWrite({
      TransactItems: [
        generateDrawWorkflowDrawCategoryExistsCondition({ drawCategoryId: input.drawCategoryId, organizationId }),

        // This is the actual draw workflow entity, which is passed in as an argument.
        {
          Put: {
            TableName: tableName,
            Item: drawWorkflowEntity,
          },
        },
        // This is the workflow entity
        {
          Put: {
            TableName: tableName,
            Item: workflowEntity,
          },
        },
        // this is the workflow instance entity
        {
          Put: {
            TableName: tableName,
            Item: workflowInstanceEntity,
          },
        },
      ],
    });
  } catch (err) {
    if (!isTransactionCanceledException(err)) {
      throw new Error("Failed to create draw workflow entity", { cause: err });
    }

    const failedIndexes = findConditionalCheckFailedIndexes(err);

    if (!failedIndexes || failedIndexes.length === 0) {
      throw new Error("Failed to create draw workflow entity", { cause: err });
    }

    const errorMessage = findAppropriateErrorMessage(failedIndexes, {
      "Provided draw category ID does not exist.": {
        start: 0,
        end: 1,
      },
    });

    throw new Error(errorMessage, { cause: err });
  }

  return {
    drawWorkflowEntity,
    workflowEntity,
  };
};

export const getDrawWorkflowEntity = async (id: {
  drawWorkflowId: string;
  organizationId: string;
}): Promise<DrawWorkflowEntity> => {
  const { organizationId, drawWorkflowId } = id;

  const { Item: drawWorkflowItem } = await dynamoDBClient.get({
    TableName: getWorkflowTableName(),
    Key: {
      pk: organizationId,
      sk: drawWorkflowId,
    },
    ConsistentRead: true,
  });

  const drawWorkflowEntity = drawWorkflowItem as DrawWorkflowEntity;

  if (!drawWorkflowEntity) {
    throw new Error("Draw workflow not found");
  }

  return drawWorkflowEntity;
};

export const updateDrawWorkflowEntity = async (input: {
  id: { drawWorkflowId: string; organizationId: string };
  updates: {
    name?: string;
    drawCategoryId?: string;
  };
  updatedBy: string;
}): Promise<DrawWorkflowEntity> => {
  const {
    id: { organizationId, drawWorkflowId },
    updates,
    updatedBy,
  } = input;

  const updateFields = scrubUpdateFields(updates);

  const entity = await getDrawWorkflowEntity({ drawWorkflowId, organizationId });

  const tableName = getWorkflowTableName();
  const transactItems = [];
  let errorMessageIndices: ErrorIntervals = {};

  if (updateFields.drawCategoryId && entity.drawCategoryId !== updateFields.drawCategoryId) {
    transactItems.push(
      generateDrawWorkflowDrawCategoryExistsCondition({
        drawCategoryId: updateFields.drawCategoryId,
        organizationId,
      }),
    );

    errorMessageIndices = addErrorIntervalByEndExclusive({
      intervals: errorMessageIndices,
      message: "Provided draw category ID does not exist.",
      endExclusive: transactItems.length,
    });
  }

  const updateExpression = generateUpdateExpression({
    fields: updateFields,
    updatedBy,
    table: tableName,
    pk: organizationId,
    sk: drawWorkflowId,
  });
  transactItems.push({
    Update: updateExpression,
  });

  try {
    await dynamoDBClient.transactWrite({
      TransactItems: transactItems,
    });
  } catch (err) {
    if (!isTransactionCanceledException(err)) {
      throw new Error("Failed to update draw workflow entity", { cause: err });
    }

    const failedIndexes = findConditionalCheckFailedIndexes(err);
    if (!failedIndexes || failedIndexes.length === 0) {
      throw new Error("Failed to update draw workflow entity", { cause: err });
    }

    const errorMessage = findAppropriateErrorMessage(failedIndexes, errorMessageIndices);

    throw new Error(errorMessage, { cause: err });
  }

  return await getDrawWorkflowEntity({ drawWorkflowId, organizationId });
};

export const deleteDrawWorkflowEntity = async (input: {
  drawWorkflowId: string;
  organizationId: string;
}): Promise<boolean> => {
  const { organizationId, drawWorkflowId } = input;

  const { Item: drawWorkflowEntity } = await dynamoDBClient.get({
    TableName: getWorkflowTableName(),
    Key: {
      pk: organizationId,
      sk: drawWorkflowId,
    },
    ConsistentRead: true,
  });

  if (!drawWorkflowEntity) {
    return false;
  }

  const tableName = getWorkflowTableName();
  const { workflowId } = drawWorkflowEntity;

  await dynamoDBClient.transactWrite({
    TransactItems: [
      {
        Delete: {
          TableName: tableName,
          Key: {
            pk: organizationId,
            sk: drawWorkflowId,
          },
        },
      },
      {
        Delete: {
          TableName: tableName,
          Key: {
            pk: organizationId,
            sk: workflowId,
          },
        },
      },
    ],
  });

  return true;
};

export const listDrawWorkflowEntitiesForDrawCategory = async (input: {
  drawCategoryId: string;
  organizationId: string;
  paginationRequest: ListDrawWorkflowsInput;
}): Promise<{
  items: DrawWorkflowEntity[];
  nextToken?: string;
}> => {
  const { drawCategoryId, organizationId, paginationRequest } = input;

  try {
    return await queryOneToManyEntitiesWithPagination(
      {
        tableName: getWorkflowTableName(),
        organizationId,
        key: buildDrawCategoryWorkflowsOtmPath({ parentId: drawCategoryId }),
      },
      paginationRequest,
    );
  } catch (err) {
    throw new Error("Failed to list draw workflow entities for draw category", { cause: err });
  }
};

export const listAllDrawWorkflowEntitiesForDrawCategory = async (input: {
  organizationId: string;
  drawCategoryId: string;
  pageSize?: number;
}): Promise<WorkflowEntity[]> => {
  const { organizationId, drawCategoryId, pageSize = 100 } = input;

  try {
    return await queryAllOneToManyEntities(
      {
        tableName: getWorkflowTableName(),
        organizationId,
        key: buildDrawCategoryWorkflowsOtmPath({ parentId: drawCategoryId }),
      },
      pageSize,
    );
  } catch (err) {
    throw new Error("Failed to list all draw workflow entities for draw category", { cause: err });
  }
};

export const deleteDrawWorkflowEntitiesByDrawCategoryId = async (input: {
  organizationId: string;
  drawCategoryId: string;
}) => {
  const { organizationId, drawCategoryId } = input;

  const workflows = await listAllDrawWorkflowEntitiesForDrawCategory({
    drawCategoryId,
    organizationId,
  });
  await deleteEntities(workflows, getWorkflowTableName());
};

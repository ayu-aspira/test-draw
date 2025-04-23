import type { Position } from "@/dynamo/dynamo";
import {
  DynamoDBPrefix,
  DynamoDBTypename,
  deleteEntities,
  dynamoDBClient,
  getWorkflowTableName,
  otmPathBuilder,
  queryAllOneToManyEntities,
  queryOneToManyEntitiesWithPagination,
} from "@/dynamo/dynamo";
import { addErrorIntervalByEndExclusive, generateEntityExistsCondition, scrubUpdateFields } from "@/dynamo/util";
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
import { ulid } from "ulidx";

export type WorkflowEdgeEntity = WithMetadata<
  BaseEntity & {
    id: string;
    workflowId: string;
    workflowInstanceId: string;
    oneToManyKey: string;
    sourceNodeId: string;
    sourceHandlePosition?: Position | null;
    targetNodeId: string;
    targetHandlePosition?: Position | null;
  }
>;

export const createWorkflowEdgeEntity = async (input: {
  workflowId: string;
  workflowInstanceId: string;
  sourceNodeId: string;
  sourceHandlePosition?: Position | null;
  sourceHandle?: string | null;
  targetNodeId: string;
  targetHandlePosition?: Position | null;
  targetHandle?: string | null;
  createdBy: string;
  organizationId: string;
}): Promise<WorkflowEdgeEntity> => {
  const { organizationId, createdBy, workflowInstanceId, ...rest } = input;

  const id = `${DynamoDBPrefix.WORKFLOW_EDGE}_${ulid()}`;
  const metadata = buildCreateMetadata(createdBy);
  const entity: WorkflowEdgeEntity = {
    __typename: DynamoDBTypename.WORKFLOW_EDGE,
    pk: organizationId,
    sk: id,
    id,
    ...metadata,
    oneToManyKey: buildWorkflowEdgeOtmPath({ parentId: workflowInstanceId, childId: id }),
    ...rest,
    workflowInstanceId,
  };

  const workflowTableName = getWorkflowTableName();

  try {
    await dynamoDBClient.transactWrite({
      TransactItems: [
        generateEntityExistsCondition({ entityId: workflowInstanceId, organizationId, table: workflowTableName }),
        generateEntityExistsCondition({ entityId: input.sourceNodeId, organizationId, table: workflowTableName }),
        generateEntityExistsCondition({ entityId: input.targetNodeId, organizationId, table: workflowTableName }),
        {
          Put: {
            TableName: workflowTableName,
            Item: entity,
          },
        },
      ],
    });
  } catch (err) {
    if (!isTransactionCanceledException(err)) {
      throw new Error("Failed to create workflow edge", { cause: err });
    }

    const failedIndexes = findConditionalCheckFailedIndexes(err);

    if (!failedIndexes || failedIndexes.length === 0) {
      throw new Error("Failed to create workflow edge", { cause: err });
    }

    const errorMessage = findAppropriateErrorMessage(failedIndexes, {
      "Provided workflow instance ID does not exist.": {
        start: 0,
        end: 1,
      },
      "Provided source node ID does not exist.": {
        start: 1,
        end: 2,
      },
      "Provided target node ID does not exist.": {
        start: 2,
        end: 3,
      },
    });

    throw new Error(errorMessage, { cause: err });
  }

  return entity;
};

export const getWorkflowEdgeEntity = async (input: {
  organizationId: string;
  workflowEdgeId: string;
}): Promise<WorkflowEdgeEntity> => {
  const { organizationId, workflowEdgeId } = input;

  const { Item } = await dynamoDBClient.get({
    TableName: getWorkflowTableName(),
    Key: {
      pk: organizationId,
      sk: workflowEdgeId,
    },
    ConsistentRead: true,
  });

  if (!Item) {
    throw new Error("Workflow edge not found");
  }

  return Item as WorkflowEdgeEntity;
};

export const updateWorkflowEdgeEntity = async (input: {
  id: {
    organizationId: string;
    workflowEdgeId: string;
  };
  updates: {
    sourceNodeId?: string;
    sourceHandlePosition?: Position | null;
    sourceHandle?: string | null;
    targetNodeId?: string;
    targetHandlePosition?: Position | null;
    targetHandle?: string | null;
  };
  updatedBy: string;
}): Promise<WorkflowEdgeEntity> => {
  const {
    id: { organizationId, workflowEdgeId },
    updates,
    updatedBy,
  } = input;

  const transactItems = [];
  let errorMessageIndices: ErrorIntervals = {};

  const workflowTableName = getWorkflowTableName();

  if (updates.sourceNodeId) {
    const end = transactItems.push(
      generateEntityExistsCondition({ entityId: updates.sourceNodeId, organizationId, table: workflowTableName }),
    );
    errorMessageIndices = addErrorIntervalByEndExclusive({
      intervals: errorMessageIndices,
      message: "Provided source node ID does not exist.",
      endExclusive: end,
    });
  }

  if (updates.targetNodeId) {
    const end = transactItems.push(
      generateEntityExistsCondition({ entityId: updates.targetNodeId, organizationId, table: workflowTableName }),
    );
    errorMessageIndices = addErrorIntervalByEndExclusive({
      intervals: errorMessageIndices,
      message: "Provided target node ID does not exist.",
      endExclusive: end,
    });
  }

  transactItems.push({
    Update: generateUpdateExpression({
      fields: scrubUpdateFields(updates),
      table: workflowTableName,
      pk: organizationId,
      sk: workflowEdgeId,
      updatedBy,
    }),
  });

  try {
    await dynamoDBClient.transactWrite({
      TransactItems: transactItems,
    });
    return await getWorkflowEdgeEntity({ organizationId, workflowEdgeId });
  } catch (err) {
    if (!isTransactionCanceledException(err)) {
      throw new Error("Failed to update workflow edge entity", { cause: err });
    }

    const failedIndexes = findConditionalCheckFailedIndexes(err);

    if (!failedIndexes || failedIndexes.length === 0) {
      throw new Error("Failed to update workflow edge entity", { cause: err });
    }

    const errorMessage = findAppropriateErrorMessage(failedIndexes, errorMessageIndices);

    throw new Error(errorMessage, { cause: err });
  }
};

export const deleteWorkflowEdgeEntity = async (input: {
  workflowEdgeId: string;
  organizationId: string;
}): Promise<void> => {
  const { organizationId, workflowEdgeId } = input;

  try {
    await dynamoDBClient.transactWrite({
      TransactItems: [
        {
          Delete: {
            TableName: getWorkflowTableName(),
            Key: {
              pk: organizationId,
              sk: workflowEdgeId,
            },
          },
        },
      ],
    });
  } catch (err) {
    throw new Error("Failed to delete workflow edge entity.", { cause: err });
  }
};

export const listWorkflowEdgeEntitiesByWorkflowInstanceId = async (input: {
  organizationId: string;
  workflowInstanceId: string;
  paginationRequest?: {
    limit?: number | null;
    nextToken?: string | null;
  };
}): Promise<{ items: WorkflowEdgeEntity[]; nextToken?: string }> => {
  const { organizationId, workflowInstanceId, paginationRequest } = input;

  try {
    return await queryOneToManyEntitiesWithPagination(
      {
        tableName: getWorkflowTableName(),
        organizationId,
        key: buildWorkflowEdgeOtmPath({ parentId: workflowInstanceId }),
      },
      paginationRequest,
    );
  } catch (err) {
    throw new Error("Failed to get workflow edges.", { cause: err });
  }
};

export const listAllWorkflowEdgeEntitiesByWorkflowInstanceId = async (input: {
  organizationId: string;
  workflowInstanceId: string;
  pageSize?: number;
}): Promise<WorkflowEdgeEntity[]> => {
  const { organizationId, workflowInstanceId, pageSize = 100 } = input;

  try {
    return await queryAllOneToManyEntities(
      {
        tableName: getWorkflowTableName(),
        organizationId,
        key: buildWorkflowEdgeOtmPath({ parentId: workflowInstanceId }),
      },
      pageSize,
    );
  } catch (err) {
    throw new Error("Failed to get all workflow edges.", { cause: err });
  }
};

export const deleteWorkflowEdgeEntities = async (workflowEdges: WorkflowEdgeEntity[]): Promise<void> => {
  try {
    await deleteEntities(workflowEdges, getWorkflowTableName());
  } catch (err) {
    throw new Error("Failed to delete workflow edge entities.", { cause: err });
  }
};

const buildWorkflowEdgeOtmPath = otmPathBuilder(DynamoDBPrefix.WORKFLOW_EDGE);

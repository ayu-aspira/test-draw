import {
  DynamoDBIndex,
  DynamoDBPrefix,
  DynamoDBTypename,
  deleteEntities,
  dynamoDBClient,
  getWorkflowTableName,
  queryAllOneToManyEntities,
  queryOneToManyEntitiesWithPagination,
} from "@/dynamo/dynamo";
import {
  type BaseEntity,
  type CreateMetadata,
  type WithMetadata,
  buildCreateMetadata,
  chunkBatchGetItems,
  generateUpdateExpression,
} from "@aspira-nextgen/core/dynamodb";
import { ulid } from "ulidx";

export type WorkflowEntity = WithMetadata<
  BaseEntity & {
    id: string;
  }
>;

export enum WorkflowInstanceStatus {
  /**
   * Indicates that the workflow instance has changed
   * and requires a new build.
   */
  BuildNeeded = "BUILD_NEEDED",

  /**
   * Indicates that the workflow instance build has started.
   */
  BuildStarted = "BUILD_STARTED",

  /**
   * Indicates that the workflow instance build has failed.
   */
  BuildFailed = "BUILD_FAILED",

  /**
   * Indicates that the workflow instance is ready for
   * executions.
   */
  Ready = "READY",
}

export type WorkflowInstanceEntity = WithMetadata<
  BaseEntity & {
    id: string;
    workflowId: string;
    aslS3Key?: string;
    stateMachineArn?: string;
    status: WorkflowInstanceStatus;
    oneToManyKey: string;
  }
>;

/**
 * Creates a new workflow entity. It is the responsibility of the caller to persist this entity
 * to ensure transactional integrity.
 *
 * @param organizationId
 * @param createMetadata
 * @returns
 */
export const buildWorkflowEntity = (organizationId: string, createMetadata: CreateMetadata): WorkflowEntity => {
  const id = `${DynamoDBPrefix.WORKFLOW}_${ulid()}`;

  return {
    pk: organizationId,
    sk: id,
    id: id,
    ...createMetadata,
    __typename: DynamoDBTypename.WORKFLOW,
  };
};

export const getWorkflowEntity = async (input: {
  organizationId: string;
  workflowId: string;
}): Promise<WorkflowEntity> => {
  const { organizationId, workflowId } = input;

  const { Item } = await dynamoDBClient.get({
    TableName: getWorkflowTableName(),
    Key: {
      pk: organizationId,
      sk: workflowId,
    },
    ConsistentRead: true,
  });

  if (!Item) {
    throw new Error("Workflow not found");
  }

  return Item as WorkflowEntity;
};

/**
 * Creates a new workflow instance entity. This function persists the entity to DynamoDB
 * under the parent workflow ID. The underlying entity is created in a non-ready state.
 * The workflow creator state machine will handle building the workflow instance and
 * updating the entity to a ready state.
 *
 * @param input
 * @returns
 */
export const buildWorkflowInstanceEntity = (input: {
  workflowId: string;
  organizationId: string;
  createdBy: string;
  status: WorkflowInstanceStatus;
}): WorkflowInstanceEntity => {
  const { workflowId, organizationId, createdBy, status } = input;
  const id = `${DynamoDBPrefix.WORKFLOW_INSTANCE}_${ulid()}`;

  return {
    pk: organizationId,
    sk: id,
    id,
    workflowId: workflowId,
    status,
    __typename: DynamoDBTypename.WORKFLOW_INSTANCE,
    oneToManyKey: buildWorkflowInstanceOtmPath({ workflowId: input.workflowId, workflowInstanceId: id }),
    ...buildCreateMetadata(createdBy),
  };
};

export const getWorkflowInstanceEntity = async (input: {
  organizationId: string;
  workflowInstanceId: string;
}): Promise<WorkflowInstanceEntity> => {
  const { organizationId, workflowInstanceId } = input;

  const { Item } = await dynamoDBClient.get({
    TableName: getWorkflowTableName(),
    Key: {
      pk: organizationId,
      sk: workflowInstanceId,
    },
    ConsistentRead: true,
  });

  if (!Item) {
    throw new Error("Workflow instance not found");
  }

  return Item as WorkflowInstanceEntity;
};

export const getLatestWorkflowInstanceEntity = async (input: {
  organizationId: string;
  workflowId: string;
}): Promise<WorkflowInstanceEntity | undefined> => {
  const { organizationId, workflowId } = input;

  const { Items } = await dynamoDBClient.query({
    TableName: getWorkflowTableName(),
    IndexName: DynamoDBIndex.ONE_TO_MANY,
    KeyConditionExpression: "pk = :pk and begins_with(oneToManyKey, :otmKey)",
    ExpressionAttributeValues: {
      ":pk": organizationId,
      ":otmKey": buildWorkflowInstanceOtmPath({ workflowId }),
    },
    ScanIndexForward: false,
    Limit: 1,
  });

  return Items?.[0] as WorkflowInstanceEntity;
};

export const listWorkflowInstanceEntitiesByWorkflowId = async (input: {
  organizationId: string;
  workflowId: string;
  paginationRequest?: {
    limit?: number | null;
    nextToken?: string | null;
  };
}): Promise<{ items: WorkflowInstanceEntity[]; nextToken?: string }> => {
  const { organizationId, workflowId, paginationRequest } = input;

  try {
    return await queryOneToManyEntitiesWithPagination(
      {
        tableName: getWorkflowTableName(),
        organizationId,
        key: buildWorkflowInstanceOtmPath({ workflowId }),
      },
      paginationRequest,
    );
  } catch (err) {
    throw new Error("Failed to list workflow instance entities.", { cause: err });
  }
};

export const listAllWorkflowInstanceEntitiesByWorkflowId = async (input: {
  organizationId: string;
  workflowId: string;
  pageSize?: number;
}): Promise<WorkflowInstanceEntity[]> => {
  const { organizationId, workflowId, pageSize = 100 } = input;

  try {
    return await queryAllOneToManyEntities(
      { tableName: getWorkflowTableName(), organizationId, key: buildWorkflowInstanceOtmPath({ workflowId }) },
      pageSize,
    );
  } catch (err) {
    throw new Error("Failed to list all workflow instances.", { cause: err });
  }
};

export const deleteWorkflowInstanceEntities = async (workflowInstances: WorkflowInstanceEntity[]): Promise<void> => {
  try {
    await deleteEntities(workflowInstances, getWorkflowTableName());
  } catch (err) {
    throw new Error("Failed to delete workflow instance entities.", { cause: err });
  }
};

export const updateWorkflowInstanceEntity = async (input: {
  id: {
    workflowInstanceId: string;
    organizationId: string;
  };
  updates: {
    aslS3Key?: string;
    stateMachineArn?: string;
    status?: WorkflowInstanceStatus;
  };
  updatedBy: string;
}): Promise<void> => {
  const {
    id: { workflowInstanceId, organizationId },
    updates,
    updatedBy,
  } = input;

  if ("status" in updates && !updates.status) {
    throw new Error("If providing the status key, it must have a value.");
  }

  const updateExpression = generateUpdateExpression({
    fields: updates,
    table: getWorkflowTableName(),
    pk: organizationId,
    sk: workflowInstanceId,
    updatedBy,
  });

  try {
    await dynamoDBClient.update(updateExpression);
  } catch (err) {
    throw new Error("Failed to update workflow instance entity.", { cause: err });
  }
};

const buildWorkflowInstanceOtmPath = (input: { workflowId: string; workflowInstanceId?: string }): string => {
  return `${input.workflowId}#${input.workflowInstanceId ?? DynamoDBPrefix.WORKFLOW_INSTANCE}`;
};

export const batchGetWorkflowEntities = async (input: { organizationId: string; workflowIds: string[] }): Promise<
  WorkflowEntity[]
> => {
  const { organizationId, workflowIds } = input;
  const keys = workflowIds.map((workflowId) => ({ pk: organizationId, sk: workflowId }));

  const workflowTableName = getWorkflowTableName();

  const chunks = chunkBatchGetItems(keys);
  const results = [];

  try {
    for (const chunk of chunks) {
      const result = await dynamoDBClient.batchGet({
        RequestItems: {
          [workflowTableName]: {
            Keys: chunk,
            ConsistentRead: true,
          },
        },
      });

      results.push(...(result.Responses?.[workflowTableName] as WorkflowEntity[]));
    }

    return results;
  } catch (err) {
    throw new Error("Failed to batch get workflows.", { cause: err });
  }
};

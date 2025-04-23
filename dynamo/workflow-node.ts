import type { Position } from "@/dynamo/dynamo";
import {
  DynamoDBPrefix,
  deleteEntities,
  dynamoDBClient,
  getWorkflowTableName,
  otmPathBuilder,
  queryAllEntities,
  queryAllOneToManyEntities,
  queryOneToManyEntitiesWithPagination,
} from "@/dynamo/dynamo";
import { generateEntityExistsCondition, scrubUpdateFields } from "@/dynamo/util";
import {
  type BaseEntity,
  type WithMetadata,
  buildCreateMetadata,
  findAppropriateErrorMessage,
  findConditionalCheckFailedIndexes,
  generateUpdateExpression,
  isTransactionCanceledException,
} from "@aspira-nextgen/core/dynamodb";
import { ulid } from "ulidx";

export type WorkflowNodeEntity = WithMetadata<
  BaseEntity & {
    id: string;
    workflowId: string;
    workflowInstanceId: string;
    oneToManyKey: string;
    position?: Position | null;
  }
>;

export const createWorkflowNodeEntity = async (input: {
  workflowId: string;
  workflowInstanceId: string;
  type: string;
  position?: Position | null;
  createdBy: string;
  organizationId: string;
}): Promise<WorkflowNodeEntity> => {
  const { organizationId, createdBy, workflowInstanceId, type, ...rest } = input;

  const id = `${DynamoDBPrefix.WORKFLOW_NODE}_${ulid()}`;
  const metadata = buildCreateMetadata(createdBy);
  const entity: WorkflowNodeEntity = {
    pk: organizationId,
    sk: id,
    id,
    ...metadata,
    oneToManyKey: buildWorkflowNodeOtmPath({ parentId: workflowInstanceId, childId: id }),
    ...rest,
    workflowInstanceId,
    __typename: type,
  };

  try {
    await dynamoDBClient.transactWrite({
      TransactItems: [
        generateEntityExistsCondition({ entityId: workflowInstanceId, organizationId, table: getWorkflowTableName() }),
        {
          Put: {
            TableName: getWorkflowTableName(),
            Item: entity,
          },
        },
      ],
    });
  } catch (err) {
    if (!isTransactionCanceledException(err)) {
      throw new Error("Failed to create workflow node", { cause: err });
    }

    const failedIndexes = findConditionalCheckFailedIndexes(err);

    if (!failedIndexes || failedIndexes.length === 0) {
      throw new Error("Failed to create workflow node", { cause: err });
    }

    const errorMessage = findAppropriateErrorMessage(failedIndexes, {
      "Provided workflow ID does not exist.": {
        start: 0,
        end: 1,
      },
    });

    throw new Error(errorMessage, { cause: err });
  }

  return entity;
};

export const getWorkflowNodeEntity = async (input: {
  organizationId: string;
  workflowNodeId: string;
}): Promise<WorkflowNodeEntity | undefined> => {
  const { organizationId, workflowNodeId } = input;

  const { Item } = await dynamoDBClient.get({
    TableName: getWorkflowTableName(),
    Key: {
      pk: organizationId,
      sk: workflowNodeId,
    },
    ConsistentRead: true,
  });

  return Item as WorkflowNodeEntity | undefined;
};

export const updateWorkflowNodeEntity = async (input: {
  id: {
    organizationId: string;
    workflowNodeId: string;
  };
  updates: {
    position?: Position | null;
  };
  updatedBy: string;
}): Promise<WorkflowNodeEntity> => {
  const {
    id: { organizationId, workflowNodeId },
    updates,
    updatedBy,
  } = input;

  const updateExpression = generateUpdateExpression({
    fields: scrubUpdateFields(updates),
    table: getWorkflowTableName(),
    pk: organizationId,
    sk: workflowNodeId,
    updatedBy,
  });

  try {
    await dynamoDBClient.update(updateExpression);
  } catch (err) {
    throw new Error("Failed to update workflow node entity.", { cause: err });
  }

  // Casting because it has to exist at this point.
  return (await getWorkflowNodeEntity({ organizationId, workflowNodeId })) as WorkflowNodeEntity;
};

export const deleteWorkflowNodeEntity = async (input: {
  workflowNodeId: string;
  organizationId: string;
}): Promise<void> => {
  const { organizationId, workflowNodeId } = input;

  try {
    await dynamoDBClient.transactWrite({
      TransactItems: [
        {
          Delete: {
            TableName: getWorkflowTableName(),
            Key: {
              pk: organizationId,
              sk: workflowNodeId,
            },
          },
        },
      ],
    });
  } catch (err) {
    throw new Error("Failed to delete workflow node entity.", { cause: err });
  }
};

export const listWorkflowNodeEntitiesByWorkflowInstanceId = async (input: {
  organizationId: string;
  workflowInstanceId: string;
  paginationRequest?: {
    limit?: number | null;
    nextToken?: string | null;
  };
}): Promise<{ items: WorkflowNodeEntity[]; nextToken?: string }> => {
  const { organizationId, workflowInstanceId, paginationRequest } = input;

  try {
    return await queryOneToManyEntitiesWithPagination(
      {
        tableName: getWorkflowTableName(),
        organizationId,
        key: buildWorkflowNodeOtmPath({ parentId: workflowInstanceId }),
      },
      paginationRequest,
    );
  } catch (err) {
    throw new Error("Failed to get workflow node.", { cause: err });
  }
};

export const listAllWorkflowNodeEntitiesByWorkflowInstanceId = async (input: {
  organizationId: string;
  workflowInstanceId: string;
  pageSize?: number;
}): Promise<WorkflowNodeEntity[]> => {
  const { organizationId, workflowInstanceId, pageSize = 100 } = input;

  try {
    return await queryAllOneToManyEntities(
      {
        tableName: getWorkflowTableName(),
        organizationId,
        key: buildWorkflowNodeOtmPath({ parentId: workflowInstanceId }),
      },
      pageSize,
    );
  } catch (err) {
    throw new Error("Failed to list workflow nodes.", { cause: err });
  }
};

export const getItemsByPrefix = async (pk: string, skPrefix: string): Promise<WorkflowNodeEntity[]> => {
  const query = {
    TableName: getWorkflowTableName(),
    KeyConditionExpression: "pk = :pk and begins_with(sk, :skPrefix)",
    ExpressionAttributeValues: {
      ":pk": pk,
      ":skPrefix": skPrefix,
    },
  };
  try {
    return await queryAllEntities(query);
  } catch (err) {
    throw new Error("Failed to get items by prefix", { cause: err });
  }
};

export const deleteWorkflowNodeEntities = async (workflowNodes: WorkflowNodeEntity[]): Promise<void> => {
  let allEntities: BaseEntity[] = [];

  for (const entity of workflowNodes) {
    const entities = await getItemsByPrefix(entity.pk, entity.sk);
    allEntities = allEntities.concat(entities as BaseEntity[]);
  }
  allEntities = allEntities.concat(workflowNodes);

  try {
    await deleteEntities(allEntities, getWorkflowTableName());
  } catch (err) {
    throw new Error("Failed to delete workflow instance entities.", { cause: err });
  }
};

const buildWorkflowNodeOtmPath = otmPathBuilder(DynamoDBPrefix.WORKFLOW_NODE);

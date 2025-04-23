import {
  DynamoDBIndex,
  DynamoDBPrefix,
  DynamoDBTypename,
  dynamoDBClient,
  getWorkflowTableName,
  otmPathBuilder,
  queryAllOneToManyEntities,
} from "@/dynamo/dynamo";
import { generateEntityExistsCondition, scrubUpdateFields } from "@/dynamo/util";
import {
  type BaseEntity,
  type WithMetadata,
  buildCreateMetadata,
  generateUpdateExpression,
  isTransactionConditionalCheckFailed,
} from "@aspira-nextgen/core/dynamodb";
import type { WorkflowJobLogParam, WorkflowLogLevel, WorkflowMessageKey } from "@aspira-nextgen/graphql/resolvers";
import { ulid } from "ulidx";

/**
 * Represents a log message for a workflow job.
 */
export type WorkflowJobLogEntity = WithMetadata<
  BaseEntity<DynamoDBTypename.WORKFLOW_JOB_LOG> & {
    id: string;
    oneToManyKey: string;
    workflowJobId: string;
    workflowNodeId?: string;
    level: WorkflowLogLevel;
    messageKey: WorkflowMessageKey;
    messageParams?: WorkflowJobLogParam[];
  }
>;

/**
 * Represents a job that is created when a workflow instance is executed.
 */
export type WorkflowJobEntity = WithMetadata<
  BaseEntity & {
    id: string;
    workflowInstanceId: string;
    executionArn?: string;

    /**
     * A flag indicating that the pre-execution steps to kick off a State Machine Execution have failed.
     * This is primarily used to determine if the workflow job should be considered failed before we have
     * an execution ARN. If the execution ARN is present, we should use the status of the execution in AWS
     * Step Functions.
     */
    preExecutionFailure?: boolean;

    oneToManyKey: string;
  }
>;

export const createWorkflowJobEntity = async (input: {
  organizationId: string;
  workflowInstanceId: string;
  executionArn?: string;
  createdBy: string;
}): Promise<WorkflowJobEntity> => {
  const { organizationId, workflowInstanceId, executionArn, createdBy } = input;

  const id = `${DynamoDBPrefix.WORKFLOW_JOB}_${ulid()}`;

  const workflowJobEntity: WorkflowJobEntity = {
    pk: organizationId,
    sk: id,
    id: id,
    workflowInstanceId,
    executionArn,
    oneToManyKey: buildWorkflowJobOtmPath({ workflowInstanceId, workflowJobId: id }),
    __typename: DynamoDBTypename.WORKFLOW_JOB,
    ...buildCreateMetadata(createdBy),
  };

  const workflowTableName = getWorkflowTableName();

  const putEntity = {
    Put: {
      TableName: getWorkflowTableName(),
      Item: workflowJobEntity,
    },
  };

  try {
    await dynamoDBClient.transactWrite({
      TransactItems: [
        generateEntityExistsCondition({ entityId: workflowInstanceId, organizationId, table: workflowTableName }),
        putEntity,
      ],
    });
  } catch (err) {
    if (isTransactionConditionalCheckFailed(err)) {
      throw new Error(`Workflow instance ${workflowInstanceId} not found.`);
    }

    throw new Error("Failed to create workflow job entity.", { cause: err });
  }

  return workflowJobEntity;
};

export const updateWorkflowJobEntity = async (input: {
  id: {
    organizationId: string;
    workflowJobId: string;
  };
  updates: {
    executionArn?: string;
    preExecutionFailure?: boolean;
  };
  updatedBy: string;
}): Promise<void> => {
  const { organizationId, workflowJobId } = input.id;

  const updateFields = scrubUpdateFields(input.updates);

  if (!Object.keys(updateFields).length) {
    return;
  }

  const updateExpression = generateUpdateExpression({
    table: getWorkflowTableName(),
    pk: organizationId,
    sk: workflowJobId,
    updatedBy: input.updatedBy,
    fields: updateFields,
  });

  try {
    await dynamoDBClient.update(updateExpression);
  } catch (err) {
    throw new Error("Failed to update workflow job entity.", { cause: err });
  }
};

export const getWorkflowJobEntity = async (input: {
  organizationId: string;
  workflowJobId: string;
}): Promise<WorkflowJobEntity> => {
  const { organizationId, workflowJobId } = input;

  const { Item } = await dynamoDBClient.get({
    TableName: getWorkflowTableName(),
    Key: {
      pk: organizationId,
      sk: workflowJobId,
    },
    ConsistentRead: true,
  });

  if (!Item) {
    throw new Error("Workflow job not found");
  }

  return Item as WorkflowJobEntity;
};

export const getLatestWorkflowJobEntity = async (input: {
  organizationId: string;
  workflowInstanceId: string;
}): Promise<WorkflowJobEntity | null> => {
  const { organizationId, workflowInstanceId } = input;

  const { Items } = await dynamoDBClient.query({
    TableName: getWorkflowTableName(),
    IndexName: DynamoDBIndex.ONE_TO_MANY,
    KeyConditionExpression: "pk = :pk AND begins_with(oneToManyKey, :otmKey)",
    ExpressionAttributeValues: {
      ":pk": organizationId,
      ":otmKey": buildWorkflowJobOtmPath({ workflowInstanceId }),
    },
    ScanIndexForward: false,
    Limit: 1,
  });

  return (Items?.[0] as WorkflowJobEntity) || null;
};

export const createWorkflowJobLogEntity = async (input: {
  organizationId: string;
  workflowJobId: string;
  workflowNodeId?: string;
  level: WorkflowLogLevel;
  messageKey: WorkflowMessageKey;
  messageParams?: WorkflowJobLogParam[];
  createdBy: string;
}): Promise<WorkflowJobLogEntity> => {
  const { organizationId, workflowJobId, workflowNodeId, level, messageKey, messageParams, createdBy } = input;

  const id = `${DynamoDBPrefix.WORKFLOW_JOB_LOG}_${ulid()}`;
  const entity: WorkflowJobLogEntity = {
    __typename: DynamoDBTypename.WORKFLOW_JOB_LOG,
    pk: organizationId,
    sk: id,
    id,
    ...buildCreateMetadata(createdBy),
    oneToManyKey: buildWorkflowJobLogOtmPath({ parentId: workflowJobId, childId: id }),
    workflowJobId,
    workflowNodeId,
    level,
    messageKey,
    messageParams: messageParams || [],
  };

  await dynamoDBClient.put({
    TableName: getWorkflowTableName(),
    Item: entity,
  });

  return entity;
};

export const getWorkflowJobLogEntities = async (input: {
  organizationId: string;
  workflowJobId: string;
}): Promise<WorkflowJobLogEntity[]> => {
  const { organizationId, workflowJobId } = input;

  const entities = await queryAllOneToManyEntities<WorkflowJobLogEntity>({
    tableName: getWorkflowTableName(),
    organizationId,
    key: buildWorkflowJobLogOtmPath({ parentId: workflowJobId }),
  });

  return entities as WorkflowJobLogEntity[];
};

const buildWorkflowJobOtmPath = (input: { workflowInstanceId: string; workflowJobId?: string }): string =>
  `${input.workflowInstanceId}#${input.workflowJobId ?? DynamoDBPrefix.WORKFLOW_JOB}`;

const buildWorkflowJobLogOtmPath = otmPathBuilder(DynamoDBPrefix.WORKFLOW_JOB_LOG);

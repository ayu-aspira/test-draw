import {
  DynamoDBPrefix,
  DynamoDBTypename,
  dynamoDBClient,
  getWorkflowTableName,
  paginateOverOneToManyEntities,
} from "@/dynamo/dynamo";
import { generateEntityExistsCondition } from "@/dynamo/util";
import type { WorkflowNodeData } from "@/step-function/workflow-types";
import { type BaseEntity, type WithMetadata, buildCreateMetadata } from "@aspira-nextgen/core/dynamodb";
import { ulid } from "ulidx";

/**
 * Represents the result of a workflow node's execution.
 * These are saved in case we need to fetch the results
 * for a given node within a specific job.
 */
export type WorkflowNodeResultEntity = WithMetadata<
  BaseEntity & {
    id: string;
    workflowJobId: string;
    nodeId: string;
    results: WorkflowNodeData[];
    oneToManyKey: string;
  }
>;

export const createWorkflowNodeResultEntity = async (input: {
  organizationId: string;
  workflowJobId: string;
  nodeId: string;
  results: WorkflowNodeData[];
  createdBy: string;
}): Promise<WorkflowNodeResultEntity> => {
  const { organizationId, workflowJobId, nodeId, createdBy } = input;
  const id = `${DynamoDBPrefix.WORKFLOW_NODE_RESULT}_${ulid()}`;

  const workflowNodeResultEntity: WorkflowNodeResultEntity = {
    pk: organizationId,
    sk: id,
    id,
    workflowJobId,
    nodeId,
    results: input.results,
    oneToManyKey: buildWorkflowNodeResultOtmPath({ workflowJobId, workflowNodeResultId: id }),
    __typename: DynamoDBTypename.WORKFLOW_NODE_RESULT,
    ...buildCreateMetadata(createdBy),
  };

  const workflowTableName = getWorkflowTableName();

  const putEntity = {
    Put: {
      TableName: workflowTableName,
      Item: workflowNodeResultEntity,
    },
  };

  try {
    await dynamoDBClient.transactWrite({
      TransactItems: [
        generateEntityExistsCondition({ entityId: workflowJobId, organizationId, table: workflowTableName }),
        putEntity,
      ],
    });
  } catch (err) {
    throw new Error("Failed to create workflow node result entity.", { cause: err });
  }

  return workflowNodeResultEntity;
};

export const getWorkflowNodeResultEntity = async (input: {
  organizationId: string;
  workflowNodeResultId: string;
}): Promise<WorkflowNodeResultEntity | null> => {
  const { organizationId, workflowNodeResultId } = input;

  const { Item: entity } = await dynamoDBClient.get({
    TableName: getWorkflowTableName(),
    Key: {
      pk: organizationId,
      sk: workflowNodeResultId,
    },
  });

  if (!entity) {
    return null;
  }

  return entity as WorkflowNodeResultEntity;
};

export const listWorkflowNodeResultEntities = async (input: {
  organizationId: string;
  workflowJobId: string;
  pageSize?: number;
}): Promise<WorkflowNodeResultEntity[]> => {
  const { organizationId, workflowJobId, pageSize = 100 } = input;

  const paginator = paginateOverOneToManyEntities<WorkflowNodeResultEntity>(
    {
      tableName: getWorkflowTableName(),
      organizationId,
      key: buildWorkflowNodeResultOtmPath({ workflowJobId }),
    },
    pageSize,
  );

  const results: WorkflowNodeResultEntity[] = [];

  for await (const page of paginator) {
    results.push(...page);
  }

  return results;
};

const buildWorkflowNodeResultOtmPath = (input: { workflowJobId: string; workflowNodeResultId?: string }): string =>
  `${input.workflowJobId}#${input.workflowNodeResultId ?? DynamoDBPrefix.WORKFLOW_NODE_RESULT}`;

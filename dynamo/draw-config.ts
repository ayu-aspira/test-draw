import {
  DynamoDBPrefix,
  DynamoDBTypename,
  dynamoDBClient,
  entityExistsConditionCheck,
  getDrawTableName,
} from "@/dynamo/dynamo";
import { scrubUpdateFields } from "@/dynamo/util";
import type { BaseEntity, WithMetadata } from "@aspira-nextgen/core/dynamodb";
import {
  buildCreateMetadata,
  generateUpdateExpression,
  isTransactionConditionalCheckFailed,
} from "@aspira-nextgen/core/dynamodb";
import type { DrawConfigQuotaRule } from "@aspira-nextgen/graphql/resolvers";
import { ulid } from "ulidx";

export type DrawConfigEntity = WithMetadata<
  BaseEntity<DynamoDBTypename.DRAW_CONFIG> & {
    id: string;
    name: string;
    usePoints: boolean;
    sortId?: string;
    quotaRuleFlags?: DrawConfigQuotaRule[];
    applicants?: string[];
  }
>;

type DrawConfigWorkflowNodeLinkEntity = WithMetadata<
  BaseEntity<DynamoDBTypename.DRAW_CONFIG_WORKFLOW_NODE_LINK> & {
    workflowNodeId: string;
    drawConfigId: string;
  }
>;

const generateDrawConfigWorkflowNodeLinkSk = ({ workflowNodeId }: { workflowNodeId: string }) =>
  `${workflowNodeId}#drawConfig`;

const generateDrawSortExistsConditionCheck = (input: { organizationId: string; sortId: string }) =>
  entityExistsConditionCheck({ pk: input.organizationId, sk: input.sortId, tableName: getDrawTableName() });

export const createDrawConfigEntity = async (input: {
  organizationId: string;
  workflowNodeId: string;
  createdBy: string;
  name: string;
  usePoints: boolean;
  sortId?: string;
  quotaRuleFlags: DrawConfigQuotaRule[];
  applicants?: string[];
}): Promise<DrawConfigEntity> => {
  const { createdBy, organizationId, workflowNodeId, sortId, ...fields } = input;

  const drawConfigId = `${DynamoDBPrefix.DRAW_CONFIG}_${ulid()}`;
  const metadata = buildCreateMetadata(createdBy);

  const transactItems = [];

  if (sortId) {
    transactItems.push(generateDrawSortExistsConditionCheck({ organizationId, sortId }));
  }

  const entity: DrawConfigEntity = {
    pk: organizationId,
    sk: drawConfigId,
    id: drawConfigId,
    __typename: DynamoDBTypename.DRAW_CONFIG,
    sortId,
    ...metadata,
    ...fields,
  };

  const link: DrawConfigWorkflowNodeLinkEntity = {
    pk: organizationId,
    sk: generateDrawConfigWorkflowNodeLinkSk({ workflowNodeId }),
    __typename: DynamoDBTypename.DRAW_CONFIG_WORKFLOW_NODE_LINK,
    ...metadata,
    workflowNodeId,
    drawConfigId,
  };

  const drawTableName = getDrawTableName();
  transactItems.push(
    {
      Put: {
        TableName: drawTableName,
        Item: entity,
      },
    },
    {
      Put: {
        TableName: drawTableName,
        Item: link,
      },
    },
  );

  try {
    await dynamoDBClient.transactWrite({
      TransactItems: transactItems,
    });
  } catch (err) {
    if (!isTransactionConditionalCheckFailed(err)) {
      throw new Error("Failed to create draw config entity", { cause: err });
    }

    throw new Error(`Draw sort with ID ${sortId} does not exist.`);
  }

  return entity as DrawConfigEntity;
};

export const updateDrawConfigEntity = async (input: {
  id: {
    organizationId: string;
    drawConfigId: string;
  };
  updates: {
    name?: string;
    usePoints?: boolean;
    sortId?: string;
    quotaRuleFlags?: DrawConfigQuotaRule[];
    applicants?: string[];
  };
  updatedBy: string;
}): Promise<DrawConfigEntity> => {
  const {
    id: { organizationId, drawConfigId },
    updates,
    updatedBy,
  } = input;

  const transactItems = [];

  if (updates.sortId) {
    transactItems.push(generateDrawSortExistsConditionCheck({ organizationId, sortId: updates.sortId }));
  }

  const fields = scrubUpdateFields(updates);

  const updateExpression = generateUpdateExpression({
    fields,
    updatedBy,
    table: getDrawTableName(),
    pk: organizationId,
    sk: drawConfigId,
  });

  transactItems.push({
    Update: updateExpression,
  });

  try {
    await dynamoDBClient.transactWrite({
      TransactItems: transactItems,
    });
  } catch (err) {
    if (!isTransactionConditionalCheckFailed(err)) {
      throw new Error("Failed to update draw workflow entity", { cause: err });
    }

    throw new Error(`Draw sort with ID ${updates.sortId} does not exist.`);
  }

  return await getDrawConfigEntity({ drawConfigId, organizationId });
};

export const getDrawConfigEntity = async (input: {
  organizationId: string;
  drawConfigId: string;
}): Promise<DrawConfigEntity> => {
  const { organizationId, drawConfigId } = input;
  const { Item: entity } = await dynamoDBClient.get({
    TableName: getDrawTableName(),
    Key: {
      pk: organizationId,
      sk: drawConfigId,
    },
    ConsistentRead: true,
  });

  return entity as DrawConfigEntity;
};

export const deleteDrawConfigEntity = async (input: {
  organizationId: string;
  drawConfigId: string;
}): Promise<boolean> => {
  const { organizationId, drawConfigId } = input;

  await dynamoDBClient.delete({
    TableName: getDrawTableName(),
    Key: {
      pk: organizationId,
      sk: drawConfigId,
    },
  });

  return true;
};

export const getDrawConfigEntityForWorkflowNode = async (input: {
  organizationId: string;
  workflowNodeId: string;
}): Promise<DrawConfigEntity | null> => {
  const { organizationId, workflowNodeId } = input;

  const { Item: link } = await dynamoDBClient.get({
    TableName: getDrawTableName(),
    Key: {
      pk: organizationId,
      sk: generateDrawConfigWorkflowNodeLinkSk({ workflowNodeId }),
    },
  });

  if (!link) {
    return null;
  }

  const { drawConfigId } = link;

  return await getDrawConfigEntity({ organizationId, drawConfigId });
};

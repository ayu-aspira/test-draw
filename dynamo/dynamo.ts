import {
  type BaseEntity,
  chunkBatchWriteItems,
  createDynamoDBClient,
  getExclusiveStartKey,
  getNextToken,
} from "@aspira-nextgen/core/dynamodb";
import type { WorkflowPosition } from "@aspira-nextgen/graphql/resolvers";
import {
  type DynamoDBDocumentPaginationConfiguration,
  type QueryCommandInput,
  paginateQuery,
} from "@aws-sdk/lib-dynamodb";
import type { DynamoDBDocument } from "@aws-sdk/lib-dynamodb";
import { Table } from "sst/node/table";

export const dynamoDBClient: DynamoDBDocument = createDynamoDBClient();

export const getDrawTableName = (): string => Table.Draw.tableName;
export const getWorkflowTableName = (): string => Table.Draw.tableName;

export enum DynamoDBIndex {
  ONE_TO_MANY = "oneToManyIndex",
}

export const DEFAULT_LIMIT: number = 25;

export enum DynamoDBTypename {
  DRAW = "Draw",
  DRAW_CATEGORY = "DrawCategory",
  DRAW_SORT = "DrawSort",
  DRAW_CONFIG = "DrawConfig",
  DRAW_CONFIG_WORKFLOW_NODE_LINK = "DrawConfigWorkflowNodeLink",

  DRAW_DOCUMENT = "DrawDocument",
  DRAW_DOCUMENT_COLLECTION = "DrawDocumentCollection",
  DRAW_DOCUMENT_WORKFLOW_NODE_LINK = "DrawDocumentWorkflowNodeLink",

  WORKFLOW = "Workflow",
  WORKFLOW_INSTANCE = "WorkflowInstance",
  WORKFLOW_JOB = "WorkflowJob",
  WORKFLOW_JOB_LOG = "WorkflowJobLog",
  WORKFLOW_NODE_RESULT = "WorkflowNodeResult",
  WORKFLOW_NODE = "WorkflowNode",
  WORKFLOW_EDGE = "WorkflowEdge",
  WORKFLOW_NOOP_NODE = "WorkflowNoopNode",
  WORKFLOW_COMMIT_NODE = "WorkflowCommitNode",

  DRAW_WORKFLOW = "DrawWorkflow",
}

export enum DynamoDBPrefix {
  DRAW = "draw_main",
  DRAW_NAME_UNIQUE_CONSTRAINT = "unique_constraint_draw_main_name",

  DRAW_CATEGORY = "draw_category",

  DRAW_CONFIG = "draw_config",

  DRAW_SORT = "draw_sort",
  DRAW_SORT_NAME_UNIQUE_CONSTRAINT = "unique_constraint_draw_sort_name",

  DRAW_DOCUMENT = "draw_document",

  DRAW_DOCUMENT_COLLECTION = "draw_doc_collection",

  WORKFLOW = "workflow_main",
  WORKFLOW_INSTANCE = "workflow_instance",
  WORKFLOW_JOB = "workflow_job_main",
  WORKFLOW_JOB_LOG = "workflow_job_log",
  WORKFLOW_NODE_RESULT = "workflow_node_result",
  WORKFLOW_NODE = "workflow_node_main",
  WORKFLOW_EDGE = "workflow_edge_main",

  DRAW_WORKFLOW = "draw_workflow",
}

export type Position = Omit<WorkflowPosition, "__typename">;

export const otmPathBuilder =
  (prefix: DynamoDBPrefix) =>
  (input: { parentId: string; childId?: string }): string => {
    return `${input.parentId}#${input.childId ?? prefix}`;
  };

export const entityExistsConditionCheck = (input: { pk: string; sk: string; tableName: string }) => ({
  ConditionCheck: {
    TableName: input.tableName,
    Key: {
      pk: input.pk,
      sk: input.sk,
    },
    ConditionExpression: "attribute_exists(pk) AND attribute_exists(sk)",
  },
});

interface PaginationRequest {
  limit?: number | null;
  nextToken?: string | null;
}

interface PaginatedResponse<T> {
  items: T[];
  nextToken?: string;
}

export const generateOneToManyQuery = (
  input: { tableName: string; organizationId: string; key: string },
  query?: QueryCommandInput,
): QueryCommandInput => {
  const { tableName, organizationId, key } = input;
  return {
    TableName: tableName,
    IndexName: DynamoDBIndex.ONE_TO_MANY,
    KeyConditionExpression: "pk = :pk and begins_with(oneToManyKey, :key)",
    ExpressionAttributeValues: {
      ":pk": organizationId,
      ":key": key,
    },
    ScanIndexForward: false,
    ...(query || {}),
  };
};

export const queryEntitiesWithPagination = async <T>(
  query: QueryCommandInput,
  paginationRequest?: PaginationRequest,
): Promise<PaginatedResponse<T>> => {
  const { limit: unsafeLimit, nextToken: token } = paginationRequest ?? {};

  const limit = unsafeLimit ?? DEFAULT_LIMIT;

  if (limit < 0) {
    throw new Error("Limit must be greater than 0");
  }

  const { Items: items = [], LastEvaluatedKey } = await dynamoDBClient.query({
    ...query,

    // set pagination last
    Limit: limit,
    ExclusiveStartKey: getExclusiveStartKey(token),
  });

  const nextToken = getNextToken(LastEvaluatedKey) ?? undefined;

  return {
    items: items as T[],
    nextToken,
  };
};

export const queryAllEntities = async <T>(query: QueryCommandInput, pageSize: number = DEFAULT_LIMIT): Promise<T[]> => {
  const paginatorConfig: DynamoDBDocumentPaginationConfiguration = {
    client: dynamoDBClient,
    pageSize,
  };

  const paginator = paginateQuery(paginatorConfig, query);
  const items = [];
  for await (const page of paginator) {
    if (!page.Items) {
      break;
    }

    items.push(...page.Items!);
  }

  return items as T[];
};

export const queryOneToManyEntitiesWithPagination = async <T>(
  input: { tableName: string; organizationId: string; key: string },
  paginationRequest?: PaginationRequest,
): Promise<PaginatedResponse<T>> => {
  const query = generateOneToManyQuery(input);
  return await queryEntitiesWithPagination<T>(query, paginationRequest);
};

export const queryAllOneToManyEntities = async <T>(
  input: { tableName: string; organizationId: string; key: string },
  pageSize: number = DEFAULT_LIMIT,
): Promise<T[]> => {
  const query = generateOneToManyQuery(input);
  return await queryAllEntities<T>(query, pageSize);
};

export async function* paginateOverOneToManyEntities<T>(
  input: { tableName: string; organizationId: string; key: string },
  pageSize: number = DEFAULT_LIMIT,
): AsyncGenerator<T[]> {
  const query = generateOneToManyQuery(input);
  const paginatorConfig: DynamoDBDocumentPaginationConfiguration = {
    client: dynamoDBClient,
    pageSize,
  };

  const paginator = paginateQuery(paginatorConfig, query);

  for await (const page of paginator) {
    if (page.Items) {
      yield page.Items as T[];
    }
  }
}

export const deleteEntities = async (entities: BaseEntity[], table: string): Promise<void> => {
  const pks = Array.from(new Set(entities.map((e) => e.pk)));
  if (pks.length > 1) {
    throw new Error("Cannot delete entities with different partition keys");
  }
  const pk = pks[0];

  const sks = Array.from(new Set(entities.map((e) => e.sk)));

  const chunks = chunkBatchWriteItems(sks, (sk) => ({
    DeleteRequest: {
      Key: { pk, sk },
    },
  }));

  for (const chunk of chunks) {
    const command = {
      RequestItems: {
        [table]: chunk,
      },
    };

    await dynamoDBClient.batchWrite(command);
  }
};

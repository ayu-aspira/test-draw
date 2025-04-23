import { DynamoDBPrefix, DynamoDBTypename, dynamoDBClient, getDrawTableName } from "@/dynamo/dynamo";
import {
  type BaseEntity,
  type CreateMetadata,
  type WithMetadata,
  chunkBatchGetItems,
  isTransactionConditionalCheckFailed,
} from "@aspira-nextgen/core/dynamodb";
import type { DrawDocumentProcessingStatus, DrawDocumentType } from "@aspira-nextgen/graphql/resolvers";
import { ulid } from "ulidx";

export type DrawDocumentEntity = WithMetadata<
  BaseEntity<DynamoDBTypename.DRAW_DOCUMENT> & {
    id: string;
    s3Key: string;
    filename: string;
    name: string;
    contentType: string;
    processingStatus: DrawDocumentProcessingStatus;
    type: DrawDocumentType;
  }
>;

/**
 * An entity representing a collection of draw-documents
 * that have been archived via a zip file.
 */
export type DrawDocumentCollectionEntity = WithMetadata<
  BaseEntity & {
    id: string;
    s3Key: string;
    filename: string;
    name: string;
    contentType: string;
  }
>;

type DrawDocumentWorkflowNodeLinkEntity = WithMetadata<
  BaseEntity & {
    workflowNodeId: string;
    drawDocumentId: string;
  }
>;

export const generateDocumentId = (): string => {
  return `${DynamoDBPrefix.DRAW_DOCUMENT}_${ulid()}`;
};

export const generateDocumentCollectionId = (): string => {
  return `${DynamoDBPrefix.DRAW_DOCUMENT_COLLECTION}_${ulid()}`;
};

const generateDrawDocumentWorkflowNodeLinkSk = ({ workflowNodeId }: { workflowNodeId: string }) =>
  `${workflowNodeId}#drawDocument`;

export const createDrawDocumentEntity = async (
  input: {
    id: string;
    filename: string;
    name: string;
    contentType: string;
    s3Key: string;
    processingStatus: DrawDocumentProcessingStatus;
    type: DrawDocumentType;
    workflowNodeId?: string | null;
  } & CreateMetadata,
  organizationId: string,
): Promise<DrawDocumentEntity> => {
  const transactItems = [];

  const drawDocumentEntity: DrawDocumentEntity = {
    pk: organizationId,
    sk: input.id,
    id: input.id,
    s3Key: input.s3Key,
    filename: input.filename,
    name: input.name,
    contentType: input.contentType,
    processingStatus: input.processingStatus,
    type: input.type,
    __typename: DynamoDBTypename.DRAW_DOCUMENT,
  };

  const drawTableName = getDrawTableName();

  transactItems.push({
    Put: {
      TableName: drawTableName,
      Item: drawDocumentEntity,
    },
  });

  if (input.workflowNodeId) {
    const workflowNodeLink: DrawDocumentWorkflowNodeLinkEntity = {
      pk: organizationId,
      sk: generateDrawDocumentWorkflowNodeLinkSk({ workflowNodeId: input.workflowNodeId }),
      workflowNodeId: input.workflowNodeId,
      drawDocumentId: input.id,
      __typename: DynamoDBTypename.DRAW_DOCUMENT_WORKFLOW_NODE_LINK,
    };

    transactItems.push({
      Put: {
        TableName: drawTableName,
        Item: workflowNodeLink,
      },
    });
  }

  try {
    await dynamoDBClient.transactWrite({
      TransactItems: transactItems,
    });
  } catch (err) {
    if (isTransactionConditionalCheckFailed(err)) {
      throw new Error(`A document with the name "${input.name}" already exists.`);
    }

    throw new Error("Failed to create draw document.", { cause: err });
  }

  return drawDocumentEntity;
};

export const createDrawDocumentCollectionEntity = async (
  input: {
    id: string;
    filename: string;
    name: string;
    contentType: string;
    s3Key: string;
  } & CreateMetadata,
  organizationId: string,
): Promise<DrawDocumentCollectionEntity> => {
  const drawDocumentCollectionEntity: DrawDocumentCollectionEntity = {
    pk: organizationId,
    sk: input.id,
    id: input.id,
    s3Key: input.s3Key,
    filename: input.filename,
    name: input.name,
    contentType: input.contentType,
    __typename: DynamoDBTypename.DRAW_DOCUMENT_COLLECTION,
  };

  try {
    await dynamoDBClient.put({
      TableName: getDrawTableName(),
      Item: drawDocumentCollectionEntity,
    });

    return drawDocumentCollectionEntity;
  } catch (err) {
    throw new Error("Failed to create draw document collection.", { cause: err });
  }
};

export const getDrawDocumentEntities = async (
  documentIds: string[],
  organizationId: string,
): Promise<DrawDocumentEntity[]> => {
  if (documentIds.length === 0) {
    return [];
  }

  const drawTableName = getDrawTableName();

  const chunks = chunkBatchGetItems(Array.from(new Set(documentIds)).map((id) => ({ pk: organizationId, sk: id })));

  const results = [];

  try {
    for (const chunk of chunks) {
      const { Responses } = await dynamoDBClient.batchGet({
        RequestItems: {
          [drawTableName]: {
            Keys: chunk,
            ConsistentRead: true,
          },
        },
      });

      if (!Responses) {
        continue;
      }

      results.push(...(Responses[drawTableName] as DrawDocumentEntity[]));
    }

    return results;
  } catch (err) {
    throw new Error("Failed to get draw documents.", { cause: err });
  }
};

export const getDocumentEntity = async (input: {
  organizationId: string;
  documentId: string;
}): Promise<DrawDocumentEntity> => {
  const { organizationId, documentId } = input;

  const { Item } = await dynamoDBClient.get({
    TableName: getDrawTableName(),
    Key: { pk: organizationId, sk: documentId },
  });

  if (!Item) {
    throw new Error("Document not found.");
  }

  return Item as DrawDocumentEntity;
};

export const listDrawDocumentEntitiesByWorkflowNodeIds = async (input: {
  organizationId: string;
  workflowNodeIds: string[];
}): Promise<(DrawDocumentEntity & { workflowNodeId: string })[]> => {
  const { organizationId, workflowNodeIds } = input;

  if (workflowNodeIds.length === 0) {
    return [];
  }

  const drawTableName = getDrawTableName();

  const chunks = chunkBatchGetItems(
    Array.from(new Set(workflowNodeIds)).map((workflowNodeId) => ({
      pk: organizationId,
      sk: generateDrawDocumentWorkflowNodeLinkSk({ workflowNodeId }),
    })),
  );

  const results = [];

  try {
    for (const chunk of chunks) {
      const { Responses: linkEntities } = await dynamoDBClient.batchGet({
        RequestItems: {
          [drawTableName]: {
            Keys: chunk,
            ConsistentRead: true,
          },
        },
      });

      if (!linkEntities) {
        continue;
      }

      const nodeIdByDocumentId = (linkEntities[drawTableName] as DrawDocumentWorkflowNodeLinkEntity[]).reduce(
        (acc, link) => {
          acc[link.drawDocumentId] = link.workflowNodeId;
          return acc;
        },
        {} as Record<string, string>,
      );

      const documentEntities = await getDrawDocumentEntities(Object.keys(nodeIdByDocumentId), organizationId);

      results.push(
        ...documentEntities.map((document) => ({ ...document, workflowNodeId: nodeIdByDocumentId[document.id] })),
      );
    }

    return results;
  } catch (err) {
    throw new Error("Failed to get draw documents by workflow node IDs.", { cause: err });
  }
};

export const getDrawDocumentCollectionEntity = async (input: {
  documentCollectionId: string;
  organizationId: string;
}): Promise<DrawDocumentCollectionEntity | undefined> => {
  const { documentCollectionId, organizationId } = input;

  const { Item } = await dynamoDBClient.get({
    TableName: getDrawTableName(),
    Key: { pk: organizationId, sk: documentCollectionId },
  });

  return Item as DrawDocumentCollectionEntity;
};

export const updateProcessingStatus = async (
  organizationId: string,
  documentId: string,
  processingStatus: DrawDocumentProcessingStatus,
  errorMessage?: string,
): Promise<void> => {
  let updateExpression = "SET processingStatus = :processingStatus";

  const expressionAttributeValues: Record<string, string> = {
    ":processingStatus": processingStatus,
  };

  if (errorMessage) {
    updateExpression += ", errorMessage = :errorMessage";
    expressionAttributeValues[":errorMessage"] = errorMessage;
  }

  try {
    await dynamoDBClient.update({
      TableName: getDrawTableName(),
      Key: { pk: organizationId, sk: documentId },
      UpdateExpression: updateExpression,
      ConditionExpression: "attribute_exists(pk) and attribute_exists(sk)",
      ExpressionAttributeValues: expressionAttributeValues,
    });
  } catch (err) {
    throw new Error("Failed to update JSON processing status.", { cause: err });
  }
};

export const deleteDrawDocumentEntity = async (input: {
  organizationId: string;
  documentId: string;
}): Promise<boolean> => {
  const { organizationId, documentId } = input;
  const document = await getDocumentEntity({ organizationId, documentId });

  if (!document) {
    return false;
  }

  const drawDocumentTable = getDrawTableName();

  const transactItems = [
    {
      Delete: {
        TableName: drawDocumentTable,
        Key: { pk: organizationId, sk: documentId },
      },
    },
  ];

  try {
    await dynamoDBClient.transactWrite({
      TransactItems: transactItems,
    });
  } catch (err) {
    throw new Error("Failed to delete draw document.", { cause: err });
  }

  return true;
};

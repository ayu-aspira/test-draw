import { config as dotenvx } from "@dotenvx/dotenvx";
dotenvx({ logLevel: "error" });

import { readFile } from "node:fs/promises";
import path from "node:path";
import { DrawDocumentType, DrawSortDirection } from "@aspira-nextgen/graphql/resolvers";
import type {
  CreateDrawConfigInput,
  CreateDrawSortInput,
  DrawCategory,
  DrawConfig,
  DrawDocument,
  DrawDocumentUpload,
  DrawSort,
  DrawWorkflow,
  Workflow,
  WorkflowEdge,
  WorkflowNode,
  WorkflowPosition,
} from "@aspira-nextgen/graphql/resolvers";
import { GraphQLClient, gql } from "graphql-request";
import fetch from "node-fetch";
import { ulid } from "ulidx";

export const createGraphQLClient = (): GraphQLClient => {
  const { GRAPHQL_URL, GRAPHQL_JWT } = process.env;
  if (!GRAPHQL_URL) {
    throw new Error("Missing GRAPHQL_URL environment variable");
  }

  if (!GRAPHQL_JWT) {
    throw new Error("Missing GRAPHQL_JWT environment variable");
  }

  return new GraphQLClient(GRAPHQL_URL, {
    headers: {
      Authorization: `Bearer ${GRAPHQL_JWT}`,
    },
  });
};

type CreateDrawCategoryResponse = {
  createDrawCategory: DrawCategory;
};

export const createDrawCategory = async (input: { name: string }, client: GraphQLClient): Promise<DrawCategory> => {
  const mutation = gql`
    mutation CreateDrawCategory($name: String!) {
      createDrawCategory(input: { name: $name }) {
        id
        name
        __typename
      }
    }
  `;

  const { createDrawCategory } = await client.request<CreateDrawCategoryResponse>(mutation, input);
  return createDrawCategory;
};

type DeleteDrawCategoryResponse = {
  deleteDrawCategory: boolean;
};

export const deleteDrawCategory = async (input: { id: string }, client: GraphQLClient): Promise<boolean> => {
  const mutation = gql`
    mutation DeleteDrawCategory($id: ID!) {
      deleteDrawCategory(input: { id: $id })
    }
  `;

  const { deleteDrawCategory } = await client.request<DeleteDrawCategoryResponse>(mutation, input);
  return deleteDrawCategory;
};

type CreateDrawWorkflowResponse = {
  createDrawWorkflow: DrawWorkflow;
};

export const createDrawWorkflow = async (
  input: { name: string; drawCategoryId: string },
  client: GraphQLClient,
): Promise<DrawWorkflow> => {
  const mutation = gql`
    mutation CreateDrawWorkflow($name: String!, $drawCategoryId: ID!) {
      createDrawWorkflow(input: { name: $name, drawCategoryId: $drawCategoryId }) {
        id
        name
        workflow { id }
        __typename
      }
    }
  `;

  return client
    .request<CreateDrawWorkflowResponse>(mutation, input)
    .then(({ createDrawWorkflow }) => createDrawWorkflow);
};

type DeleteDrawDocumentResponse = {
  deleteDrawDocument: boolean;
};

export const deleteDrawDocument = async (input: { id: string }, client: GraphQLClient): Promise<boolean> => {
  const mutation = gql`
    mutation DeleteDrawDocument($id: ID!) {
      deleteDrawDocument(input: { id: $id })
    }
  `;

  const { deleteDrawDocument } = await client.request<DeleteDrawDocumentResponse>(mutation, input);
  return deleteDrawDocument;
};

type DeleteDrawConfigResponse = {
  deleteDrawConfig: boolean;
};

export const deleteDrawConfig = async (input: { id: string }, client: GraphQLClient): Promise<boolean> => {
  const mutation = gql`
    mutation DeleteDrawConfig($id: ID!) {
      deleteDrawConfig(input: { id: $id })
    }
  `;

  const { deleteDrawConfig } = await client.request<DeleteDrawConfigResponse>(mutation, input);
  return deleteDrawConfig;
};

type DeleteDrawSortResponse = {
  deleteDrawSort: boolean;
};

export const deleteDrawSort = async (input: { id: string }, client: GraphQLClient): Promise<boolean> => {
  const mutation = gql`
    mutation DeleteDrawSort($id: ID!) {
      deleteDrawSort(input: { id: $id })
    }
  `;

  const { deleteDrawSort } = await client.request<DeleteDrawSortResponse>(mutation, input);
  return deleteDrawSort;
};

type CreateWorkflowNodeResponse = {
  createWorkflowNode: WorkflowNode;
};

export const createWorkflowNode = async (
  input: {
    workflowId: string;
    type: string;
    position?: WorkflowPosition;
  },
  client: GraphQLClient,
): Promise<WorkflowNode> => {
  const { workflowId, type, position } = input;
  const mutation = gql`
    mutation CreateWorkflowNode($workflowId: ID!, $type: WorkflowNodeType!, $position: WorkflowPositionInput) {
      createWorkflowNode(input: { workflowId: $workflowId, type: $type, position: $position }) {
        id
        __typename
      }
    }
  `;

  const variables = {
    workflowId,
    type,
    ...(position && { position }),
  };

  return client
    .request<CreateWorkflowNodeResponse>(mutation, variables)
    .then(({ createWorkflowNode }) => createWorkflowNode);
};

type CreateWorkflowDataNodeResponse = {
  createWorkflowDataNode: WorkflowNode;
};

export const createWorkflowDataNode = async (
  input: {
    workflowId: string;
    type: string;
    position?: WorkflowPosition;
  },
  client: GraphQLClient,
): Promise<WorkflowNode> => {
  const { workflowId, type, position } = input;
  const mutation = gql`
    mutation CreateWorkflowDataNode($workflowId: ID!, $type: WorkflowDataNodeType!, $position: WorkflowPositionInput) {
      createWorkflowDataNode(input: { workflowId: $workflowId, type: $type, position: $position }) {
        id
        __typename
      }
    }
  `;

  const variables = {
    workflowId,
    type,
    ...(position && { position }),
  };

  return client
    .request<CreateWorkflowDataNodeResponse>(mutation, variables)
    .then(({ createWorkflowDataNode }) => createWorkflowDataNode);
};

type CreateWorkflowEdgeResponse = {
  createWorkflowEdge: WorkflowEdge;
};

export const createWorkflowEdge = async (
  input: {
    workflowId: string;
    sourceNodeId: string;
    sourceHandlePosition?: WorkflowPosition;
    targetNodeId: string;
    targetHandlePosition?: WorkflowPosition;
  },
  client: GraphQLClient,
): Promise<WorkflowEdge> => {
  const { workflowId, sourceNodeId, sourceHandlePosition, targetNodeId, targetHandlePosition } = input;
  const mutation = gql`
    mutation CreateWorkflowEdge(
      $workflowId: ID!,
      $sourceNodeId: ID!,
      $sourceHandlePosition: WorkflowPositionInput,
      $targetNodeId: ID!,
      $targetHandlePosition: WorkflowPositionInput
    ) {
      createWorkflowEdge(input: {
        workflowId: $workflowId,
        sourceNodeId: $sourceNodeId,
        sourceHandlePosition: $sourceHandlePosition
        targetNodeId: $targetNodeId
        targetHandlePosition: $targetHandlePosition
      }) {
        id
        __typename
      }
    }
  `;

  const variables = {
    workflowId,
    sourceNodeId,
    ...(sourceHandlePosition && { sourceHandlePosition: sourceHandlePosition }),
    targetNodeId,
    ...(targetHandlePosition && { targetHandlePosition: targetHandlePosition }),
  };

  const { createWorkflowEdge } = await client.request<CreateWorkflowEdgeResponse>(mutation, variables);
  return createWorkflowEdge;
};

type PrepareDrawUploadResponse = {
  prepareDrawDocumentUpload: DrawDocumentUpload;
};

export const prepareDrawDocumentUpload = async (
  input: {
    filename: string;
    name: string;
    contentType: string;
    type: DrawDocumentType;
    workflowNodeId: string;
  },
  client: GraphQLClient,
): Promise<DrawDocumentUpload> => {
  const mutation = gql`
    mutation PrepareDrawDocumentUpload($filename: String!, $name: String!, $contentType: String!, $type: DrawDocumentType!, $workflowNodeId: ID!) {
      prepareDrawDocumentUpload(input: { filename: $filename, name: $name, contentType: $contentType, type: $type, workflowNodeId: $workflowNodeId }) {
        __typename
        url
        document {
          id
          name
          type
          processingStatus
        }
      }
    }
  `;

  const { prepareDrawDocumentUpload } = await client.request<PrepareDrawUploadResponse>(mutation, input);
  return prepareDrawDocumentUpload;
};

type CreateDrawConfigResponse = {
  createDrawConfig: DrawConfig;
};

export const createDrawConfig = async (input: CreateDrawConfigInput, client: GraphQLClient): Promise<DrawConfig> => {
  const mutation = gql`
    mutation CreateDrawConfig($input: CreateDrawConfigInput!) {
      createDrawConfig(input: $input) {
        id
        name
        __typename
      }
    }
  `;

  const { createDrawConfig } = await client.request<CreateDrawConfigResponse>(mutation, { input });
  return createDrawConfig;
};

type CreateDrawSortResponse = {
  createDrawSort: DrawSort;
};

export const createDrawSort = async (input: CreateDrawSortInput, client: GraphQLClient): Promise<DrawSort> => {
  const mutation = gql`
    mutation CreateDrawSort($input: CreateDrawSortInput!) {
      createDrawSort(input: $input) {
        id
        name
        __typename
      }
    }
  `;

  const { createDrawSort } = await client.request<CreateDrawSortResponse>(mutation, { input });
  return createDrawSort;
};

export type WorkflowFixtures = {
  drawCategory: DrawCategory;
  drawWorkflow: DrawWorkflow;
  workflow: Workflow;
  data1: WorkflowNode;
  huntcodes: DrawDocument;
  data2: WorkflowNode;
  applicants: DrawDocument;
  node1: WorkflowNode;
  drawConfig: DrawConfig;
  drawSort: DrawSort;
  node2: WorkflowNode;
  edge1: WorkflowEdge;
};

async function uploadFile(input: { testName: string; filename: string; url: string }) {
  const { testName, filename, url } = input;
  const absoluteFilePath = path.resolve(__dirname, "../test-fixtures", testName, filename);

  const fileBuffer = new Uint8Array(await readFile(absoluteFilePath));
  const response = await fetch(url, {
    method: "PUT",
    body: fileBuffer,
    headers: {
      "Content-Type": "text/csv",
    },
  });

  if (!response.ok) {
    throw new Error(`Upload failed: ${response.statusText}`);
  }
}

export const createWorkflowFixtures = async (client: GraphQLClient): Promise<WorkflowFixtures> => {
  const drawCategory = await createDrawCategory({ name: `Draw Category ${ulid()}` }, client);

  const drawWorkflow = await createDrawWorkflow(
    { name: `Workflow ${ulid()}`, drawCategoryId: drawCategory.id },
    client,
  );

  const { workflow } = drawWorkflow;

  const data1 = await createWorkflowDataNode({ type: "WorkflowDrawDataNode", workflowId: workflow.id }, client);

  const upload1 = await prepareDrawDocumentUpload(
    {
      filename: `draw_upload_${ulid()}.csv`,
      name: `draw upload ${ulid()}`,
      contentType: "text/csv",
      type: DrawDocumentType.HuntCodes,
      workflowNodeId: data1.id,
    },
    client,
  );
  uploadFile({
    testName: "draw-data-applicant-filtering",
    filename: "draw-data-input.csv",
    url: upload1.url,
  });
  const huntcodes = upload1.document;

  const data2 = await createWorkflowDataNode(
    { type: "WorkflowDrawApplicantDataNode", workflowId: workflow.id },
    client,
  );

  const upload2 = await prepareDrawDocumentUpload(
    {
      filename: `draw_upload_${ulid()}.csv`,
      name: `draw upload ${ulid()}`,
      contentType: "text/csv",
      type: DrawDocumentType.Applicants,
      workflowNodeId: data2.id,
    },
    client,
  );
  uploadFile({
    testName: "draw-data-applicant-filtering",
    filename: "applicant-input.csv",
    url: upload2.url,
  });
  const applicants = upload2.document;

  const node1 = await createWorkflowNode({ type: "WorkflowDrawNode", workflowId: workflow.id }, client);

  await createWorkflowEdge({ workflowId: workflow.id, sourceNodeId: data1.id, targetNodeId: node1.id }, client);

  await createWorkflowEdge({ workflowId: workflow.id, sourceNodeId: data2.id, targetNodeId: node1.id }, client);

  const drawSort = await createDrawSort(
    {
      name: `Draw Sort ${ulid()}`,
      rules: [{ field: "pointBalance", direction: DrawSortDirection.Asc }],
    },
    client,
  );

  const drawConfig = await createDrawConfig(
    {
      workflowNodeId: node1.id,
      name: `Draw Config ${ulid()}`,
      usePoints: true,
      sortId: drawSort.id,
      quotaRules: { ruleFlags: [] },
    },
    client,
  );

  const node2 = await createWorkflowNode({ type: "WorkflowNoopNode", workflowId: workflow.id }, client);

  const edge1 = await createWorkflowEdge(
    { workflowId: workflow.id, sourceNodeId: node1.id, targetNodeId: node2.id },
    client,
  );

  return {
    drawCategory,
    drawWorkflow,
    workflow,
    data1,
    huntcodes,
    data2,
    applicants,
    node1,
    drawConfig,
    drawSort,
    node2,
    edge1,
  };
};

export const cleanupWorkflowFixtures = async (fixtures: WorkflowFixtures, client: GraphQLClient): Promise<void> => {
  if (!fixtures) {
    return;
  }

  if (fixtures.drawCategory) {
    await deleteDrawCategory({ id: fixtures.drawCategory.id }, client);
  }

  if (fixtures.huntcodes) {
    await deleteDrawDocument({ id: fixtures.huntcodes.id }, client);
  }

  if (fixtures.applicants) {
    await deleteDrawDocument({ id: fixtures.applicants.id }, client);
  }

  if (fixtures.drawConfig) {
    await deleteDrawConfig({ id: fixtures.drawConfig.id }, client);
  }
};

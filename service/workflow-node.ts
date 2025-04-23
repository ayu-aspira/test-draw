import { getLatestWorkflowInstanceEntity } from "@/dynamo/workflow";
import {
  createWorkflowNodeEntity,
  deleteWorkflowNodeEntities,
  deleteWorkflowNodeEntity,
  getWorkflowNodeEntity,
  listAllWorkflowNodeEntitiesByWorkflowInstanceId,
  listWorkflowNodeEntitiesByWorkflowInstanceId,
  updateWorkflowNodeEntity,
} from "@/dynamo/workflow-node";
import { GraphQLTypename } from "@/service/graphql";
import { batchResolved } from "@/service/graphql";
import { listWorkflowNodeResults } from "@/service/workflow-node-result";
import { WorkflowMimeDataType } from "@/step-function/workflow-types";
import { CreateWorkflowNodeInputSchema, UpdateWorkflowNodeInputSchema, WorkflowNodeID } from "@/validation/workflow";
import type { Token } from "@aspira-nextgen/core/authn";
import type {
  CreateWorkflowDataNodeInput,
  CreateWorkflowNodeInput,
  DeleteWorkflowNodeInput,
  ListWorkflowNodesInput,
  UpdateWorkflowNodeInput,
  Workflow,
  WorkflowFunctionalNodeDefinition,
  WorkflowNode,
  WorkflowNodeConnection,
  WorkflowNodeWithResults,
} from "@aspira-nextgen/graphql/resolvers";
import { WorkflowDomain, WorkflowNodeType } from "@aspira-nextgen/graphql/resolvers";

export const WorkflowNoopNodeDefinition: WorkflowFunctionalNodeDefinition = {
  type: WorkflowNodeType.WorkflowNoopNode,
  inputs: [
    {
      domain: WorkflowDomain.Any,
      domainModel: "Any",
      mimetypes: [WorkflowMimeDataType.ANY],
    },
  ],
  outputs: [
    {
      domain: WorkflowDomain.Any,
      domainModel: "Any",
      mimetypes: [WorkflowMimeDataType.ANY],
    },
  ],
  __typename: GraphQLTypename.WORKFLOW_FUNCTIONAL_NODE_DEFINITION,
};

export const WorkflowCommitNodeDefinition: WorkflowFunctionalNodeDefinition = {
  type: WorkflowNodeType.WorkflowCommitNode,
  inputs: [
    {
      domain: WorkflowDomain.Any,
      domainModel: "Any",
      mimetypes: [WorkflowMimeDataType.ANY],
    },
  ],
  outputs: [
    {
      domain: WorkflowDomain.Any,
      domainModel: "Any",
      mimetypes: [WorkflowMimeDataType.ANY],
    },
  ],
  __typename: GraphQLTypename.WORKFLOW_FUNCTIONAL_NODE_DEFINITION,
};

export const createWorkflowNode = async (input: CreateWorkflowNodeInput, token: Token): Promise<WorkflowNode> => {
  const {
    sub: createdBy,
    claims: { organizationId },
  } = token;

  const validation = CreateWorkflowNodeInputSchema.safeParse(input);
  if (!validation.success) {
    throw new Error(`Invalid input: ${JSON.stringify(validation.error.flatten())}`, { cause: validation.error });
  }
  const { workflowId, ...data } = validation.data;

  const workflowInstance = await getLatestWorkflowInstanceEntity({ organizationId, workflowId });
  if (!workflowInstance) {
    throw new Error("Provided workflow ID does not exist.");
  }
  const { id: workflowInstanceId } = workflowInstance;

  const entity = await createWorkflowNodeEntity({ ...data, workflowId, workflowInstanceId, createdBy, organizationId });

  return entity as WorkflowNode;
};

export const createWorkflowDataNode = async (
  input: CreateWorkflowDataNodeInput,
  token: Token,
): Promise<WorkflowNode> => {
  const {
    sub: createdBy,
    claims: { organizationId },
  } = token;

  const validation = CreateWorkflowNodeInputSchema.safeParse(input);
  if (!validation.success) {
    throw new Error(`Invalid input: ${JSON.stringify(validation.error.flatten())}`, { cause: validation.error });
  }
  const { workflowId, ...data } = validation.data;

  const workflowInstance = await getLatestWorkflowInstanceEntity({ organizationId, workflowId });
  if (!workflowInstance) {
    throw new Error("Provided workflow ID does not exist.");
  }
  const { id: workflowInstanceId } = workflowInstance;

  const entity = await createWorkflowNodeEntity({ ...data, workflowId, workflowInstanceId, createdBy, organizationId });

  return entity as WorkflowNode;
};

export const getWorkflowNode = async (id: string, token: Token): Promise<WorkflowNode> => {
  const {
    claims: { organizationId },
  } = token;

  validateWorkflowNodeId(id);

  const node = await getWorkflowNodeEntity({ organizationId, workflowNodeId: id });

  if (!node) {
    throw new Error(`Workflow node with ID ${id} does not exist`);
  }

  return node;
};

export const updateWorkflowNode = async (input: UpdateWorkflowNodeInput, token: Token): Promise<WorkflowNode> => {
  const validation = UpdateWorkflowNodeInputSchema.safeParse(input);
  if (!validation.success) {
    throw new Error(`Invalid input: ${JSON.stringify(validation.error.flatten())}`, { cause: validation.error });
  }
  const { id: workflowNodeId, ...updates } = validation.data;

  const {
    sub: updatedBy,
    claims: { organizationId },
  } = token;

  const entity = await updateWorkflowNodeEntity({ id: { organizationId, workflowNodeId }, updates, updatedBy });

  return entity as WorkflowNode;
};

export const deleteWorkflowNode = async (input: DeleteWorkflowNodeInput, token: Token): Promise<boolean> => {
  const { id: workflowNodeId } = input;

  validateWorkflowNodeId(workflowNodeId);

  const {
    claims: { organizationId },
  } = token;

  await deleteWorkflowNodeEntity({ organizationId, workflowNodeId });
  return true;
};

export const listWorkflowNodesByWorkflowId = async (
  input: { workflowId: string; listWorkflowNodesInput?: ListWorkflowNodesInput },
  token: Token,
): Promise<WorkflowNodeConnection> => {
  const { workflowId, listWorkflowNodesInput = {} } = input;
  const {
    claims: { organizationId },
  } = token;

  const workflowInstanceEntity = await getLatestWorkflowInstanceEntity({ organizationId, workflowId });
  if (!workflowInstanceEntity) {
    return batchResolved();
  }

  const { workflowJobId, ...paginationRequest } = listWorkflowNodesInput;

  const results = await listWorkflowNodeEntitiesByWorkflowInstanceId({
    organizationId,
    workflowInstanceId: workflowInstanceEntity.id,
    paginationRequest,
  });

  const items = results.items.map(convertWorkflowNodeEntityToWorkflowNode);

  const nodeResults = await listWorkflowNodeResults({ workflowId, workflowJobId }, token);
  for (const node of items) {
    (node as WorkflowNodeWithResults).results = nodeResults[node.id] ?? [];
  }

  return {
    ...results,
    items,
    __typename: GraphQLTypename.WORKFLOW_NODE_CONNECTION,
  };
};

export const batchListNodesForWorkflows = async (
  input: {
    workflow: Workflow;
    listWorkflowNodesInput: ListWorkflowNodesInput;
  }[],
  token: Token,
): Promise<WorkflowNodeConnection[]> => {
  return await Promise.all(
    input.map(({ workflow, listWorkflowNodesInput }) =>
      listWorkflowNodesByWorkflowId({ workflowId: workflow.id, listWorkflowNodesInput }, token),
    ),
  );
};

const convertWorkflowNodeEntityToWorkflowNode = (entity: {
  id: string;
  workflowId: string;
  position?: { x: number; y: number } | null;
}): WorkflowNode => {
  const { position, ...rest } = entity;
  const positions: {
    position?: { x: number; y: number };
  } = {};
  if (position) {
    positions.position = { ...position };
  }

  return {
    ...rest,
    ...positions,
  };
};

export const deleteWorkflowNodesByWorkflowInstanceId = async (
  input: { workflowInstanceId: string },
  token: Token,
): Promise<void> => {
  const { workflowInstanceId } = input;
  const { organizationId } = token.claims;

  const workflowNodes = await listAllWorkflowNodeEntitiesByWorkflowInstanceId({ organizationId, workflowInstanceId });
  await deleteWorkflowNodeEntities(workflowNodes);
};

const validateWorkflowNodeId = (id: string): void => {
  const validation = WorkflowNodeID.safeParse(id);

  if (!validation.success) {
    throw new Error(`Invalid input: ${JSON.stringify(validation.error.flatten())}`, { cause: validation.error });
  }
};

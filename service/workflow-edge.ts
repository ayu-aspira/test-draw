import { getLatestWorkflowInstanceEntity } from "@/dynamo/workflow";
import {
  createWorkflowEdgeEntity,
  deleteWorkflowEdgeEntities,
  deleteWorkflowEdgeEntity,
  listAllWorkflowEdgeEntitiesByWorkflowInstanceId,
  listWorkflowEdgeEntitiesByWorkflowInstanceId,
  updateWorkflowEdgeEntity,
} from "@/dynamo/workflow-edge";
import { batchResolved } from "@/service/graphql";
import { GraphQLTypename } from "@/service/graphql";
import {
  CreateWorkflowEdgeInputSchema,
  DeleteWorkflowEdgeInputSchema,
  UpdateWorkflowEdgeInputSchema,
} from "@/validation/workflow";
import type { Token } from "@aspira-nextgen/core/authn";
import type {
  CreateWorkflowEdgeInput,
  DeleteWorkflowEdgeInput,
  ListWorkflowEdgesInput,
  UpdateWorkflowEdgeInput,
  Workflow,
  WorkflowEdge,
  WorkflowEdgeConnection,
} from "@aspira-nextgen/graphql/resolvers";

export const createWorkflowEdge = async (input: CreateWorkflowEdgeInput, token: Token): Promise<WorkflowEdge> => {
  const {
    sub: createdBy,
    claims: { organizationId },
  } = token;

  const validation = CreateWorkflowEdgeInputSchema.safeParse(input);
  if (!validation.success) {
    throw new Error(`Invalid input: ${JSON.stringify(validation.error.flatten())}`, { cause: validation.error });
  }
  const { workflowId, ...data } = validation.data;

  const workflowInstance = await getLatestWorkflowInstanceEntity({ organizationId, workflowId });
  if (!workflowInstance) {
    throw new Error("Provided workflow ID does not exist.");
  }
  const { id: workflowInstanceId } = workflowInstance;

  const entity = await createWorkflowEdgeEntity({ ...data, workflowId, workflowInstanceId, organizationId, createdBy });

  return entity as WorkflowEdge;
};

export const updateWorkflowEdge = async (input: UpdateWorkflowEdgeInput, token: Token): Promise<WorkflowEdge> => {
  const validation = UpdateWorkflowEdgeInputSchema.safeParse(input);
  if (!validation.success) {
    throw new Error(`Invalid input: ${JSON.stringify(validation.error.flatten())}`, { cause: validation.error });
  }
  const { id: workflowEdgeId, ...updates } = validation.data;

  const {
    sub: updatedBy,
    claims: { organizationId },
  } = token;

  const entity = await updateWorkflowEdgeEntity({ id: { organizationId, workflowEdgeId }, updates, updatedBy });

  return entity as WorkflowEdge;
};

export const deleteWorkflowEdge = async (input: DeleteWorkflowEdgeInput, token: Token): Promise<boolean> => {
  const { id: workflowEdgeId } = input;

  const validation = DeleteWorkflowEdgeInputSchema.safeParse(input);
  if (!validation.success) {
    throw new Error(`Invalid input: ${JSON.stringify(validation.error.flatten())}`, { cause: validation.error });
  }

  const {
    claims: { organizationId },
  } = token;

  await deleteWorkflowEdgeEntity({ organizationId, workflowEdgeId });
  return true;
};

export const listWorkflowEdgesByWorkflowId = async (
  input: { workflowId: string; listWorkflowEdgesInput?: ListWorkflowEdgesInput },
  token: Token,
): Promise<WorkflowEdgeConnection> => {
  const { workflowId, listWorkflowEdgesInput = {} } = input;
  const {
    claims: { organizationId },
  } = token;

  const workflowInstanceEntity = await getLatestWorkflowInstanceEntity({ organizationId, workflowId });
  if (!workflowInstanceEntity) {
    return batchResolved();
  }

  const results = await listWorkflowEdgeEntitiesByWorkflowInstanceId({
    organizationId,
    workflowInstanceId: workflowInstanceEntity.id,
    paginationRequest: listWorkflowEdgesInput,
  });

  const items = results.items.map(convertWorkflowEdgeEntityToWorkflowEdge);

  return {
    ...results,
    items,
    __typename: GraphQLTypename.WORKFLOW_EDGE_CONNECTION,
  };
};

export const batchListEdgesForWorkflows = async (
  input: {
    workflow: Workflow;
    listWorkflowEdgesInput: ListWorkflowEdgesInput;
  }[],
  token: Token,
): Promise<WorkflowEdgeConnection[]> => {
  return await Promise.all(
    input.map(({ workflow, listWorkflowEdgesInput }) =>
      listWorkflowEdgesByWorkflowId({ workflowId: workflow.id, listWorkflowEdgesInput }, token),
    ),
  );
};

const convertWorkflowEdgeEntityToWorkflowEdge = (entity: {
  id: string;
  workflowId: string;
  sourceNodeId: string;
  sourceHandlePosition?: { x: number; y: number } | null;
  targetNodeId: string;
  targetHandlePosition?: { x: number; y: number } | null;
}): WorkflowEdge => {
  const { sourceHandlePosition, targetHandlePosition, ...rest } = entity;
  const positions: {
    sourceHandlePosition?: { x: number; y: number };
    targetHandlePosition?: { x: number; y: number };
  } = {};
  if (sourceHandlePosition) {
    positions.sourceHandlePosition = {
      ...sourceHandlePosition,
    };
  }
  if (targetHandlePosition) {
    positions.targetHandlePosition = {
      ...targetHandlePosition,
    };
  }

  return {
    ...rest,
    ...positions,
  };
};

export const deleteWorkflowEdgesByWorkflowInstanceId = async (
  input: { workflowInstanceId: string },
  token: Token,
): Promise<void> => {
  const { workflowInstanceId } = input;
  const { organizationId } = token.claims;
  const workflowEdges = await listAllWorkflowEdgeEntitiesByWorkflowInstanceId({ organizationId, workflowInstanceId });
  await deleteWorkflowEdgeEntities(workflowEdges);
};

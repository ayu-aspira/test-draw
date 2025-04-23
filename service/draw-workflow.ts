import {
  createDrawWorkflowEntity,
  deleteDrawWorkflowEntitiesByDrawCategoryId,
  deleteDrawWorkflowEntity,
  getDrawWorkflowEntity,
  listDrawWorkflowEntitiesForDrawCategory,
  updateDrawWorkflowEntity,
} from "@/dynamo/draw-workflow";
import { DynamoDBTypename } from "@/dynamo/dynamo";
import { getWorkflowEntity } from "@/dynamo/workflow";
import { type WorkflowEntity, batchGetWorkflowEntities } from "@/dynamo/workflow";
import { DRAW_APPLICANT_DOMAIN_MODEL, DRAW_DRAW_DATA_DOMAIN_MODEL } from "@/service/draw-constants";
import { batchResolved } from "@/service/graphql";
import { GraphQLTypename } from "@/service/graphql";
import { WorkflowDrawNodeDefinition } from "@/service/workflow-draw-node";
import { WorkflowCommitNodeDefinition } from "@/service/workflow-node";
import { WorkflowMimeDataType } from "@/step-function/workflow-types";
import type { Token } from "@aspira-nextgen/core/authn";
import type {
  CreateDrawWorkflowInput,
  DeleteDrawWorkflowInput,
  DrawCategory,
  DrawWorkflow,
  DrawWorkflowConnection,
  ListDrawWorkflowsInput,
  UpdateDrawWorkflowInput,
  WorkflowDataDefinition,
  WorkflowDataNodeDefinition,
} from "@aspira-nextgen/graphql/resolvers";
import { WorkflowDataNodeType, WorkflowDomain } from "@aspira-nextgen/graphql/resolvers";
import { z } from "zod";
import { trimmedString } from "#validation/common.ts";

export const WorkflowDrawApplicantDataNodeDefinition: WorkflowDataNodeDefinition = {
  type: WorkflowDataNodeType.WorkflowDrawApplicantDataNode,
  outputs: [
    {
      domain: WorkflowDomain.Draw,
      domainModel: DRAW_APPLICANT_DOMAIN_MODEL,
      mimetypes: [WorkflowMimeDataType.CSV],
    },
  ],
  __typename: GraphQLTypename.WORKFLOW_DATA_NODE_DEFINITION,
};

export const WorkflowDrawDataNodeDefinition: WorkflowDataNodeDefinition = {
  type: WorkflowDataNodeType.WorkflowDrawDataNode,
  outputs: [
    {
      domain: WorkflowDomain.Draw,
      domainModel: DRAW_DRAW_DATA_DOMAIN_MODEL,
      mimetypes: [WorkflowMimeDataType.CSV],
    },
  ],
  __typename: GraphQLTypename.WORKFLOW_DATA_NODE_DEFINITION,
};

const CreateDrawWorkflowInputSchema = z.object({
  name: trimmedString(),
  drawCategoryId: z.string(),
});

const UpdateDrawWorkflowInputSchema = CreateDrawWorkflowInputSchema.partial().extend({
  id: z.string(),
});

export const createDrawWorkflow = async (input: CreateDrawWorkflowInput, token: Token): Promise<DrawWorkflow> => {
  const validation = CreateDrawWorkflowInputSchema.safeParse(input);
  if (!validation.success) {
    throw new Error(`Invalid input: ${JSON.stringify(validation.error.flatten())}`, { cause: validation.error });
  }
  const entity = validation.data;

  const results = await createDrawWorkflowEntity({
    ...entity,
    createdBy: token.sub,
    organizationId: token.claims.organizationId,
  });

  return {
    ...results.drawWorkflowEntity,
    __typename: DynamoDBTypename.DRAW_WORKFLOW,
    workflow: {
      ...results.workflowEntity,
      nodes: batchResolved(),
      edges: batchResolved(),
      __typename: DynamoDBTypename.WORKFLOW,
    },
  };
};

export const getDrawWorkflow = async (drawWorkflowId: string, token: Token): Promise<DrawWorkflow> => {
  const { organizationId } = token.claims;

  const drawWorkflowEntity = await getDrawWorkflowEntity({ drawWorkflowId, organizationId });

  const workflowEntity = await getWorkflowEntity({
    organizationId,
    workflowId: drawWorkflowEntity.workflowId,
  });

  return {
    ...drawWorkflowEntity,
    __typename: DynamoDBTypename.DRAW_WORKFLOW,
    workflow: {
      ...workflowEntity,
      nodes: batchResolved(),
      edges: batchResolved(),
      __typename: DynamoDBTypename.WORKFLOW,
    },
  };
};

export const updateDrawWorkflow = async (input: UpdateDrawWorkflowInput, token: Token): Promise<DrawWorkflow> => {
  const { id: drawWorkflowId } = input;
  const {
    claims: { organizationId },
    sub: updatedBy,
  } = token;

  const validation = UpdateDrawWorkflowInputSchema.safeParse(input);
  if (!validation.success) {
    throw new Error(`Invalid input: ${JSON.stringify(validation.error.flatten())}`, { cause: validation.error });
  }
  const updates = validation.data;

  const drawWorkflowEntity = await updateDrawWorkflowEntity({
    id: { drawWorkflowId, organizationId },
    updates: { name: updates.name, drawCategoryId: updates.drawCategoryId },
    updatedBy,
  });

  const workflowEntity = await getWorkflowEntity({
    organizationId,
    workflowId: drawWorkflowEntity.workflowId,
  });

  return {
    ...drawWorkflowEntity,
    __typename: DynamoDBTypename.DRAW_WORKFLOW,
    workflow: {
      ...workflowEntity,
      nodes: batchResolved(),
      edges: batchResolved(),
      __typename: DynamoDBTypename.WORKFLOW,
    },
  };
};

export const deleteDrawWorkflow = async (input: DeleteDrawWorkflowInput, token: Token): Promise<boolean> => {
  const { id: drawWorkflowId } = input;
  const {
    claims: { organizationId },
  } = token;

  return await deleteDrawWorkflowEntity({ drawWorkflowId, organizationId });
};

export const deleteDrawWorkflowByDrawCategoryId = async (
  input: { drawCategoryId: string },
  token: Token,
): Promise<boolean> => {
  const { drawCategoryId } = input;
  const {
    claims: { organizationId },
  } = token;

  await deleteDrawWorkflowEntitiesByDrawCategoryId({ drawCategoryId, organizationId });
  return true;
};

export const batchListDrawWorkflowsForDrawCategories = async (
  input: {
    drawCategory: DrawCategory;
    paginationRequest: ListDrawWorkflowsInput;
  }[],
  token: Token,
): Promise<DrawWorkflowConnection[]> => {
  return await Promise.all(
    input.map(({ drawCategory, paginationRequest }) =>
      listDrawWorkflowsForDrawCategory({ drawCategoryId: drawCategory.id, paginationRequest }, token),
    ),
  );
};

export const listDrawWorkflowsForDrawCategory = async (
  input: {
    drawCategoryId: string;
    paginationRequest?: ListDrawWorkflowsInput;
  },
  token: Token,
): Promise<DrawWorkflowConnection> => {
  const { drawCategoryId, paginationRequest = {} } = input;
  const { organizationId } = token.claims;

  const { items: drawWorkflowEntities, nextToken } = await listDrawWorkflowEntitiesForDrawCategory({
    drawCategoryId,
    organizationId,
    paginationRequest: paginationRequest,
  });

  const workflowIds = drawWorkflowEntities.map(({ workflowId }) => workflowId);
  const workflowEntities = await batchGetWorkflowEntities({ organizationId, workflowIds });

  const workflowEntityMap = workflowEntities.reduce(
    (acc, entity) => {
      acc[entity.id] = entity;
      return acc;
    },
    {} as Record<string, WorkflowEntity>,
  );

  const items: DrawWorkflow[] = drawWorkflowEntities.map((workflow) => {
    return {
      ...workflow,
      workflow: {
        ...workflowEntityMap[workflow.workflowId],
        nodes: batchResolved(),
        edges: batchResolved(),
        __typename: DynamoDBTypename.WORKFLOW,
      },
      __typename: DynamoDBTypename.DRAW_WORKFLOW,
    };
  });

  return {
    items,
    nextToken,
    __typename: GraphQLTypename.DRAW_WORKFLOW_CONNECTION,
  };
};

const drawWorkflowDataDefinitions: WorkflowDataDefinition = {
  nodes: [WorkflowDrawNodeDefinition, WorkflowCommitNodeDefinition],
  dataNodes: [WorkflowDrawApplicantDataNodeDefinition, WorkflowDrawDataNodeDefinition],
};

export const listDrawWorkflowDataDefinitions = (): WorkflowDataDefinition => drawWorkflowDataDefinitions;

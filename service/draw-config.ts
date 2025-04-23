import {
  type DrawConfigEntity,
  createDrawConfigEntity,
  deleteDrawConfigEntity,
  getDrawConfigEntity,
  getDrawConfigEntityForWorkflowNode,
  updateDrawConfigEntity,
} from "@/dynamo/draw-config";
import type { Token } from "@aspira-nextgen/core/authn";
import type {
  CreateDrawConfigInput,
  DeleteDrawConfigInput,
  DrawConfig,
  UpdateDrawConfigInput,
} from "@aspira-nextgen/graphql/resolvers";
import { CreateDrawConfigInputSchema, DrawConfigID, UpdateDrawConfigInputSchema } from "#validation/draw-config.ts";

export const createDrawConfig = async (input: CreateDrawConfigInput, token: Token): Promise<DrawConfig> => {
  const {
    sub: createdBy,
    claims: { organizationId },
  } = token;

  const validation = CreateDrawConfigInputSchema.safeParse(input);
  if (!validation.success) {
    throw new Error(`Invalid input: ${JSON.stringify(validation.error.flatten())}`, { cause: validation.error });
  }
  const data = validation.data;

  const entity = await createDrawConfigEntity({
    organizationId,
    createdBy,
    workflowNodeId: data.workflowNodeId,
    name: data.name,
    sortId: data.sortId,
    usePoints: data.usePoints,
    quotaRuleFlags: Array.from(new Set(data.quotaRules.ruleFlags)),
    applicants: data.applicants,
  });

  return mapDrawConfigEntityToDrawConfig(entity);
};

export const updateDrawConfig = async (input: UpdateDrawConfigInput, token: Token): Promise<DrawConfig> => {
  const {
    sub: updatedBy,
    claims: { organizationId },
  } = token;

  const validation = UpdateDrawConfigInputSchema.safeParse(input);
  if (!validation.success) {
    throw new Error(`Invalid input: ${JSON.stringify(validation.error.flatten())}`, { cause: validation.error });
  }
  const data = validation.data;
  const { drawConfigId, ...updates } = data;

  const entity = await updateDrawConfigEntity({
    id: { organizationId, drawConfigId },
    updates: {
      name: updates.name,
      sortId: updates.sortId,
      usePoints: updates.usePoints,
      quotaRuleFlags: updates?.quotaRules?.ruleFlags ? Array.from(new Set(updates.quotaRules.ruleFlags)) : undefined,
      applicants: updates.applicants,
    },
    updatedBy,
  });

  return mapDrawConfigEntityToDrawConfig(entity);
};

export const getDrawConfig = async (id: string, token: Token): Promise<DrawConfig> => {
  validateDrawConfigId(id);

  const drawConfig = await getDrawConfigEntity({ drawConfigId: id, organizationId: token.claims.organizationId });

  if (!drawConfig) {
    throw new Error(`Draw config with ID ${id} does not exist.`);
  }

  return mapDrawConfigEntityToDrawConfig(drawConfig);
};

export const getDrawConfigForWorkflowNode = async (input: {
  workflowNodeId: string;
  organizationId: string;
}): Promise<DrawConfig | null> => {
  const drawConfig = await getDrawConfigEntityForWorkflowNode(input);
  return drawConfig ? mapDrawConfigEntityToDrawConfig(drawConfig) : null;
};

export const deleteDrawConfig = async (input: DeleteDrawConfigInput, token: Token): Promise<boolean> => {
  const { id: drawConfigId } = input;

  validateDrawConfigId(drawConfigId);

  const { organizationId } = token.claims;

  return await deleteDrawConfigEntity({ organizationId, drawConfigId });
};

export const deleteDrawConfigForWorkflowNode = async (
  input: {
    workflowNodeId: string;
  },
  token: Token,
): Promise<boolean> => {
  const { workflowNodeId } = input;
  const { organizationId } = token.claims;

  const drawConfig = await getDrawConfigEntityForWorkflowNode({ workflowNodeId, organizationId });
  if (!drawConfig) {
    return false; // No draw config found for the given workflow node
  }
  return await deleteDrawConfigEntity({ organizationId, drawConfigId: drawConfig.id });
};

const mapDrawConfigEntityToDrawConfig = (entity: DrawConfigEntity): DrawConfig => ({
  id: entity.id,
  name: entity.name,
  sortId: entity.sortId,
  usePoints: entity.usePoints,
  quotaRules: {
    ruleFlags: entity.quotaRuleFlags ?? [],
  },
  applicants: entity.applicants,
});

const validateDrawConfigId = (id: string): void => {
  const validation = DrawConfigID.safeParse(id);
  if (!validation.success) {
    throw new Error(`Invalid input: ${JSON.stringify(validation.error.flatten())}`, { cause: validation.error });
  }
};

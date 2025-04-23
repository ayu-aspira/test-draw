import {
  createDrawCategoryEntity,
  deleteDrawCategoryEntity,
  getDrawCategoryEntity,
  listDrawCategoryEntities,
  updateDrawCategoryEntity,
} from "@/dynamo/draw-category";
import { listAllDrawWorkflowEntitiesForDrawCategory } from "@/dynamo/draw-workflow";
import { DynamoDBTypename } from "@/dynamo/dynamo";
import { deleteDrawWorkflow } from "@/service/draw-workflow";
import { GraphQLTypename, batchResolved } from "@/service/graphql";
import type { Token } from "@aspira-nextgen/core/authn";
import { addTypename, buildCreateMetadata } from "@aspira-nextgen/core/dynamodb";
import type {
  CreateDrawCategoryInput,
  DeleteDrawCategoryInput,
  DrawCategory,
  DrawCategoryConnection,
  ListDrawCategoriesInput,
  UpdateDrawCategoryInput,
} from "@aspira-nextgen/graphql/resolvers";
import { z } from "zod";

const UpdateDrawCategoryInputSchema = z.object({
  id: z.string(),
  name: z.string().optional(),
});

export const createDrawCategory = async (input: CreateDrawCategoryInput, token: Token): Promise<DrawCategory> => {
  const drawCategoryEntity = await createDrawCategoryEntity(
    {
      ...input,
      ...buildCreateMetadata(token.sub),
    },
    token,
  );

  return {
    ...drawCategoryEntity,
    draws: batchResolved(), // Returning empty because a field-level resolver is used to fetch draws.
    workflows: batchResolved(),
    __typename: DynamoDBTypename.DRAW_CATEGORY,
  };
};

export const getDrawCategory = async (drawCategoryId: string, token: Token): Promise<DrawCategory> => {
  const { organizationId } = token.claims;

  const drawCategoryEntity = await getDrawCategoryEntity({ drawCategoryId, organizationId });

  return {
    ...drawCategoryEntity,
    draws: batchResolved(), // Returning empty because a field-level resolver is used to fetch draws.
    workflows: batchResolved(),
    __typename: DynamoDBTypename.DRAW_CATEGORY,
  };
};

export const listDrawCategories = async (
  input: ListDrawCategoriesInput,
  token: Token,
): Promise<DrawCategoryConnection> => {
  const organizationId = token.claims.organizationId;

  const results = await listDrawCategoryEntities(organizationId, input);

  const items = addTypename(DynamoDBTypename.DRAW_CATEGORY, results.items).map((item) => ({
    ...item,
    draws: batchResolved(), // Returning empty because a field-level resolver is used to fetch draws.
    workflows: batchResolved(),
  }));

  return {
    items: items as DrawCategory[],
    nextToken: results.nextToken,
    __typename: GraphQLTypename.DRAW_CATEGORY_CONNECTION,
  };
};

export const deleteDrawCategory = async (input: DeleteDrawCategoryInput, token: Token): Promise<boolean> => {
  const { id: drawCategoryId } = input;

  await deleteDrawCategoryEntity({ drawCategoryId, token });
  return true;
};

export const deleteDrawWorkflowEntitiesByDrawCategoryId = async (
  { drawCategoryId }: { drawCategoryId: string },
  token: Token,
) => {
  const {
    claims: { organizationId },
  } = token;

  const workflows = await listAllDrawWorkflowEntitiesForDrawCategory({
    drawCategoryId,
    organizationId,
  });
  await Promise.all(workflows.map((drawWorkflow) => deleteDrawWorkflow({ id: drawWorkflow.id }, token)));
};

export const updateDrawCategory = async (input: UpdateDrawCategoryInput, token: Token): Promise<DrawCategory> => {
  const { id: drawCategoryId } = input;
  const {
    claims: { organizationId },
    sub: updatedBy,
  } = token;

  const validation = UpdateDrawCategoryInputSchema.safeParse(input);
  if (!validation.success) {
    throw new Error(`Invalid input: ${JSON.stringify(validation.error.flatten())}`, { cause: validation.error });
  }

  const entity = await updateDrawCategoryEntity({
    id: { drawCategoryId, organizationId },
    updates: validation.data as z.infer<typeof UpdateDrawCategoryInputSchema>,
    updatedBy,
  });

  return {
    ...entity,
    draws: batchResolved(), // Returning empty because a field-level resolver is used to fetch draws.
    workflows: batchResolved(),
    __typename: DynamoDBTypename.DRAW_CATEGORY,
  };
};

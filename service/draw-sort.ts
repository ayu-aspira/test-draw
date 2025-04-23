import {
  createDrawSortEntity,
  deleteDrawSortEntity,
  getDrawSortEntity,
  listDrawSortEntities,
  updateDrawSortEntity,
} from "@/dynamo/draw-sort";
import { ApplicantSortOptions } from "@/s3/applicant";
import { CreateDrawSortInputSchema, DrawSortID, UpdateDrawSortInputSchema } from "@/validation/draw-sort";
import type { Token } from "@aspira-nextgen/core/authn";
import type {
  CreateDrawSortInput,
  DeleteDrawSortInput,
  DrawApplicantSortOption,
  DrawSort,
  DrawSortConnection,
  ListDrawSortsInput,
  UpdateDrawSortInput,
} from "@aspira-nextgen/graphql/resolvers";
import type { z } from "zod";

export const getApplicantDrawSortOptions = (): DrawApplicantSortOption[] => {
  return ApplicantSortOptions;
};

export const createDrawSort = async (input: CreateDrawSortInput, token: Token): Promise<DrawSort> => {
  const validation = CreateDrawSortInputSchema.safeParse(input);

  if (!validation.success) {
    throw new Error(`Invalid input: ${JSON.stringify(validation.error.flatten())}`, { cause: validation.error });
  }

  const drawSort = await createDrawSortEntity({
    organizationId: token.claims.organizationId,
    name: input.name,
    rules: input.rules,
    createdBy: token.sub,
  });

  return drawSort;
};

export const updateDrawSort = async (input: UpdateDrawSortInput, token: Token): Promise<DrawSort> => {
  const { id: drawSortId } = input;

  const validation = UpdateDrawSortInputSchema.safeParse(input);

  if (!validation.success) {
    throw new Error(`Invalid input: ${JSON.stringify(validation.error.flatten())}`, { cause: validation.error });
  }

  const updatedDrawSort = await updateDrawSortEntity({
    id: {
      organizationId: token.claims.organizationId,
      drawSortId,
    },
    updates: validation.data as z.infer<typeof UpdateDrawSortInputSchema>,
    updatedBy: token.sub,
  });

  return updatedDrawSort;
};

export const getDrawSort = async (input: {
  id: string;
  organizationId: string;
}): Promise<DrawSort> => {
  const { id, organizationId } = input;

  validateDrawSortId(id);

  const drawSort = await getDrawSortEntity({
    id,
    organizationId,
  });

  if (!drawSort) {
    throw new Error("Draw sort not found");
  }

  return drawSort;
};

export const listDrawSorts = async (input: ListDrawSortsInput, token: Token): Promise<DrawSortConnection> => {
  return await listDrawSortEntities({
    organizationId: token.claims.organizationId,
    paginationRequest: input,
  });
};

export const deleteDrawSort = async (input: DeleteDrawSortInput, token: Token): Promise<boolean> => {
  const { id: drawSortId } = input;

  validateDrawSortId(drawSortId);

  const { organizationId } = token.claims;

  return await deleteDrawSortEntity({ organizationId, drawSortId });
};

const validateDrawSortId = (id: string): void => {
  const validation = DrawSortID.safeParse(id);

  if (!validation.success) {
    throw new Error(`Invalid input: ${JSON.stringify(validation.error.flatten())}`, { cause: validation.error });
  }
};

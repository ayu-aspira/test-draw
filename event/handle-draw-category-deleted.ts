import type { Token } from "@aspira-nextgen/core/authn";
import { deleteDrawWorkflowByDrawCategoryId } from "#service/draw-workflow.ts";

export const handleDeleteDrawCategory = async (input: { drawCategoryId: string }, token: Token) => {
  return await deleteDrawWorkflowByDrawCategoryId(input, token);
};

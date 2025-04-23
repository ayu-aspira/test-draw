import { deleteDrawConfigForWorkflowNode } from "@/service/draw-config";
import type { Token } from "@aspira-nextgen/core/authn";

export const handleDeleteWorkflowDrawNode = async (input: { workflowNodeId: string }, token: Token) => {
  await deleteDrawConfigForWorkflowNode(input, token);
};

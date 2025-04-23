import { deleteWorkflowInstancesByWorkflowId } from "@/service/workflow";
import type { Token } from "@aspira-nextgen/core/authn";

export const handleDeleteWorkflow = async (input: { workflowId: string }, token: Token) => {
  return await deleteWorkflowInstancesByWorkflowId(input, token);
};

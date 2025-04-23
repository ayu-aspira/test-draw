import { deleteWorkflowEdgesByWorkflowInstanceId } from "@/service/workflow-edge";
import { deleteWorkflowNodesByWorkflowInstanceId } from "@/service/workflow-node";
import type { Token } from "@aspira-nextgen/core/authn";

export const handleDeleteWorkflowInstance = async (input: { workflowInstanceId: string }, token: Token) => {
  await deleteWorkflowNodesByWorkflowInstanceId(input, token);
  await deleteWorkflowEdgesByWorkflowInstanceId(input, token);
};

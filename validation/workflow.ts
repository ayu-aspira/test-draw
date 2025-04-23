import { z } from "zod";
import { DynamoDBPrefix } from "#dynamo/dynamo.ts";
import { idStringPrefix } from "#validation/common.ts";

export { isWorkflowAcyclic } from "./workflow-is-acyclic";

export const WorkflowID = idStringPrefix(DynamoDBPrefix.WORKFLOW);
export const WorkflowNodeID = idStringPrefix(DynamoDBPrefix.WORKFLOW_NODE);
export const WorkflowEdgeID = idStringPrefix(DynamoDBPrefix.WORKFLOW_EDGE);

export const WorkflowPositionSchema = z.object({
  x: z.number(),
  y: z.number(),
});

export const CreateWorkflowNodeInputSchema = z.object({
  workflowId: WorkflowID,
  type: z.string(),
  position: WorkflowPositionSchema.nullish(),
});

export const UpdateWorkflowNodeInputSchema = CreateWorkflowNodeInputSchema.partial()
  .omit({ workflowId: true, type: true })
  .extend({
    id: WorkflowNodeID,
  });

export const CreateWorkflowEdgeInputSchema = z.object({
  workflowId: WorkflowID,
  sourceNodeId: WorkflowNodeID,
  sourceHandlePosition: WorkflowPositionSchema.nullish(),
  sourceHandle: z.string().nullish(),
  targetNodeId: WorkflowNodeID,
  targetHandlePosition: WorkflowPositionSchema.nullish(),
  targetHandle: z.string().nullish(),
});

export const UpdateWorkflowEdgeInputSchema = CreateWorkflowEdgeInputSchema.partial().omit({ workflowId: true }).extend({
  id: WorkflowEdgeID,
});

export const DeleteWorkflowEdgeInputSchema = z.object({
  id: WorkflowEdgeID,
});

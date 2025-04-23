import { wrapWorkflowHandler } from "@/step-function/workflow-node-utils";

/**
 * Handler that simply passes the data from the node override and result data onward.
 * Primarily used for testing purposes.
 */

// biome-ignore lint/suspicious/useAwait: not needed but required by the handler.
export const workflowNodeHandler = wrapWorkflowHandler(async (event) => {
  return event.nodeResultData[event.context.previousNodeId ?? ""] ?? [];
});

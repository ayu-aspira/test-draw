import { createWorkflowNodeResultEntity } from "@/dynamo/workflow-node-result";
import { logger } from "@/logger/logger";
import {
  LoggableWorkflowError,
  type WorkflowNodeContext,
  type WorkflowNodeData,
  type WorkflowNodeHandler,
  type WorkflowNodePayload,
} from "@/step-function/workflow-types";
import { type WorkflowDomain, WorkflowLogLevel } from "@aspira-nextgen/graphql/resolvers";
import type { Handler } from "aws-lambda";
import { createWorkflowJobLogEntity } from "#dynamo/workflow-job.ts";

/**
 * A utility function to log a message at the info level with the context of the workflow node.
 * @param context
 * @param message
 * @returns
 */
export const logInfo = (context: WorkflowNodeContext, message: string): void =>
  logger.info(`[${context.workflowJobId}-${context.nodeId}]: ${message}`);

type WorkflowWrappedNodeHandler = (
  event: Parameters<Handler<WorkflowNodePayload>>[0],
  context: Parameters<Handler<WorkflowNodePayload>>[1],
  callback: Parameters<Handler<WorkflowNodePayload>>[2],
) => Promise<WorkflowNodePayload>;

/**
 * Wraps a workflow handler with the logic to:
 * 1. Save the result of the handler to the database.
 * 2. Control the payload that is passed to the next node in the workflow.
 * 3. Capture exceptions and save them to the database.
 */
export const wrapWorkflowHandler =
  (handler: WorkflowNodeHandler): WorkflowWrappedNodeHandler =>
  async (event, context) => {
    const { context: ourContext } = event;

    const { organizationId, workflowJobId, nodeId: workflowNodeId } = ourContext;

    const result = await (async () => {
      try {
        return await handler(event, context, () => {});
      } catch (err) {
        if (err instanceof LoggableWorkflowError) {
          await createWorkflowJobLogEntity({
            organizationId,
            workflowJobId,
            workflowNodeId,
            createdBy: "system",
            level: WorkflowLogLevel.Error,
            messageKey: err.messageKey,
            messageParams: err.messageParams,
          });
        }

        throw new Error(`Error in workflow node handler: ${workflowJobId} ${ourContext.nodeId}`, { cause: err });
      }
    })();

    await createWorkflowNodeResultEntity({
      organizationId: ourContext.organizationId,
      nodeId: ourContext.nodeId,
      workflowJobId,
      results: result,
      createdBy: "system",
    });

    return {
      ...event,
      context: {
        ...ourContext,
        previousNodeId: ourContext.nodeId,
      },
      nodeResultData: {
        ...event.nodeResultData,
        [ourContext.nodeId]: result,
      },
    };
  };

const findDataSource = (nodeData: WorkflowNodeData[], domain: WorkflowDomain, domainModel: string) =>
  (nodeData ?? []).find((d) => d.mime.domain === domain && d.mime.domainModel === domainModel);

/**
 * Given a workflow node payload, domain, and domain model, find the workflow data source that matches.
 *
 * @param input
 * @param domain
 * @param domainModel
 * @returns
 */
export const findWorkflowDataSource = (input: {
  payload: WorkflowNodePayload;
  domain: WorkflowDomain;
  domainModel: string;
}): WorkflowNodeData => {
  const {
    payload: {
      context: { previousNodeId, nodeId },
      nodeOverrideData,
      nodeResultData,
    },
    domain,
    domainModel,
  } = input;

  // Consult the overriden data first, then the result data.
  let nodeData = nodeOverrideData[nodeId];
  let dataSource = findDataSource(nodeData, domain, domainModel);

  if (!dataSource) {
    // When looking at results, we always look at the previous node.
    nodeData = nodeResultData[previousNodeId ?? ""];
    dataSource = findDataSource(nodeData, domain, domainModel);
  }

  if (!dataSource) {
    throw new Error(
      `Data source ${domain} and model ${domainModel} not found for previous node (${previousNodeId}) nor current node (${nodeId}).`,
    );
  }

  return dataSource;
};

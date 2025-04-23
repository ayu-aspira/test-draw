import { handleDeleteDrawCategory } from "@/event/handle-draw-category-deleted";
import { handleDeleteWorkflow } from "@/event/handle-workflow-deleted";
import { handleDeleteWorkflowDrawNode } from "@/event/handle-workflow-draw-node-deleted";
import { handleDeleteWorkflowInstance } from "@/event/handle-workflow-instance-deleted";
import { logger, tracer } from "@/logger/logger";
import type { Token } from "@aspira-nextgen/core/authn";
import type { BaseEntity } from "@aspira-nextgen/core/dynamodb";
import { wrapHandlerWithLoggerAndTracer } from "@aspira-nextgen/core/logger";
import type { DomainEvent, Snapshot } from "@aspira-nextgen/feature-domain-events";
import type { EventBridgeHandler } from "aws-lambda";
import { DrawDomainEvent } from "#lib/index.ts";

export const handlerFn: EventBridgeHandler<string, DomainEvent<Snapshot<BaseEntity>>, void> = async (event) => {
  const detailType = event["detail-type"];

  switch (detailType) {
    case DrawDomainEvent.DrawCategoryDeleted: {
      const { id, token } = extractIdsForDelete(event.detail);
      await handleDeleteDrawCategory({ drawCategoryId: id }, token);
      break;
    }
    case DrawDomainEvent.WorkflowDeleted: {
      const { id, token } = extractIdsForDelete(event.detail);
      await handleDeleteWorkflow({ workflowId: id }, token);
      break;
    }
    case DrawDomainEvent.WorkflowInstanceDeleted: {
      const { id, token } = extractIdsForDelete(event.detail);
      await handleDeleteWorkflowInstance({ workflowInstanceId: id }, token);
      break;
    }
    case DrawDomainEvent.WorkflowDrawNodeDeleted: {
      const { id, token } = extractIdsForDelete(event.detail);
      await handleDeleteWorkflowDrawNode({ workflowNodeId: id }, token);
      break;
    }
    default:
      logger.warn("Unhandled detail type:", detailType);
  }
};

const extractIdsForDelete = (detail: DomainEvent<Snapshot<BaseEntity>>) => {
  const { data } = detail;
  const { old } = data;

  if (!old) {
    throw new Error("Old data not found in detail");
  }

  const { pk: organizationId, sk: id } = old;
  const token = { claims: { organizationId }, sub: "system" } as Token;

  return { organizationId, id, token };
};

export const handler = wrapHandlerWithLoggerAndTracer(handlerFn, logger, tracer);

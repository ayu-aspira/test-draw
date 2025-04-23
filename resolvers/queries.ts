import { batchListDrawsForDrawCategories, getDraw } from "@/service/draw";
import { getDrawCategory, listDrawCategories } from "@/service/draw-category";
import { getDrawConfig, getDrawConfigForWorkflowNode } from "@/service/draw-config";
import {
  getDrawDocument,
  getDrawDocumentByWorkflowNodeId,
  prepareDrawDocumentCollectionDownload,
  prepareDrawDocumentDownload,
} from "@/service/draw-document";
import { listDrawSorts } from "@/service/draw-sort";
import { getApplicantDrawSortOptions, getDrawSort } from "@/service/draw-sort";
import {
  batchListDrawWorkflowsForDrawCategories,
  getDrawWorkflow,
  listDrawWorkflowDataDefinitions,
} from "@/service/draw-workflow";
import { getWorkflowJob } from "@/service/workflow";
import { batchListEdgesForWorkflows } from "@/service/workflow-edge";
import { batchListNodesForWorkflows, getWorkflowNode } from "@/service/workflow-node";
import { getOrganizationId, getToken } from "@aspira-nextgen/core/authn";
import type { Resolvers as BatchResolvers } from "@aspira-nextgen/graphql/batch-resolvers";
import type { Resolvers } from "@aspira-nextgen/graphql/resolvers";

export const queries: Resolvers = {
  Query: {
    draw: ({ arguments: { id }, identity }) => getDraw(id, getToken(identity)),
    drawDocument: ({ arguments: { id }, identity }) => getDrawDocument(id, getOrganizationId(identity)),
    prepareDrawDocumentDownload: ({ arguments: { id }, identity }) =>
      prepareDrawDocumentDownload(id, getToken(identity)),
    prepareDrawDocumentCollectionDownload: ({ arguments: { id }, identity }) =>
      prepareDrawDocumentCollectionDownload(id, getToken(identity)),
    drawCategory: ({ arguments: { id }, identity }) => getDrawCategory(id, getToken(identity)),
    drawCategories: ({ arguments: { input }, identity }) => listDrawCategories(input, getToken(identity)),
    drawApplicantSortOptions: () => getApplicantDrawSortOptions(),
    drawSort: ({ arguments: { id }, identity }) => getDrawSort({ id, organizationId: getOrganizationId(identity) }),
    drawSorts: ({ arguments: { input }, identity }) => listDrawSorts(input, getToken(identity)),
    drawConfig: ({ arguments: { id }, identity }) => getDrawConfig(id, getToken(identity)),
    drawWorkflow: ({ arguments: { id }, identity }) => getDrawWorkflow(id, getToken(identity)),
    workflowNode: ({ arguments: { id }, identity }) => getWorkflowNode(id, getToken(identity)),
    workflowJob: ({ arguments: { id }, identity }) => getWorkflowJob(id, getToken(identity)),
    drawWorkflowDataDefinitions: () => listDrawWorkflowDataDefinitions(),
  },

  WorkflowDrawNode: {
    config: ({ source: { id }, identity }) =>
      getDrawConfigForWorkflowNode({ workflowNodeId: id, organizationId: getOrganizationId(identity) }),
  },

  WorkflowDrawApplicantDataNode: {
    document: ({ source: { id }, identity }) =>
      getDrawDocumentByWorkflowNodeId({ workflowNodeId: id, organizationId: getOrganizationId(identity) }),
  },

  WorkflowDrawDataNode: {
    document: ({ source: { id }, identity }) =>
      getDrawDocumentByWorkflowNodeId({ workflowNodeId: id, organizationId: getOrganizationId(identity) }),
  },
};

export const batchQueries: BatchResolvers = {
  DrawCategory: {
    draws: (event) =>
      batchListDrawsForDrawCategories(
        event.map((e) => ({
          drawCategory: e.source,
          listDrawsInput: e.arguments.input,
        })),
        getToken(event[0].identity),
      ),
    workflows: (event) =>
      batchListDrawWorkflowsForDrawCategories(
        event.map((e) => ({
          drawCategory: e.source,
          paginationRequest: e.arguments.input,
        })),
        getToken(event[0].identity),
      ),
  },
  Workflow: {
    nodes: (event) =>
      batchListNodesForWorkflows(
        event.map((e) => ({
          workflow: e.source,
          listWorkflowNodesInput: e.arguments.input,
        })),
        getToken(event[0].identity),
      ),
    edges: (event) =>
      batchListEdgesForWorkflows(
        event.map((e) => ({
          workflow: e.source,
          listWorkflowEdgesInput: e.arguments.input,
        })),
        getToken(event[0].identity),
      ),
  },
};

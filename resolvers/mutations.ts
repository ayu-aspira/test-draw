import { deleteDraw } from "@/service/draw";
import { createDraw, updateDraw } from "@/service/draw";
import { createDrawCategory, deleteDrawCategory, updateDrawCategory } from "@/service/draw-category";
import { createDrawConfig, deleteDrawConfig, updateDrawConfig } from "@/service/draw-config";
import { deleteDrawDocument, prepareDrawDocumentUpload } from "@/service/draw-document";
import { createDrawSort, deleteDrawSort, updateDrawSort } from "@/service/draw-sort";
import { createDrawWorkflow, deleteDrawWorkflow, updateDrawWorkflow } from "@/service/draw-workflow";
import { queueWorkflowJob } from "@/service/workflow";
import { createWorkflowEdge, deleteWorkflowEdge, updateWorkflowEdge } from "@/service/workflow-edge";
import {
  createWorkflowDataNode,
  createWorkflowNode,
  deleteWorkflowNode,
  updateWorkflowNode,
} from "@/service/workflow-node";
import { prepareWorkflowNodeResultDownload } from "@/service/workflow-node-result";
import { getToken } from "@aspira-nextgen/core/authn";
import type { Resolvers } from "@aspira-nextgen/graphql/resolvers";

export const mutations: Resolvers = {
  Mutation: {
    createDrawWorkflow: ({ arguments: { input }, identity }) => createDrawWorkflow(input, getToken(identity)),
    updateDrawWorkflow: ({ arguments: { input }, identity }) => updateDrawWorkflow(input, getToken(identity)),
    deleteDrawWorkflow: ({ arguments: { input }, identity }) => deleteDrawWorkflow(input, getToken(identity)),
    prepareDrawDocumentUpload: ({ arguments: { input }, identity }) =>
      prepareDrawDocumentUpload(input, getToken(identity)),
    prepareWorkflowNodeResultDownload: ({ arguments: { input }, identity }) =>
      prepareWorkflowNodeResultDownload(input, getToken(identity)),
    deleteDrawDocument: ({ arguments: { input }, identity }) => deleteDrawDocument(input, getToken(identity)),
    createDraw: ({ arguments: { input }, identity }) => createDraw(input, getToken(identity)),
    updateDraw: ({ arguments: { input }, identity }) => updateDraw(input, getToken(identity)),
    deleteDraw: ({ arguments: { input }, identity }) => deleteDraw(input, getToken(identity)),
    createDrawCategory: ({ arguments: { input }, identity }) => createDrawCategory(input, getToken(identity)),
    deleteDrawCategory: ({ arguments: { input }, identity }) => deleteDrawCategory(input, getToken(identity)),
    updateDrawCategory: ({ arguments: { input }, identity }) => updateDrawCategory(input, getToken(identity)),
    createWorkflowNode: ({ arguments: { input }, identity }) => createWorkflowNode(input, getToken(identity)),
    createWorkflowDataNode: ({ arguments: { input }, identity }) => createWorkflowDataNode(input, getToken(identity)),
    updateWorkflowNode: ({ arguments: { input }, identity }) => updateWorkflowNode(input, getToken(identity)),
    deleteWorkflowNode: ({ arguments: { input }, identity }) => deleteWorkflowNode(input, getToken(identity)),
    createWorkflowEdge: ({ arguments: { input }, identity }) => createWorkflowEdge(input, getToken(identity)),
    updateWorkflowEdge: ({ arguments: { input }, identity }) => updateWorkflowEdge(input, getToken(identity)),
    deleteWorkflowEdge: ({ arguments: { input }, identity }) => deleteWorkflowEdge(input, getToken(identity)),
    queueWorkflowJob: ({ arguments: { input }, identity }) => queueWorkflowJob(input, getToken(identity)),
    createDrawSort: ({ arguments: { input }, identity }) => createDrawSort(input, getToken(identity)),
    updateDrawSort: ({ arguments: { input }, identity }) => updateDrawSort(input, getToken(identity)),
    deleteDrawSort: ({ arguments: { input }, identity }) => deleteDrawSort(input, getToken(identity)),
    createDrawConfig: ({ arguments: { input }, identity }) => createDrawConfig(input, getToken(identity)),
    updateDrawConfig: ({ arguments: { input }, identity }) => updateDrawConfig(input, getToken(identity)),
    deleteDrawConfig: ({ arguments: { input }, identity }) => deleteDrawConfig(input, getToken(identity)),
  },
};

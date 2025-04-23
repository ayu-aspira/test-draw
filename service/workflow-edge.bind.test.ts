import { dynamoDBClient } from "@/dynamo/dynamo";
import { getWorkflowEdgeEntity } from "@/dynamo/workflow-edge";
import {
  createWorkflowEdge,
  deleteWorkflowEdge,
  deleteWorkflowEdgesByWorkflowInstanceId,
  listWorkflowEdgesByWorkflowId,
  updateWorkflowEdge,
} from "@/service/workflow-edge";
import {
  createRandomTestWorkflowId,
  createRandomTestWorkflowNodeId,
  createTestWorkflow,
  deleteAllS3TestDataByOrg,
} from "@/util/test-utils";
import { createTestWorkflowNode } from "@/util/test-utils";
import { buildTestToken } from "@aspira-nextgen/core/authn-test-utils";
import { deleteAllTableItemsByPk, withConsistencyRetries } from "@aspira-nextgen/core/dynamodb-test-utils";
import type { CreateWorkflowEdgeInput } from "@aspira-nextgen/graphql/resolvers";
import { Table } from "sst/node/table";
import { ulid } from "ulidx";
import { afterEach, describe, expect, it } from "vitest";
import { getLatestWorkflowInstanceEntity } from "#dynamo/workflow.ts";

const ORG_ID = `org_${ulid()}`;
const TOKEN = buildTestToken(ORG_ID);

describe("Workflow Edge Service Tests", () => {
  afterEach(async () => {
    await deleteAllTableItemsByPk(dynamoDBClient, Table.Draw.tableName, [ORG_ID]);
    await deleteAllS3TestDataByOrg(ORG_ID);
  });

  describe("Create Workflow Edge", () => {
    it("Should create a workflow node", async () => {
      const node1 = await createTestWorkflowNode(TOKEN);
      const { workflowId } = node1;
      const node2 = await createTestWorkflowNode(TOKEN, { workflowId });

      const input: CreateWorkflowEdgeInput = {
        workflowId,
        sourceNodeId: node1.id,
        sourceHandlePosition: { x: Math.floor(Math.random() * 100), y: Math.floor(Math.random() * 100) },
        sourceHandle: `source_handle_${ulid()}`,
        targetNodeId: node2.id,
        targetHandlePosition: { x: Math.floor(Math.random() * 100), y: Math.floor(Math.random() * 100) },
        targetHandle: `target_handle_${ulid()}`,
      };

      const entity = await createWorkflowEdge(input, TOKEN);
      const edge = await getWorkflowEdgeEntity({ organizationId: ORG_ID, workflowEdgeId: entity.id });

      expect(edge).toMatchObject({
        id: expect.stringMatching(/^workflow_edge_/),
        workflowId,
        sourceNodeId: input.sourceNodeId,
        sourceHandlePosition: input.sourceHandlePosition,
        sourceHandle: input.sourceHandle,
        targetNodeId: input.targetNodeId,
        targetHandlePosition: input.targetHandlePosition,
        targetHandle: input.targetHandle,
      });
    });

    it("Should ensure that the workflow exists", async () => {
      const node1 = await createTestWorkflowNode(TOKEN);
      const { workflowId } = node1;
      const node2 = await createTestWorkflowNode(TOKEN, { workflowId });

      const input: CreateWorkflowEdgeInput = {
        workflowId: createRandomTestWorkflowId(),
        sourceNodeId: node1.id,
        targetNodeId: node2.id,
      };

      await expect(createWorkflowEdge(input, TOKEN)).rejects.toThrowError("Provided workflow ID does not exist.");
    });

    it("Should ensure that the source node exists", async () => {
      const node1 = await createTestWorkflowNode(TOKEN);
      const { workflowId } = node1;

      const input: CreateWorkflowEdgeInput = {
        workflowId,
        sourceNodeId: createRandomTestWorkflowNodeId(),
        targetNodeId: node1.id,
      };

      await expect(createWorkflowEdge(input, TOKEN)).rejects.toThrowError("Provided source node ID does not exist.");
    });

    it("Should ensure that the target node exists", async () => {
      const node1 = await createTestWorkflowNode(TOKEN);
      const { workflowId } = node1;

      const input: CreateWorkflowEdgeInput = {
        workflowId,
        sourceNodeId: node1.id,
        targetNodeId: createRandomTestWorkflowNodeId(),
      };

      await expect(createWorkflowEdge(input, TOKEN)).rejects.toThrowError("Provided target node ID does not exist.");
    });
  });

  describe("Update Workflow Edge", () => {
    it("Should ensure that the source node exists", async () => {
      const node1 = await createTestWorkflowNode(TOKEN);
      const { workflowId } = node1;
      const node2 = await createTestWorkflowNode(TOKEN, { workflowId });

      const input: CreateWorkflowEdgeInput = {
        workflowId,
        sourceNodeId: node1.id,
        targetNodeId: node2.id,
      };
      const entity = await createWorkflowEdge(input, TOKEN);
      const workflowEdgeId = entity.id;

      await expect(
        updateWorkflowEdge({ id: workflowEdgeId, sourceNodeId: createRandomTestWorkflowNodeId() }, TOKEN),
      ).rejects.toThrowError("Provided source node ID does not exist.");
    });

    it("Should update a workflow edge sourceHandlePosition", async () => {
      const node1 = await createTestWorkflowNode(TOKEN);
      const { workflowId } = node1;
      const node2 = await createTestWorkflowNode(TOKEN, { workflowId });

      const input: CreateWorkflowEdgeInput = {
        workflowId,
        sourceNodeId: node1.id,
        targetNodeId: node2.id,
      };
      const entity = await createWorkflowEdge(input, TOKEN);
      const workflowEdgeId = entity.id;

      let position = { x: Math.floor(Math.random() * 100), y: Math.floor(Math.random() * 100) };
      expect(await updateWorkflowEdge({ id: workflowEdgeId, sourceHandlePosition: position }, TOKEN)).toMatchObject({
        ...entity,
        sourceHandlePosition: position,
      });

      position = { x: Math.floor(Math.random() * 100), y: Math.floor(Math.random() * 100) };
      expect(await updateWorkflowEdge({ id: workflowEdgeId, sourceHandlePosition: position }, TOKEN)).toMatchObject({
        ...entity,
        sourceHandlePosition: position,
      });

      expect(await updateWorkflowEdge({ id: workflowEdgeId, sourceHandlePosition: null }, TOKEN)).toMatchObject({
        ...entity,
      });
    });

    it("Should update a workflow edge sourceHandle", async () => {
      const node1 = await createTestWorkflowNode(TOKEN);
      const { workflowId } = node1;
      const node2 = await createTestWorkflowNode(TOKEN, { workflowId });

      const input: CreateWorkflowEdgeInput = {
        workflowId,
        sourceNodeId: node1.id,
        targetNodeId: node2.id,
      };
      const entity = await createWorkflowEdge(input, TOKEN);
      const workflowEdgeId = entity.id;

      let sourceHandle = `target_handle_${ulid()}`;
      expect(await updateWorkflowEdge({ id: workflowEdgeId, sourceHandle }, TOKEN)).toMatchObject({
        ...entity,
        sourceHandle,
      });

      sourceHandle = `target_handle_${ulid()}`;
      expect(await updateWorkflowEdge({ id: workflowEdgeId, sourceHandle }, TOKEN)).toMatchObject({
        ...entity,
        sourceHandle,
      });

      expect(await updateWorkflowEdge({ id: workflowEdgeId, sourceHandle: null }, TOKEN)).toMatchObject({
        ...entity,
      });
    });

    it("Should ensure that the target node exists", async () => {
      const node1 = await createTestWorkflowNode(TOKEN);
      const { workflowId } = node1;
      const node2 = await createTestWorkflowNode(TOKEN, { workflowId });

      const input: CreateWorkflowEdgeInput = {
        workflowId,
        sourceNodeId: node1.id,
        targetNodeId: node2.id,
      };
      const entity = await createWorkflowEdge(input, TOKEN);
      const workflowEdgeId = entity.id;

      await expect(
        updateWorkflowEdge({ id: workflowEdgeId, targetNodeId: createRandomTestWorkflowNodeId() }, TOKEN),
      ).rejects.toThrowError("Provided target node ID does not exist.");
    });

    it("Should update a workflow edge targetHandlePosition", async () => {
      const node1 = await createTestWorkflowNode(TOKEN);
      const { workflowId } = node1;
      const node2 = await createTestWorkflowNode(TOKEN, { workflowId });

      const input: CreateWorkflowEdgeInput = {
        workflowId,
        sourceNodeId: node1.id,
        targetNodeId: node2.id,
      };
      const entity = await createWorkflowEdge(input, TOKEN);
      const workflowEdgeId = entity.id;

      let position = { x: Math.floor(Math.random() * 100), y: Math.floor(Math.random() * 100) };
      expect(await updateWorkflowEdge({ id: workflowEdgeId, targetHandlePosition: position }, TOKEN)).toMatchObject({
        ...entity,
        targetHandlePosition: position,
      });

      position = { x: Math.floor(Math.random() * 100), y: Math.floor(Math.random() * 100) };
      expect(await updateWorkflowEdge({ id: workflowEdgeId, targetHandlePosition: position }, TOKEN)).toMatchObject({
        ...entity,
        targetHandlePosition: position,
      });

      expect(await updateWorkflowEdge({ id: workflowEdgeId, targetHandlePosition: null }, TOKEN)).toMatchObject({
        ...entity,
      });
    });

    it("Should throw an error if the ID is invalid", async () => {
      await expect(updateWorkflowEdge({ id: "invalid_id" }, TOKEN)).rejects.toThrowError(
        `Invalid input: {"formErrors":[],"fieldErrors":{"id":["Invalid ID"]}}`,
      );
    });

    it("Should update a workflow edge targetHandle", async () => {
      const node1 = await createTestWorkflowNode(TOKEN);
      const { workflowId } = node1;
      const node2 = await createTestWorkflowNode(TOKEN, { workflowId });

      const input: CreateWorkflowEdgeInput = {
        workflowId,
        sourceNodeId: node1.id,
        targetNodeId: node2.id,
      };
      const entity = await createWorkflowEdge(input, TOKEN);
      const workflowEdgeId = entity.id;

      let targetHandle = `target_handle_${ulid()}`;
      expect(await updateWorkflowEdge({ id: workflowEdgeId, targetHandle }, TOKEN)).toMatchObject({
        ...entity,
        targetHandle,
      });

      targetHandle = `target_handle_${ulid()}`;
      expect(await updateWorkflowEdge({ id: workflowEdgeId, targetHandle }, TOKEN)).toMatchObject({
        ...entity,
        targetHandle,
      });

      expect(await updateWorkflowEdge({ id: workflowEdgeId, targetHandle: null }, TOKEN)).toMatchObject({
        ...entity,
      });
    });
  });

  describe("Delete Workflow Edge", () => {
    it("Should delete a workflow edge", async () => {
      const node1 = await createTestWorkflowNode(TOKEN);
      const { workflowId } = node1;
      const node2 = await createTestWorkflowNode(TOKEN, { workflowId });

      const input: CreateWorkflowEdgeInput = {
        workflowId,
        sourceNodeId: node1.id,
        targetNodeId: node2.id,
      };
      const entity = await createWorkflowEdge(input, TOKEN);
      const { id } = entity;

      await deleteWorkflowEdge({ id }, TOKEN);

      await expect(getWorkflowEdgeEntity({ organizationId: ORG_ID, workflowEdgeId: id })).rejects.toThrowError(
        "Workflow edge not found",
      );
    });

    it("Should throw an error if the ID is invalid", async () => {
      await expect(deleteWorkflowEdge({ id: "invalid_id" }, TOKEN)).rejects.toThrowError(
        `Invalid input: {"formErrors":[],"fieldErrors":{"id":["Invalid ID"]}}`,
      );
    });
  });

  describe("List Workflow Edges", () => {
    it("Should list workflow edges", async () => {
      const {
        workflow: { id: workflowId },
      } = await createTestWorkflow(TOKEN);
      const node1 = await createTestWorkflowNode(TOKEN, { workflowId });
      const node2 = await createTestWorkflowNode(TOKEN, { workflowId });
      const node3 = await createTestWorkflowNode(TOKEN, { workflowId });

      const edge1 = await createWorkflowEdge(
        {
          workflowId,
          sourceNodeId: node1.id,
          targetNodeId: node2.id,
        },
        TOKEN,
      );
      const edge2 = await createWorkflowEdge(
        {
          workflowId,
          sourceNodeId: node2.id,
          targetNodeId: node3.id,
        },
        TOKEN,
      );

      await withConsistencyRetries(async () => {
        const results = await listWorkflowEdgesByWorkflowId(
          { workflowId, listWorkflowEdgesInput: { limit: 10, nextToken: null } },
          TOKEN,
        );

        expect(results.items).toHaveLength(2);
        expect(results.items).toContainEqual(expect.objectContaining({ id: edge1.id }));
        expect(results.items).toContainEqual(expect.objectContaining({ id: edge2.id }));
      });
    });
  });

  describe("Delete Workflow Edge", () => {
    it("Should delete workflow edges by workflow instance id", async () => {
      const {
        workflow: { id: workflowId },
      } = await createTestWorkflow(TOKEN);
      const node1 = await createTestWorkflowNode(TOKEN, { workflowId });
      const node2 = await createTestWorkflowNode(TOKEN, { workflowId });

      const workflowInstance = await getLatestWorkflowInstanceEntity({ organizationId: ORG_ID, workflowId });
      if (!workflowInstance) {
        throw new Error("Workflow instance not found");
      }
      const { id: workflowInstanceId } = workflowInstance;

      await createWorkflowEdge(
        {
          workflowId,
          sourceNodeId: node1.id,
          targetNodeId: node2.id,
        },
        TOKEN,
      );

      await withConsistencyRetries(async () => {
        const results = await listWorkflowEdgesByWorkflowId({ workflowId }, TOKEN);
        expect(results.items).toHaveLength(1);
      });

      await deleteWorkflowEdgesByWorkflowInstanceId({ workflowInstanceId }, TOKEN);

      await withConsistencyRetries(async () => {
        const results = await listWorkflowEdgesByWorkflowId({ workflowId }, TOKEN);
        expect(results.items).toHaveLength(0);
      });
    });
  });
});

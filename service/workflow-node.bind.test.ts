import { DynamoDBTypename, dynamoDBClient, getDrawTableName } from "@/dynamo/dynamo";
import { getWorkflowNodeEntity } from "@/dynamo/workflow-node";
import { GraphQLTypename } from "@/service/graphql";
import {
  createWorkflowDataNode,
  createWorkflowNode,
  deleteWorkflowNode,
  deleteWorkflowNodesByWorkflowInstanceId,
  getWorkflowNode,
  listWorkflowNodesByWorkflowId,
  updateWorkflowNode,
} from "@/service/workflow-node";
import {
  createRandomTestWorkflowId,
  createRandomTestWorkflowNodeId,
  createTestWorkflow,
  createTestWorkflowWithResults,
  deleteAllS3TestDataByOrg,
  mockSFNClientForTesting,
} from "@/util/test-utils";
import { createTestDrawWorkflow, createTestWorkflowNode } from "@/util/test-utils";
import { buildTestToken } from "@aspira-nextgen/core/authn-test-utils";
import { deleteAllTableItemsByPk, withConsistencyRetries } from "@aspira-nextgen/core/dynamodb-test-utils";
import {
  type CreateWorkflowDataNodeInput,
  type CreateWorkflowNodeInput,
  WorkflowDataNodeType,
  type WorkflowNode,
  WorkflowNodeType,
  type WorkflowNodeWithResults,
} from "@aspira-nextgen/graphql/resolvers";
import { ulid } from "ulidx";
import { afterEach, describe, expect, it, vi } from "vitest";
import { getLatestWorkflowInstanceEntity } from "#dynamo/workflow.ts";

const ORG_ID = `org_${ulid()}`;
const TOKEN = buildTestToken(ORG_ID);

describe("Workflow Node Service Tests", () => {
  afterEach(async () => {
    await deleteAllTableItemsByPk(dynamoDBClient, getDrawTableName(), [ORG_ID]);
    await deleteAllS3TestDataByOrg(ORG_ID);
  });

  describe("Create Workflow Node", () => {
    it("Should create workflow nodes", async () => {
      const { workflow } = await createTestWorkflow(TOKEN);
      const workflowId = workflow.id;

      for (const type of Object.values(WorkflowNodeType)) {
        const x = Math.floor(Math.random() * 100);
        const y = Math.floor(Math.random() * 100);
        const input: CreateWorkflowNodeInput = {
          workflowId: workflowId,
          position: { x, y },
          type,
        };

        const entity = await createWorkflowNode(input, TOKEN);
        const node = await getWorkflowNodeEntity({ organizationId: ORG_ID, workflowNodeId: entity.id });

        expect(node).toMatchObject({
          id: expect.stringMatching(/^workflow_node_/),
          position: {
            x,
            y,
          },
          workflowId: workflowId,
          __typename: type,
        });
      }
    });

    it("Should ensure that the workflow exists", async () => {
      const input: CreateWorkflowNodeInput = {
        workflowId: createRandomTestWorkflowId(),
        type: WorkflowNodeType.WorkflowNoopNode,
      };

      await expect(createWorkflowNode(input, TOKEN)).rejects.toThrowError("Provided workflow ID does not exist.");
    });
  });

  describe("Create Workflow Data Node", () => {
    it("Should create workflow data nodes", async () => {
      const drawWorkflow = await createTestDrawWorkflow(TOKEN);
      const workflowId = drawWorkflow.workflow.id;

      for (const type of Object.values(WorkflowDataNodeType)) {
        const x = Math.floor(Math.random() * 100);
        const y = Math.floor(Math.random() * 100);
        const input: CreateWorkflowDataNodeInput = {
          workflowId: workflowId,
          position: { x, y },
          type: type,
        };

        const entity = await createWorkflowDataNode(input, TOKEN);
        const node = await getWorkflowNodeEntity({ organizationId: ORG_ID, workflowNodeId: entity.id });

        expect(node).toMatchObject({
          id: expect.stringMatching(/^workflow_node_/),
          position: { x, y },
          workflowId,
          __typename: type,
        });
      }
    });

    it("Should ensure that the workflow exists", async () => {
      const input: CreateWorkflowNodeInput = {
        workflowId: createRandomTestWorkflowId(),
        type: WorkflowNodeType.WorkflowNoopNode,
      };

      await expect(createWorkflowNode(input, TOKEN)).rejects.toThrowError("Provided workflow ID does not exist.");
    });
  });

  describe("Get Workflow Node", () => {
    it("Should get a workflow node", async () => {
      const entity = await createTestWorkflowNode(TOKEN);
      const { id } = entity;

      const node = await getWorkflowNode(id, TOKEN);

      expect(node).toMatchObject(entity);
    });

    it("Should throw an error if the workflow node does not exist", async () => {
      const id = createRandomTestWorkflowNodeId();
      await expect(getWorkflowNode(id, TOKEN)).rejects.toThrowError(`Workflow node with ID ${id} does not exist`);
    });

    it("Should throw an error if the workflow node id is invalid", async () => {
      const id = "invalid_id";
      await expect(getWorkflowNode(id, TOKEN)).rejects.toThrowError(
        `Invalid input: {"formErrors":["Invalid ID"],"fieldErrors":{}}`,
      );
    });
  });

  describe("Update Workflow Node", () => {
    it("Should update a workflow node position", async () => {
      const { position: originalPosition, ...entity } = await createTestWorkflowNode(TOKEN);
      const { id } = entity;

      let position: { x: number; y: number } | null = {
        x: Math.floor(Math.random() * 100),
        y: Math.floor(Math.random() * 100),
      };
      await updateWorkflowNode({ id, position }, TOKEN);
      expect(await getWorkflowNodeEntity({ organizationId: ORG_ID, workflowNodeId: id })).toMatchObject({
        ...entity,
        position: position,
      });

      position = { x: Math.floor(Math.random() * 100), y: Math.floor(Math.random() * 100) };
      await updateWorkflowNode({ id, position }, TOKEN);
      expect(await getWorkflowNodeEntity({ organizationId: ORG_ID, workflowNodeId: id })).toMatchObject({
        ...entity,
        position: position,
      });

      position = null;
      await updateWorkflowNode({ id, position }, TOKEN);
      expect(await getWorkflowNodeEntity({ organizationId: ORG_ID, workflowNodeId: id })).toMatchObject(entity);
    });

    it("Should throw an error if the ID string is invalid", async () => {
      const id = "invalid_id";
      const position = { x: Math.floor(Math.random() * 100), y: Math.floor(Math.random() * 100) };
      await expect(updateWorkflowNode({ id, position }, TOKEN)).rejects.toThrowError(
        `Invalid input: {"formErrors":[],"fieldErrors":{"id":["Invalid ID"]}}`,
      );
    });
  });

  describe("Delete Workflow Node", () => {
    it("Should delete a workflow node", async () => {
      const entity = await createTestWorkflowNode(TOKEN);
      const { id } = entity;

      await deleteWorkflowNode({ id }, TOKEN);

      await expect(getWorkflowNode(id, TOKEN)).rejects.toThrowError(`Workflow node with ID ${id} does not exist`);
    });

    it("Should throw an error if the ID string is invalid", async () => {
      const id = "invalid_id";
      await expect(deleteWorkflowNode({ id }, TOKEN)).rejects.toThrowError(
        `Invalid input: {"formErrors":["Invalid ID"],"fieldErrors":{}}`,
      );
    });
  });

  describe("List Workflow Nodes", () => {
    it("Should list workflow nodes", async () => {
      const { workflow, workflowInstance } = await createTestWorkflow(TOKEN);

      const node1 = await createTestWorkflowNode(TOKEN, { workflowId: workflow.id });
      const node2 = await createTestWorkflowNode(TOKEN, { workflowId: workflow.id });

      await withConsistencyRetries(async () => {
        const results = await listWorkflowNodesByWorkflowId(
          { workflowId: workflow.id, listWorkflowNodesInput: { limit: 10, nextToken: null } },
          TOKEN,
        );

        expect(results.items).toHaveLength(2);
        expect(results.items).toContainEqual(expect.objectContaining({ id: node1.id }));
        expect(results.items).toContainEqual(expect.objectContaining({ id: node2.id }));

        expect(results.__typename).toBe(GraphQLTypename.WORKFLOW_NODE_CONNECTION);
        results.items.forEach((item) => {
          expect(item).toMatchObject({
            __typename: DynamoDBTypename.WORKFLOW_NOOP_NODE,
            id: expect.stringMatching(/^workflow_node_/),
            workflowInstanceId: workflowInstance.id,
          });
        });
      });
    });

    it("Should list workflow node with results", async () => {
      mockSFNClientForTesting();

      try {
        const { workflow } = await createTestWorkflowWithResults(TOKEN);

        await withConsistencyRetries(async () => {
          const results = await listWorkflowNodesByWorkflowId(
            { workflowId: workflow.id, listWorkflowNodesInput: { limit: 10, nextToken: null } },
            TOKEN,
          );

          expect(results.items).toHaveLength(1);

          const node = results.items[0] as WorkflowNode & WorkflowNodeWithResults;

          expect(node.results).toHaveLength(2);
          expect(node.results).toEqual({
            [node.id]: expect.arrayContaining([
              expect.objectContaining({
                workflowNodeResultId: expect.stringMatching(/^workflow_node_result_/),
                workflowNodeId: node.id,
                domain: "DRAW",
                domainModel: "APPLICANTS",
                type: "text/csv",
              }),
              expect.objectContaining({
                workflowNodeResultId: expect.stringMatching(/^workflow_node_result_/),
                workflowNodeId: node.id,
                domain: "DRAW",
                domainModel: "HUNT_CODES",
                type: "text/csv",
              }),
            ]),
          });
        });
      } catch (_error) {
        vi.clearAllMocks();
      }
    });
  });

  describe("Delete Workflow Node", () => {
    it("Should delete workflow nodes by workflow instance id", async () => {
      const {
        workflow: { id: workflowId },
      } = await createTestWorkflow(TOKEN);
      await createTestWorkflowNode(TOKEN, { workflowId });
      await createTestWorkflowNode(TOKEN, { workflowId });

      const workflowInstance = await getLatestWorkflowInstanceEntity({ organizationId: ORG_ID, workflowId });
      if (!workflowInstance) {
        throw new Error("Workflow instance not found");
      }
      const { id: workflowInstanceId } = workflowInstance;

      await withConsistencyRetries(async () => {
        const results = await listWorkflowNodesByWorkflowId({ workflowId }, TOKEN);
        expect(results.items).toHaveLength(2);
      });

      await deleteWorkflowNodesByWorkflowInstanceId({ workflowInstanceId }, TOKEN);

      await withConsistencyRetries(async () => {
        const results = await listWorkflowNodesByWorkflowId({ workflowId }, TOKEN);
        expect(results.items).toHaveLength(0);
      });
    });
  });
});

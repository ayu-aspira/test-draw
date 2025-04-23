import { DynamoDBTypename } from "@/dynamo/dynamo";
import { DynamoDBPrefix, dynamoDBClient } from "@/dynamo/dynamo";
import { WorkflowInstanceStatus, getLatestWorkflowInstanceEntity } from "@/dynamo/workflow";
import { getDrawCategory } from "@/service/draw-category";
import {
  createDrawWorkflow,
  deleteDrawWorkflow,
  deleteDrawWorkflowByDrawCategoryId,
  getDrawWorkflow,
  listDrawWorkflowsForDrawCategory,
  updateDrawWorkflow,
} from "@/service/draw-workflow";
import { deleteAllS3TestDataByOrg } from "@/util/test-utils";
import { createTestDrawCategory, createTestDrawWorkflow } from "@/util/test-utils";
import { buildTestToken } from "@aspira-nextgen/core/authn-test-utils";
import { deleteAllTableItemsByPk, withConsistencyRetries } from "@aspira-nextgen/core/dynamodb-test-utils";
import type { CreateDrawWorkflowInput } from "@aspira-nextgen/graphql/resolvers";
import {} from "@aws-sdk/client-sfn";
import { Table } from "sst/node/table";
import { ulid } from "ulidx";
import { afterEach, describe, expect, it, vi } from "vitest";

const ORG_ID = `org_${ulid()}`;
const TOKEN = buildTestToken(ORG_ID);

vi.mock("@aws-sdk/client-sfn", async () => {
  const actual = await vi.importActual("@aws-sdk/client-sfn");

  return {
    ...actual,
    SFNClient: vi.fn(),
  };
});

describe("Draw Workflow Service Tests", () => {
  afterEach(async () => {
    await deleteAllTableItemsByPk(dynamoDBClient, Table.Draw.tableName, [ORG_ID]);
    await deleteAllS3TestDataByOrg(ORG_ID);
  });

  describe("Create Draw Workflow", () => {
    it("Should create a workflow", async () => {
      const drawCategory = await createTestDrawCategory(TOKEN);

      const input: CreateDrawWorkflowInput = {
        name: "Test Workflow",
        drawCategoryId: drawCategory.id,
      };

      const drawWorkflow = await createDrawWorkflow(input, TOKEN);

      expect(drawWorkflow).toMatchObject({
        id: expect.stringMatching(/^draw_workflow_/),
        workflowId: expect.stringMatching(/^workflow_main_/),
        name: input.name,
        drawCategoryId: input.drawCategoryId,
        workflow: {
          __typename: "Workflow",
          id: expect.stringMatching(/^workflow_main_/),
        },
      });

      await withConsistencyRetries(async () => {
        const workflowInstance = await getLatestWorkflowInstanceEntity({
          organizationId: ORG_ID,
          workflowId: drawWorkflow.workflow.id,
        });

        expect(workflowInstance).toMatchObject({
          id: expect.stringMatching(/^workflow_instance_/),
          workflowId: expect.stringMatching(/^workflow_main_/),
          status: WorkflowInstanceStatus.BuildNeeded,
        });
      });
    });

    // Skipping as we removed the unique constraint on names
    it.skip("Should ensure that names are unique", async () => {
      const drawWorkflow = await createTestDrawWorkflow(TOKEN);

      const input: CreateDrawWorkflowInput = {
        name: drawWorkflow.name,
        drawCategoryId: drawWorkflow.drawCategoryId,
      };

      await expect(createDrawWorkflow(input, TOKEN)).rejects.toThrowError(
        "Draw workflow with the same name already exists.",
      );
    });

    it("Should ensure that the draw category exists", async () => {
      const input: CreateDrawWorkflowInput = {
        name: "Test Workflow",
        drawCategoryId: `${DynamoDBPrefix.DRAW_CATEGORY}_${ulid()}`,
      };

      await expect(createDrawWorkflow(input, TOKEN)).rejects.toThrowError("Provided draw category ID does not exist.");
    });
  });

  describe("Get DrawWorkflow Test", () => {
    it("Should get a draw category", async () => {
      const drawWorkflow = await createTestDrawWorkflow(TOKEN);

      const retrievedDrawWorkflow = await getDrawWorkflow(drawWorkflow.id, TOKEN);

      expect(retrievedDrawWorkflow).toMatchObject({
        id: drawWorkflow.id,
        name: drawWorkflow.name,
        drawCategoryId: drawWorkflow.drawCategoryId,
        __typename: DynamoDBTypename.DRAW_WORKFLOW,
        workflow: {
          id: drawWorkflow.workflow.id,
          __typename: DynamoDBTypename.WORKFLOW,
        },
      });
    });

    it("Should throw an error if a draw category does not exist.", async () => {
      await expect(getDrawWorkflow(ulid(), TOKEN)).rejects.toThrow("Draw workflow not found");
    });
  });

  describe("Update DrawWorkflow Test", () => {
    it("Should update a draw workflow name", async () => {
      const drawWorkflow = await createTestDrawWorkflow(TOKEN);

      const updatedName = `updated workflow ${ulid()}`;
      await updateDrawWorkflow(
        {
          id: drawWorkflow.id,
          name: updatedName,
        },
        TOKEN,
      );

      const updatedDrawWorkflow = await getDrawWorkflow(drawWorkflow.id, TOKEN);
      expect(updatedDrawWorkflow).toMatchObject({
        id: drawWorkflow.id,
        name: updatedName,
        drawCategoryId: drawWorkflow.drawCategoryId,
        __typename: DynamoDBTypename.DRAW_WORKFLOW,
        workflow: {
          id: drawWorkflow.workflow.id,
          __typename: DynamoDBTypename.WORKFLOW,
        },
      });
    });

    it("Should update a draw workflow category", async () => {
      const drawWorkflow = await createTestDrawWorkflow(TOKEN);
      const drawCategory = await createTestDrawCategory(TOKEN);

      await updateDrawWorkflow(
        {
          id: drawWorkflow.id,
          drawCategoryId: drawCategory.id,
        },
        TOKEN,
      );

      const updatedDrawWorkflow = await getDrawWorkflow(drawWorkflow.id, TOKEN);
      expect(updatedDrawWorkflow).toMatchObject({
        id: drawWorkflow.id,
        name: drawWorkflow.name,
        drawCategoryId: drawCategory.id,
        __typename: DynamoDBTypename.DRAW_WORKFLOW,
        workflow: {
          id: drawWorkflow.workflow.id,
          __typename: DynamoDBTypename.WORKFLOW,
        },
      });
    });

    // Skipping as we removed the unique constraint on names
    it.skip("Should ensure that names are unique", async () => {
      const drawWorkflow = await createTestDrawWorkflow(TOKEN);

      await expect(createTestDrawWorkflow(TOKEN, { name: drawWorkflow.name })).rejects.toThrowError(
        "Draw workflow with the same name already exists.",
      );
    });

    it("Should throw an error if a draw category does not exist.", async () => {
      const drawWorkflow = await createTestDrawWorkflow(TOKEN);

      await expect(
        updateDrawWorkflow(
          {
            id: drawWorkflow.id,
            drawCategoryId: `${DynamoDBPrefix.DRAW_CATEGORY}_${ulid()}`,
          },
          TOKEN,
        ),
      ).rejects.toThrow("Provided draw category ID does not exist.");
    });

    it("Should throw an error if a draw category does not exist.", async () => {
      const drawWorkflow = await createTestDrawWorkflow(TOKEN);

      await expect(
        updateDrawWorkflow(
          {
            id: drawWorkflow.id,
            drawCategoryId: `${DynamoDBPrefix.DRAW_CATEGORY}_${ulid()}`,
          },
          TOKEN,
        ),
      ).rejects.toThrow("Provided draw category ID does not exist.");
    });

    describe("While changing multiple fields", () => {
      // Skipping as we removed the unique constraint on names
      it.skip("Report the correct error message for name conflicts", async () => {
        const drawCategory = await createTestDrawCategory(TOKEN);
        const drawWorkflow = await createTestDrawWorkflow(TOKEN);
        const anotherDrawWorkflow = await createTestDrawWorkflow(TOKEN);

        await expect(
          updateDrawWorkflow(
            {
              id: anotherDrawWorkflow.id,
              name: drawWorkflow.name,
              drawCategoryId: drawCategory.id,
            },
            TOKEN,
          ),
        ).rejects.toThrowError("Draw workflow with the same name already exists.");
      });

      it("Report the correct error message for categoryId", async () => {
        const drawWorkflow = await createTestDrawWorkflow(TOKEN);

        await expect(
          updateDrawWorkflow(
            {
              id: drawWorkflow.id,
              name: `test workflow ${ulid()}`,
              drawCategoryId: `${DynamoDBPrefix.DRAW_CATEGORY}_${ulid()}`,
            },
            TOKEN,
          ),
        ).rejects.toThrowError("Provided draw category ID does not exist.");
      });
    });
  });

  describe("List Draw Workflows", () => {
    it("Should list draw workflows", async () => {
      const { id: drawCategoryId } = await createTestDrawCategory(TOKEN);
      const drawCategory = await getDrawCategory(drawCategoryId, TOKEN);

      const count = Math.floor(Math.random() * 10) + 1;
      const drawWorkflows = [];
      for (let i = 0; i < count; i++) {
        drawWorkflows.push(await createTestDrawWorkflow(TOKEN, { drawCategoryId }));
      }

      const workflowNames = drawWorkflows.map((workflow) => workflow.name).sort();

      await withConsistencyRetries(async () => {
        const { items } = await listDrawWorkflowsForDrawCategory({ drawCategoryId: drawCategory.id }, TOKEN);

        expect(items.length).toBe(count);

        const itemNames = items.map((item) => item.name).sort();
        expect(itemNames).toMatchObject(workflowNames);
      });
    });
  });

  describe("Delete Draw Workflow", () => {
    it("Should delete a draw workflow", async () => {
      const drawWorkflow = await createTestDrawWorkflow(TOKEN);

      await deleteDrawWorkflow({ id: drawWorkflow.id }, TOKEN);

      await expect(getDrawWorkflow(drawWorkflow.id, TOKEN)).rejects.toThrow("Draw workflow not found");
    });

    it("Should delete draw workflows by draw category id", async () => {
      const drawCategory = await createTestDrawCategory(TOKEN);
      const count = Math.floor(Math.random() * 10) + 1;
      for (let i = 0; i < count; i++) {
        await createTestDrawWorkflow(TOKEN, { drawCategoryId: drawCategory.id });
      }

      await withConsistencyRetries(async () => {
        const drawWorkflows = await listDrawWorkflowsForDrawCategory(
          { drawCategoryId: drawCategory.id, paginationRequest: { limit: count } },
          TOKEN,
        );
        expect(drawWorkflows.items.length).toBe(count);
      });

      await deleteDrawWorkflowByDrawCategoryId({ drawCategoryId: drawCategory.id }, TOKEN);

      await withConsistencyRetries(async () => {
        const drawWorkflows = await listDrawWorkflowsForDrawCategory(
          { drawCategoryId: drawCategory.id, paginationRequest: { limit: count } },
          TOKEN,
        );
        expect(drawWorkflows.items.length).toBe(0);
      });
    });
  });
});

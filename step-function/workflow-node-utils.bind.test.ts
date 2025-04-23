import { buildTestToken } from "@aspira-nextgen/core/authn-test-utils";
import { deleteAllTableItemsByPk, withConsistencyRetries } from "@aspira-nextgen/core/dynamodb-test-utils";
import { WorkflowMessageKey } from "@aspira-nextgen/graphql/resolvers";
import { ulid } from "ulidx";
import { afterEach, describe, expect, it } from "vitest";
import { dynamoDBClient, getDrawTableName } from "#dynamo/dynamo.ts";
import { getWorkflowJobLogEntities } from "#dynamo/workflow-job.ts";
import { wrapWorkflowHandler } from "#step-function/workflow-node-utils.ts";
import { LoggableWorkflowError } from "#step-function/workflow-types.ts";
import { createTestWorkflowJobResources, createTestWorkflowNode, deleteAllS3TestDataByOrg } from "#util/test-utils.ts";

const ORG_ID = `org_${ulid()}`;
const TOKEN = buildTestToken(ORG_ID);

describe("Workflow Node Utils", () => {
  afterEach(async () => {
    await deleteAllTableItemsByPk(dynamoDBClient, getDrawTableName(), [ORG_ID]);
    await deleteAllS3TestDataByOrg(ORG_ID);
  });

  describe("wrapWorkflowHandler", () => {
    it("should trap and log exceptions", async () => {
      const { workflow, workflowJob } = await createTestWorkflowJobResources(TOKEN);
      const node = await createTestWorkflowNode(TOKEN, { workflowId: workflow.id });

      const message = `Test message ${ulid()}`;
      const ƒ = wrapWorkflowHandler(async () => {
        await new Promise((resolve) => setTimeout(resolve, 5));
        throw new LoggableWorkflowError(message, WorkflowMessageKey.UnknownError);
      });

      await expect(
        ƒ(
          {
            context: { organizationId: ORG_ID, workflowJobId: workflowJob.id, nodeId: node.id },
            nodeOverrideData: {},
            nodeResultData: {},
          },
          // @ts-ignore: Suppress the type error for the next line
          {},
          () => {},
        ),
      ).rejects.toThrow("Error in workflow node handler: ");

      const logs = await withConsistencyRetries(async () => {
        const logs = await getWorkflowJobLogEntities({ organizationId: ORG_ID, workflowJobId: workflowJob.id });
        expect(logs).toHaveLength(1);
        return logs;
      });

      expect(logs).toEqual([
        expect.objectContaining({
          workflowJobId: workflowJob.id,
          workflowNodeId: node.id,
          messageKey: WorkflowMessageKey.UnknownError,
        }),
      ]);
    });
  });
});

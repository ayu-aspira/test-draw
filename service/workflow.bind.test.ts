import { dynamoDBClient } from "@/dynamo/dynamo";
import {
  WorkflowInstanceStatus,
  getLatestWorkflowInstanceEntity,
  updateWorkflowInstanceEntity,
} from "@/dynamo/workflow";
import { createWorkflowJobLogEntity, getWorkflowJobEntity } from "@/dynamo/workflow-job";
import { deleteWorkflowInstancesByWorkflowId, queueWorkflowJob } from "@/service/workflow";
import { getWorkflowJob } from "@/service/workflow";
import { createTestWorkflow, createTestWorkflowNode, deleteAllS3TestDataByOrg } from "@/util/test-utils";
import { buildTestToken } from "@aspira-nextgen/core/authn-test-utils";
import { deleteAllTableItemsByPk, withConsistencyRetries } from "@aspira-nextgen/core/dynamodb-test-utils";
import { WorkflowJobStatus, WorkflowLogLevel, WorkflowMessageKey } from "@aspira-nextgen/graphql/resolvers";
import {
  DescribeExecutionCommand,
  ExecutionStatus,
  StartExecutionCommand,
  StartSyncExecutionCommand,
} from "@aws-sdk/client-sfn";
import { Table } from "sst/node/table";
import { ulid } from "ulidx";
import { afterEach, describe, expect, it, vi } from "vitest";

const ORG_ID = `org_${ulid()}`;
const TOKEN = buildTestToken(ORG_ID);

vi.mock("@aws-sdk/client-sfn", async () => {
  const actual = await vi.importActual("@aws-sdk/client-sfn");

  return {
    ...actual,
    SFNClient: vi.fn().mockImplementation(() => ({
      send: vi.fn().mockImplementation((command) => {
        if (command instanceof StartExecutionCommand || command instanceof StartSyncExecutionCommand) {
          return {
            executionArn: "execution arn",
          };
        }

        if (command instanceof DescribeExecutionCommand) {
          return {
            status: ExecutionStatus.RUNNING,
          };
        }

        throw new Error(`Unexpected command: ${command}`);
      }),
    })),
  };
});

const updateTestWorkflowInstanceStatus = async (workflowInstanceId: string, status: WorkflowInstanceStatus) => {
  await updateWorkflowInstanceEntity({
    id: {
      organizationId: ORG_ID,
      workflowInstanceId: workflowInstanceId,
    },
    updates: {
      status,
    },
    updatedBy: TOKEN.sub,
  });
};

describe("Workflow Service Tests", () => {
  afterEach(async () => {
    await deleteAllTableItemsByPk(dynamoDBClient, Table.Draw.tableName, [ORG_ID]);
    await deleteAllS3TestDataByOrg(ORG_ID);
  });

  describe("Queue workflow job tests", () => {
    it("Should queue a workflow job", async () => {
      const { workflow, workflowInstance } = await createTestWorkflow(TOKEN);
      await updateTestWorkflowInstanceStatus(workflowInstance.id, WorkflowInstanceStatus.Ready);
      const workflowJob = await queueWorkflowJob({ workflowId: workflow.id }, TOKEN);
      const workflowJobEntity = await getWorkflowJobEntity({ organizationId: ORG_ID, workflowJobId: workflowJob.id });
      expect(workflowJobEntity).toMatchObject({
        id: workflowJob.id,
        workflowInstanceId: workflowInstance.id,
        __typename: "WorkflowJob",
      });
    });
  });

  describe("Get workflow job tests", () => {
    it("Should get a workflow job", async () => {
      const { workflow, workflowInstance } = await createTestWorkflow(TOKEN);
      const node = await createTestWorkflowNode(TOKEN, { workflowId: workflow.id });
      await updateTestWorkflowInstanceStatus(workflowInstance.id, WorkflowInstanceStatus.Ready);
      const queuedWorkflowJob = await queueWorkflowJob({ workflowId: workflow.id }, TOKEN);
      const workflowJobEntity = await getWorkflowJobEntity({
        organizationId: ORG_ID,
        workflowJobId: queuedWorkflowJob.id,
      });

      await createWorkflowJobLogEntity({
        organizationId: ORG_ID,
        workflowJobId: workflowJobEntity.id,
        workflowNodeId: node.id,
        createdBy: "system",
        level: WorkflowLogLevel.Error,
        messageKey: WorkflowMessageKey.NoStartNodeFound,
      });

      const workflowJob = await withConsistencyRetries(async () => {
        const workflowJob = await getWorkflowJob(workflowJobEntity.id, TOKEN);
        expect(workflowJob.logs).toHaveLength(1);
        return workflowJob;
      });

      expect(workflowJob).toMatchObject({
        id: queuedWorkflowJob.id,
        workflowInstanceId: workflowInstance.id,
        status: WorkflowJobStatus.WorkflowBuildStarted,
        __typename: "WorkflowJob",
      });

      expect(workflowJob.logs).toEqual(
        expect.arrayContaining([
          expect.objectContaining({
            level: WorkflowLogLevel.Error,
            messageKey: WorkflowMessageKey.NoStartNodeFound,
          }),
        ]),
      );
    });

    it("Should disallow getting a non-existent workflow job", async () => {
      await expect(getWorkflowJobEntity({ organizationId: ORG_ID, workflowJobId: ulid() })).rejects.toThrowError(
        "Workflow job not found",
      );
    });
  });

  describe("Delete Workflow Instance", () => {
    it("Should delete a workflow instance by workflow id", async () => {
      const { workflow, workflowInstance } = await createTestWorkflow(TOKEN);

      expect(workflowInstance).toBeDefined();

      await withConsistencyRetries(async () => {
        const instance = await getLatestWorkflowInstanceEntity({ organizationId: ORG_ID, workflowId: workflow.id });
        expect(instance).toBeDefined();
      });

      await deleteWorkflowInstancesByWorkflowId({ workflowId: workflow.id }, TOKEN);

      await withConsistencyRetries(async () => {
        const instance = await getLatestWorkflowInstanceEntity({ organizationId: ORG_ID, workflowId: workflow.id });
        expect(instance).toBeUndefined();
      });
    });
  });
});

import type { DrawDocumentEntity } from "@/dynamo/draw-document";
import { dynamoDBClient } from "@/dynamo/dynamo";
import { WorkflowInstanceStatus, getWorkflowInstanceEntity } from "@/dynamo/workflow";
import { type WorkflowJobEntity, createWorkflowJobEntity, getWorkflowJobEntity } from "@/dynamo/workflow-job";
import { readAslFromS3, uploadAslToS3 } from "@/s3/workflow";
import { createWorkflowDataNode } from "@/service/workflow-node";
import {
  aslMapperHandler,
  runStateMachineHandler,
  stateMachineCreatorHandler,
  stateMachinePollerHandler,
} from "@/service/workflow-orchestrator";
import { createTestWorkflowEdge, createTestWorkflowJobResources, createTestWorkflowNode } from "@/util/test-utils";
import { createTestDrawDocument } from "@/util/test-utils";
import { buildTestToken } from "@aspira-nextgen/core/authn-test-utils";
import { deleteAllTableItemsByPk, withConsistencyRetries } from "@aspira-nextgen/core/dynamodb-test-utils";
import {
  DrawDocumentProcessingStatus,
  DrawDocumentType,
  WorkflowDataNodeType,
  WorkflowDomain,
  WorkflowLogLevel,
  WorkflowMessageKey,
  type WorkflowNode,
  WorkflowNodeType,
} from "@aspira-nextgen/graphql/resolvers";
import {
  CreateStateMachineCommand,
  DescribeStateMachineCommand,
  StartExecutionCommand,
  UpdateStateMachineCommand,
  ValidateStateMachineDefinitionCommand,
  ValidateStateMachineDefinitionResultCode,
} from "@aws-sdk/client-sfn";
import type { StateMachine } from "asl-types";
import type { Context } from "aws-lambda";
import { Table } from "sst/node/table";
import { ulid } from "ulidx";
import { afterEach, describe, expect, it, vi } from "vitest";
import { getWorkflowJob } from "#service/workflow.ts";

const ORG_ID = `org_${ulid()}`;
const TOKEN = buildTestToken(ORG_ID);

vi.mock("@aws-sdk/client-sfn", async () => {
  const actual = await vi.importActual("@aws-sdk/client-sfn");

  return {
    ...actual,
    SFNClient: vi.fn().mockImplementation(() => ({
      send: vi.fn().mockImplementation((command) => {
        if (command instanceof CreateStateMachineCommand || command instanceof UpdateStateMachineCommand) {
          // Constant used for testing. Can't set this as a const
          // in module scope due to how vitest mocks work.
          if (command.input.definition === "{}") {
            throw new Error("Invalid definition");
          }

          return {
            stateMachineArn: "state machine arn",
          };
        }

        if (command instanceof DescribeStateMachineCommand) {
          let status = "ACTIVE";

          // Constant used for testing. Can't set this as a const
          // in module scope due to how vitest mocks work.
          if (command.input.stateMachineArn === "failed") {
            status = "FAILED";
          }

          return {
            status,
          };
        }

        if (command instanceof ValidateStateMachineDefinitionCommand) {
          return {
            result: ValidateStateMachineDefinitionResultCode.OK,
          };
        }

        if (command instanceof StartExecutionCommand) {
          return {
            executionArn: "execution arn",
          };
        }

        throw new Error(`Unexpected command: ${command}`);
      }),
    })),
  };
});

const lambdaRegistry: Record<string, { arn: string }> = {
  WorkflowDrawNode: { arn: "draw runner arn" },
  WorkflowNoopNode: { arn: "workflow noop arn" },
  WorkflowExportNode: { arn: "workflow export arn" },
};

type TestNode = WorkflowNode & { typename: string };
type GraphDef = Record<string, { targets?: string[]; type?: WorkflowNodeType | WorkflowDataNodeType }>;

const createWorkflowGraph = async (
  graphDef: GraphDef,
): Promise<{
  workflow: { id: string };
  workflowInstance: { id: string };
  nodes: Record<string, TestNode>;
  documents: Record<string, DrawDocumentEntity>;
}> => {
  const { workflow, workflowInstance } = await createTestWorkflowJobResources(TOKEN);
  const nodes: Record<string, TestNode> = {};
  const documents: Record<string, DrawDocumentEntity> = {};

  // Create nodes
  for (const [key, { type = WorkflowNodeType.WorkflowNoopNode }] of Object.entries(graphDef)) {
    let node: WorkflowNode;

    switch (type) {
      case WorkflowDataNodeType.WorkflowDrawDataNode:
      case WorkflowDataNodeType.WorkflowDrawApplicantDataNode:
        node = await createWorkflowDataNode({ type: type as WorkflowDataNodeType, workflowId: workflow.id }, TOKEN);

        documents[key] = await createTestDrawDocument(TOKEN, {
          filename: `${key.toLowerCase()}.csv`,
          name: `${key} ${ulid()}`,
          type:
            type === WorkflowDataNodeType.WorkflowDrawDataNode
              ? DrawDocumentType.HuntCodes
              : DrawDocumentType.Applicants,
          contentType: "text/csv",
          processingStatus: DrawDocumentProcessingStatus.UrlGenerated,
          workflowNodeId: node.id,
        });
        break;

      default:
        node = await createTestWorkflowNode(TOKEN, { workflowId: workflow.id }, type as WorkflowNodeType);
        break;
    }

    nodes[key] = { ...node, typename: type };
  }

  // Create edges
  for (const [source, { targets = [] }] of Object.entries(graphDef)) {
    for (const target of targets) {
      await createTestWorkflowEdge(TOKEN, {
        workflowId: workflow.id,
        sourceNodeId: nodes[source].id,
        targetNodeId: nodes[target].id,
      });
    }
  }

  return { workflow, workflowInstance, nodes, documents };
};

const buildTestGraph = async (graphDef: GraphDef) => {
  const { workflow, workflowInstance, nodes, documents } = await createWorkflowGraph(graphDef);
  await invokeAslMapperHandler({ workflowId: workflow.id, workflowInstanceId: workflowInstance.id });

  const updatedWorkflowInstanceEntity = await getWorkflowInstanceEntity({
    organizationId: ORG_ID,
    workflowInstanceId: workflowInstance.id,
  });

  const asl = await readAslFromS3(updatedWorkflowInstanceEntity.aslS3Key ?? "");

  return { asl, nodes, documents };
};

const createWorkflowJobForInstance = async (workflowInstanceId: string): Promise<WorkflowJobEntity> =>
  await createWorkflowJobEntity({
    organizationId: ORG_ID,
    workflowInstanceId,
    createdBy: TOKEN.sub,
  });

const invokeAslMapperHandler = async (input: {
  workflowId: string;
  workflowInstanceId: string;
  workflowJobId?: string;
}) =>
  await aslMapperHandler(
    {
      Payload: {
        context: {
          organizationId: ORG_ID,
          workflow: {
            id: input.workflowId,
            instanceId: input.workflowInstanceId,
            jobId: input.workflowJobId,
          },
        },
        lambdaRegistry,
      },
    },
    {} as Context,
    () => {},
  );

const invokeStateMachineCreatorHandler = async (input: {
  workflowId: string;
  workflowInstanceId: string;
  workflowJobId?: string;
}) =>
  await stateMachineCreatorHandler(
    {
      Payload: {
        context: {
          organizationId: ORG_ID,
          workflow: {
            id: input.workflowId,
            instanceId: input.workflowInstanceId,
            jobId: input.workflowJobId,
          },
        },
      },
    },
    {} as Context,
    () => {},
  );

const invokeStateMachinePollerHandler = async (input: {
  workflowId: string;
  workflowInstanceId: string;
  workflowJobId?: string;
}) =>
  await stateMachinePollerHandler(
    {
      Payload: {
        context: {
          organizationId: ORG_ID,
          workflow: {
            id: input.workflowId,
            instanceId: input.workflowInstanceId,
            jobId: input.workflowJobId,
          },
        },
      },
    },
    {} as Context,
    () => {},
  );

const invokeRunStateMachineHandler = async (input: {
  workflowId: string;
  workflowInstanceId: string;
  workflowJobId: string;
}) =>
  await runStateMachineHandler({
    Payload: {
      context: {
        organizationId: ORG_ID,
        workflow: {
          id: input.workflowId,
          instanceId: input.workflowInstanceId,
          jobId: input.workflowJobId,
        },
      },
    },
  });

const expectTaskState = (
  node: TestNode,
  input: {
    isStartNode?: boolean;
    isEndNode?: boolean;
    next?: string;
    nodeOverrideData?: Record<
      string,
      { mime: { domain: WorkflowDomain; domainModel: string; type: string }; uri: string }
    >;
  } = {},
) => {
  const { isStartNode, isEndNode, next, nodeOverrideData = {} } = input;

  return {
    [node.id]: {
      Type: "Task",
      Resource: lambdaRegistry[node.typename].arn,
      Comment: node.typename,
      Parameters: {
        context: {
          nodeId: node.id,
          organizationId: ORG_ID,
          ...(!isStartNode && { "previousNodeId.$": "$.context.previousNodeId" }),
          "workflowJobId.$": "$.context.workflowJobId",
        },
        nodeOverrideData: nodeOverrideData || {},
        ...(isStartNode && { nodeResultData: {} }),
        ...(!isStartNode && { "nodeResultData.$": "$.nodeResultData" }),
      },
      ...(next && { Next: next }),
      ...(isEndNode && { End: true }),
    },
  };
};

const expectNodeOverrideData = (document: DrawDocumentEntity) => ({
  uri: expect.stringMatching(new RegExp(`^s3:\\/\\/([^/]+)\\/.*\\/${document.filename}$`)),
  mime: {
    type: document.contentType,
    domain: WorkflowDomain.Draw,
    domainModel: document.type,
  },
});

const defaultGraphDef = {
  A: { targets: ["D"], type: WorkflowDataNodeType.WorkflowNullDataNode },
  B: { targets: ["F"], type: WorkflowDataNodeType.WorkflowDrawDataNode },
  C: { targets: ["F"], type: WorkflowDataNodeType.WorkflowDrawApplicantDataNode },

  D: { targets: ["E"] },
  E: { targets: ["F"] },
  F: { targets: ["G"], type: WorkflowNodeType.WorkflowDrawNode },
  G: { type: WorkflowNodeType.WorkflowExportNode },
};

// This graph is also cyclic, but it doesn't have a start node.
const noStartNodeGraphDef = {
  A: { targets: ["B"], type: WorkflowDataNodeType.WorkflowNullDataNode },
  B: { targets: ["C"], type: WorkflowDataNodeType.WorkflowNullDataNode },
  C: { targets: ["A"], type: WorkflowDataNodeType.WorkflowNullDataNode },
};

const cyclicGraphDef = {
  A: { targets: ["B"], type: WorkflowNodeType.WorkflowNoopNode },
  B: { targets: ["C"], type: WorkflowNodeType.WorkflowNoopNode },
  C: { targets: ["B"], type: WorkflowNodeType.WorkflowNoopNode },
};

describe("Workflow Creator Tests", () => {
  afterEach(async () => {
    await deleteAllTableItemsByPk(dynamoDBClient, Table.Draw.tableName, [ORG_ID]);
  });

  describe("ASL Mapper Tests", () => {
    it("Should map ASL", async () => {
      const { asl, nodes, documents } = await buildTestGraph(defaultGraphDef);

      const nodeOverrideData = {
        [nodes.F.id]: expect.arrayContaining([
          expectNodeOverrideData(documents.B),
          expectNodeOverrideData(documents.C),
        ]),
      };

      expect(asl).toEqual({
        StartAt: nodes.D.id,
        States: {
          ...expectTaskState(nodes.D, { isStartNode: true, next: nodes.E.id }),
          ...expectTaskState(nodes.E, { next: nodes.F.id }),
          ...expectTaskState(nodes.F, { nodeOverrideData, next: nodes.G.id }),
          ...expectTaskState(nodes.G, { isEndNode: true }),
        },
      });
    });

    it("Should mark the workflow instance as failed if the ASL is invalid", async () => {
      const { workflow, workflowInstance } = await createWorkflowGraph(noStartNodeGraphDef);
      await expect(
        invokeAslMapperHandler({ workflowId: workflow.id, workflowInstanceId: workflowInstance.id }),
      ).rejects.toThrow("No start node found.");

      const updatedWorkflowInstanceEntity = await getWorkflowInstanceEntity({
        organizationId: ORG_ID,
        workflowInstanceId: workflowInstance.id,
      });

      expect(updatedWorkflowInstanceEntity.status).toEqual(WorkflowInstanceStatus.BuildFailed);
    });

    it("Should mark the workflow instance as failed if a cycle is detected", async () => {
      const { workflow, workflowInstance } = await createWorkflowGraph(cyclicGraphDef);
      await expect(
        invokeAslMapperHandler({ workflowId: workflow.id, workflowInstanceId: workflowInstance.id }),
      ).rejects.toThrow(/Cycle detected at node/);

      const updatedWorkflowInstanceEntity = await getWorkflowInstanceEntity({
        organizationId: ORG_ID,
        workflowInstanceId: workflowInstance.id,
      });

      expect(updatedWorkflowInstanceEntity.status).toEqual(WorkflowInstanceStatus.BuildFailed);
    });

    it("Should mark the workflow job as preExecutionFailure if the ASL is invalid", async () => {
      const { workflow, workflowInstance } = await createWorkflowGraph(noStartNodeGraphDef);
      const { id: workflowJobId } = await createWorkflowJobForInstance(workflowInstance.id);

      await expect(
        invokeAslMapperHandler({
          workflowId: workflow.id,
          workflowInstanceId: workflowInstance.id,
          workflowJobId,
        }),
      ).rejects.toThrow("No start node found.");

      const updatedWorkflowJob = await withConsistencyRetries(async () => {
        const updatedWorkflowJob = await getWorkflowJob(workflowJobId, TOKEN);
        expect(updatedWorkflowJob).toBeDefined();
        expect(updatedWorkflowJob.logs).toHaveLength(1);
        return updatedWorkflowJob;
      });

      expect(updatedWorkflowJob.preExecutionFailure).toEqual(true);

      expect(updatedWorkflowJob.logs).toEqual(
        expect.arrayContaining([
          expect.objectContaining({
            messageKey: WorkflowMessageKey.NoStartNodeFound,
            level: WorkflowLogLevel.Error,
          }),
        ]),
      );
    });
  });

  describe("State Machine Creator Tests", () => {
    it("Should create a state-machine", async () => {
      const { workflow, workflowInstance } = await createWorkflowGraph(defaultGraphDef);

      await invokeAslMapperHandler({ workflowId: workflow.id, workflowInstanceId: workflowInstance.id });
      await invokeStateMachineCreatorHandler({ workflowId: workflow.id, workflowInstanceId: workflowInstance.id });

      const updatedWorkflowInstanceEntity = await getWorkflowInstanceEntity({
        organizationId: ORG_ID,
        workflowInstanceId: workflowInstance.id,
      });

      expect(updatedWorkflowInstanceEntity.stateMachineArn).toEqual("state machine arn");
    });

    it("Should mark the workflow instance as failed if the state-machine creation fails", async () => {
      const { workflow, workflowInstance } = await createWorkflowGraph(defaultGraphDef);

      await invokeAslMapperHandler({ workflowId: workflow.id, workflowInstanceId: workflowInstance.id });

      await uploadAslToS3({
        organizationId: ORG_ID,
        workflowId: workflow.id,
        workflowInstanceId: workflowInstance.id,
        // See comment above in the mock for why this is necessary.
        asl: {} as StateMachine,
      });

      // The following error message is coming from the mock.
      await expect(
        invokeStateMachineCreatorHandler({ workflowId: workflow.id, workflowInstanceId: workflowInstance.id }),
      ).rejects.toThrow("Invalid definition");

      const updatedWorkflowInstanceEntity = await getWorkflowInstanceEntity({
        organizationId: ORG_ID,
        workflowInstanceId: workflowInstance.id,
      });

      expect(updatedWorkflowInstanceEntity.status).toEqual(WorkflowInstanceStatus.BuildFailed);
    });

    it("Should mark the workflow job as preExecutionFailure if the state-machine creation fails", async () => {
      const { workflow, workflowInstance } = await createWorkflowGraph(defaultGraphDef);
      const workflowJob = await createWorkflowJobForInstance(workflowInstance.id);

      await invokeAslMapperHandler({
        workflowId: workflow.id,
        workflowInstanceId: workflowInstance.id,
        workflowJobId: workflowJob.id,
      });

      await uploadAslToS3({
        organizationId: ORG_ID,
        workflowId: workflow.id,
        workflowInstanceId: workflowInstance.id,
        // See comment above in the mock for why this is necessary.
        asl: {} as StateMachine,
      });

      // The following error message is coming from the mock.
      await expect(
        invokeStateMachineCreatorHandler({
          workflowId: workflow.id,
          workflowInstanceId: workflowInstance.id,
          workflowJobId: workflowJob.id,
        }),
      ).rejects.toThrow("Invalid definition");

      const workflowJobEntity = await getWorkflowJobEntity({
        workflowJobId: workflowJob.id,
        organizationId: ORG_ID,
      });

      expect(workflowJobEntity.preExecutionFailure).toEqual(true);
    });
  });

  describe("State Machine Poller Tests", () => {
    it("Should poll the state-machine", async () => {
      const { workflow, workflowInstance } = await createWorkflowGraph(defaultGraphDef);

      await invokeAslMapperHandler({ workflowId: workflow.id, workflowInstanceId: workflowInstance.id });
      await invokeStateMachineCreatorHandler({ workflowId: workflow.id, workflowInstanceId: workflowInstance.id });
      await invokeStateMachinePollerHandler({ workflowId: workflow.id, workflowInstanceId: workflowInstance.id });

      const updatedWorkflowInstanceEntity = await getWorkflowInstanceEntity({
        organizationId: ORG_ID,
        workflowInstanceId: workflowInstance.id,
      });

      expect(updatedWorkflowInstanceEntity.status).toEqual(WorkflowInstanceStatus.Ready);
    });
  });

  describe("Run State Machine Tests", () => {
    it("Should run the state-machine if job id in scope", async () => {
      const { workflow, workflowInstance } = await createWorkflowGraph(defaultGraphDef);

      await invokeAslMapperHandler({ workflowId: workflow.id, workflowInstanceId: workflowInstance.id });
      await invokeStateMachineCreatorHandler({ workflowId: workflow.id, workflowInstanceId: workflowInstance.id });
      await invokeStateMachinePollerHandler({ workflowId: workflow.id, workflowInstanceId: workflowInstance.id });

      const workflowJob = await createWorkflowJobForInstance(workflowInstance.id);

      await invokeRunStateMachineHandler({
        workflowId: workflow.id,
        workflowInstanceId: workflowInstance.id,
        workflowJobId: workflowJob.id,
      });

      const updatedWorkflowJobEntity = await getWorkflowJobEntity({
        workflowJobId: workflowJob.id,
        organizationId: ORG_ID,
      });

      expect(updatedWorkflowJobEntity.executionArn).toEqual("execution arn");
      expect(updatedWorkflowJobEntity.preExecutionFailure).toBeUndefined();
    });
  });
});

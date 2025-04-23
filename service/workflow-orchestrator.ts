import type { DrawDocumentEntity } from "@/dynamo/draw-document";
import { listDrawDocumentEntitiesByWorkflowNodeIds } from "@/dynamo/draw-document";
import { WorkflowInstanceStatus, getWorkflowInstanceEntity, updateWorkflowInstanceEntity } from "@/dynamo/workflow";
import { type WorkflowEdgeEntity, listAllWorkflowEdgeEntitiesByWorkflowInstanceId } from "@/dynamo/workflow-edge";
import { createWorkflowJobLogEntity, updateWorkflowJobEntity } from "@/dynamo/workflow-job";
import { type WorkflowNodeEntity, listAllWorkflowNodeEntitiesByWorkflowInstanceId } from "@/dynamo/workflow-node";
import { getDrawDocumentBucketName } from "@/s3/s3";
import { readAslFromS3, uploadAslToS3 } from "@/s3/workflow";
import { sfnClient } from "@/step-function/step-function";
import {
  LoggableWorkflowError,
  type WorkflowExecutionInput,
  WorkflowMimeDataType,
  type WorkflowNodeContext,
  type WorkflowNodeDataMap,
} from "@/step-function/workflow-types";
import { buildS3UriString } from "@/util/uri";
import {
  WorkflowDataNodeType,
  WorkflowDomain,
  WorkflowLogLevel,
  WorkflowMessageKey,
  WorkflowNodeType,
} from "@aspira-nextgen/graphql/resolvers";
import {
  CreateStateMachineCommand,
  DescribeStateMachineCommand,
  StartExecutionCommand,
  StateMachineStatus,
  UpdateStateMachineCommand,
  ValidateStateMachineDefinitionCommand,
  ValidateStateMachineDefinitionResultCode,
} from "@aws-sdk/client-sfn";
import type { StateMachine, Task } from "asl-types";
import type { Context, Handler } from "aws-lambda";

type StepFunctionPayload<T> = {
  Payload: T;
};

type CommonWorkflowCreatorContext = {
  context: {
    organizationId: string;
    workflow: {
      id: string;
      instanceId: string;
      jobId?: string;
    };
    error?: LoggableWorkflowError;
  };
};

/**
 * This is the input to the overall state machine. It should be used by any invoker of the state machine
 * to ensure the correct context is passed.
 */
export type WorkflowStateMachineOrchestratorInput = StepFunctionPayload<CommonWorkflowCreatorContext>;

type AslMapperHandlerInput = StepFunctionPayload<
  {
    lambdaRegistry: Record<string, { arn: string }>;
  } & CommonWorkflowCreatorContext
>;

const DRAW_POLLING_INTERVAL = 1000;

const wrapWithFailureHandler =
  <T extends StepFunctionPayload<CommonWorkflowCreatorContext>>(handler: Handler<T>) =>
  async (event: T, context: Context) => {
    const {
      Payload: { context: ourContext },
    } = event;

    try {
      return await handler(event, context, () => {});
    } catch (err) {
      await updateWorkflowInstanceEntity({
        id: {
          organizationId: ourContext.organizationId,
          workflowInstanceId: ourContext.workflow.instanceId,
        },
        updates: {
          status: WorkflowInstanceStatus.BuildFailed,
        },
        updatedBy: "system",
      });

      if (err instanceof LoggableWorkflowError) {
        ourContext.error = err;
      }

      await setPreExecutionFailureOnJob(event);

      throw err;
    }
  };

type AslParameters = {
  context: {
    organizationId: string;
    "workflowJobId.$": "$.context.workflowJobId";
    "previousNodeId.$"?: "$.context.previousNodeId";
    nodeId: string;
  };
  nodeOverrideData: WorkflowNodeDataMap;

  // For the start-node, we will default this to an empty object.
  nodeResultData?: WorkflowNodeDataMap;

  // For non-start nodes, we will default this to the nodeResultData from the previous node.
  "nodeResultData.$"?: "$.nodeResultData";
};

const nodeTypes = new Set(Object.values(WorkflowNodeType).map(String));
const dataNodeTypes = new Set(Object.values(WorkflowDataNodeType).map(String));

type WorkflowGraphNode = {
  type: string;
  id: string;
  overrides: string[];
  sources: string[];
  targets: string[];
  document?: DrawDocumentEntity;
};
type WorkflowGraph = Record<string, WorkflowGraphNode>;

const isFunctionalNode = (node: WorkflowGraphNode) => nodeTypes.has(node.type);
const isDataNode = (node: WorkflowGraphNode) => dataNodeTypes.has(node.type);
const isStartNode = (node: WorkflowGraphNode) => node.sources.length === 0;
const isEndNode = (node: WorkflowGraphNode) => node.targets.length === 0;

const buildGraph = (input: {
  workflowNodes: WorkflowNodeEntity[];
  workflowEdges: WorkflowEdgeEntity[];
  workflowDocuments: (DrawDocumentEntity & { workflowNodeId?: string })[];
}): WorkflowGraph => {
  const { workflowNodes, workflowEdges, workflowDocuments } = input;

  const graph: WorkflowGraph = {};

  for (const entity of workflowNodes) {
    const node = {
      ...entity,
      type: entity.__typename,
      overrides: [],
      sources: [],
      targets: [],
    };

    if (!isFunctionalNode(node) && !isDataNode(node)) {
      throw new LoggableWorkflowError(
        `Node ${node.id} has invalid type ${node.__typename}`,
        WorkflowMessageKey.InvalidNodeType,
        [
          {
            key: "workflowNodeId",
            value: node.id,
          },
          {
            key: "workflowNodeType",
            value: node.__typename,
          },
        ],
      );
    }

    graph[node.id] = node;
  }

  for (const edge of workflowEdges) {
    const sourceNode = graph[edge.sourceNodeId];

    if (!sourceNode) {
      throw new LoggableWorkflowError(
        `Edge ${edge.id} has invalid source ${edge.sourceNodeId}.`,
        WorkflowMessageKey.EdgeHasInvalidSource,
        [
          {
            key: "workflowEdgeId",
            value: edge.id,
          },
          {
            key: "workflowNodeId",
            value: edge.sourceNodeId,
          },
        ],
      );
    }

    const targetNode = graph[edge.targetNodeId];
    if (!targetNode) {
      throw new LoggableWorkflowError(
        `Edge ${edge.id} has invalid target ${edge.targetNodeId}.`,
        WorkflowMessageKey.EdgeHasInvalidTarget,
        [
          {
            key: "workflowEdgeId",
            value: edge.id,
          },
          {
            key: "workflowNodeId",
            value: edge.targetNodeId,
          },
        ],
      );
    }

    if (isFunctionalNode(sourceNode)) {
      graph[edge.targetNodeId].sources.push(edge.sourceNodeId);
      graph[edge.sourceNodeId].targets.push(edge.targetNodeId);
    } else {
      graph[edge.targetNodeId].overrides.push(edge.sourceNodeId);
    }
  }

  for (const document of workflowDocuments) {
    const { workflowNodeId } = document;

    if (workflowNodeId) {
      graph[workflowNodeId].document = document;
    }
  }

  return graph;
};

const buildOverride = (dataNode: WorkflowGraphNode) => {
  // TODO: hard code check for now
  if (dataNode.type === WorkflowDataNodeType.WorkflowNullDataNode) {
    return undefined;
  }

  const { document } = dataNode;
  if (!document) {
    throw new LoggableWorkflowError(
      `Data node ${dataNode.id} has no document.`,
      WorkflowMessageKey.DataNodeHasNoDocument,
      [
        {
          key: "workflowNodeId",
          value: dataNode.id,
        },
      ],
    );
  }
  const domain = WorkflowDomain.Draw;
  const domainModel = document.type;
  const type = Object.values(WorkflowMimeDataType).find((type) => type === document.contentType);

  if (!type) {
    throw new LoggableWorkflowError(`Invalid mime type ${document.contentType}`, WorkflowMessageKey.InvalidMimeType, [
      {
        key: "workflowNodeId",
        value: dataNode.id,
      },
      {
        key: "mimeType",
        value: document.contentType,
      },
    ]);
  }

  return { mime: { domain, domainModel, type }, uri: buildS3UriString(getDrawDocumentBucketName(), document.s3Key) };
};

const buildTaskState = (input: {
  lambdaRegistry: Record<string, { arn: string }>;
  node: WorkflowGraphNode;
  graph: WorkflowGraph;
  organizationId: string;
}): Task => {
  const { node, graph, lambdaRegistry, organizationId } = input;

  const nodeTypename = node.type;
  const resource = lambdaRegistry[nodeTypename]?.arn;

  if (!resource) {
    throw new LoggableWorkflowError(
      `No resource found for node type ${nodeTypename}`,
      WorkflowMessageKey.NoResourceFound,
      [
        {
          key: "workflowNodeId",
          value: node.id,
        },
        {
          key: "workflowNodeType",
          value: nodeTypename,
        },
      ],
    );
  }

  const nodeId = node.id;

  const overrides = node.overrides
    .map((sourceId) => graph[sourceId])
    .map(buildOverride)
    .filter((override) => override !== undefined);

  const parameters: AslParameters = {
    context: {
      organizationId,
      "workflowJobId.$": "$.context.workflowJobId",
      nodeId: nodeId,
    },
    nodeOverrideData: overrides.length > 0 ? { [node.id]: overrides } : {},
  };

  if (!isStartNode(node)) {
    parameters.context["previousNodeId.$"] = "$.context.previousNodeId";
    parameters["nodeResultData.$"] = "$.nodeResultData";
  } else {
    parameters.nodeResultData = {};
  }

  const nextState = node.targets[0];

  return {
    Type: "Task",
    Resource: resource,
    Comment: nodeTypename,
    Parameters: parameters,
    ...(nextState ? { Next: nextState } : { End: true as true }),
  };
};

const generateAsl = (input: {
  graph: WorkflowGraph;
  lambdaRegistry: Record<string, { arn: string }>;
  organizationId: string;
}): StateMachine => {
  const { graph, lambdaRegistry, organizationId } = input;
  const states: Record<string, Task> = {};

  const functionalNodes = Object.values(graph).filter(isFunctionalNode);
  const startNodes = functionalNodes.filter(isStartNode);
  if (startNodes.length > 1) {
    throw new LoggableWorkflowError(
      `Multiple start nodes found. ${startNodes.map((node) => node.id)}`,
      WorkflowMessageKey.MultipleStartNodesFound,
      [
        {
          key: "workflowNodeIds",
          value: startNodes.map((node) => node.id).join(","),
        },
      ],
    );
  }
  const startNode = startNodes[0];
  if (!startNode) {
    throw new LoggableWorkflowError("No start node found.", WorkflowMessageKey.NoStartNodeFound);
  }

  const endNodes = functionalNodes.filter(isEndNode);
  if (endNodes.length > 1) {
    throw new LoggableWorkflowError(
      `Multiple end nodes found. ${endNodes.map((node) => node.id)}`,
      WorkflowMessageKey.MultipleEndNodesFound,
      [
        {
          key: "workflowNodeIds",
          value: endNodes.map((node) => node.id).join(","),
        },
      ],
    );
  }

  const visited = new Set<string>();
  const visit = (node: WorkflowGraphNode) => {
    if (node.targets.length > 1) {
      throw new LoggableWorkflowError(
        `Parallel nodes not supported. Node ${node.id}`,
        WorkflowMessageKey.ParallelNodesNotSupported,
        [
          {
            key: "workflowNodeId",
            value: node.id,
          },
        ],
      );
    }

    states[node.id] = buildTaskState({ lambdaRegistry, node, graph, organizationId });
    visited.add(node.id);

    for (const id of node.targets) {
      if (visited.has(id)) {
        throw new LoggableWorkflowError(`Cycle detected at node ${node.id}`, WorkflowMessageKey.CycleDetected, [
          {
            key: "workflowNodeId",
            value: node.id,
          },
        ]);
      }
      const target = graph[id];
      visit(target);
    }

    visited.delete(node.id);
  };
  visit(startNode);

  return {
    StartAt: startNode.id,
    States: states,
  };
};

/**
 * Handles mapping the workflow definition to ASL.
 */
const aslMapper = async (event: AslMapperHandlerInput) => {
  const { context, lambdaRegistry } = event.Payload;

  const {
    organizationId,
    workflow: { id: workflowId, instanceId: workflowInstanceId },
  } = context;

  await updateWorkflowInstanceEntity({
    id: {
      organizationId,
      workflowInstanceId,
    },
    updates: {
      status: WorkflowInstanceStatus.BuildStarted,
    },
    updatedBy: "system",
  });

  const workflowNodes = await listAllWorkflowNodeEntitiesByWorkflowInstanceId({
    organizationId,
    workflowInstanceId,
  });

  const workflowEdges = await listAllWorkflowEdgeEntitiesByWorkflowInstanceId({
    organizationId,
    workflowInstanceId,
  });

  const workflowNodeIds = workflowNodes.map((node) => node.id);
  const workflowDocuments = await listDrawDocumentEntitiesByWorkflowNodeIds({
    organizationId,
    workflowNodeIds,
  });

  const graph = buildGraph({
    workflowNodes,
    workflowEdges,
    workflowDocuments,
  });

  const asl = generateAsl({ graph, lambdaRegistry, organizationId });

  // Ensure that the ASL is valid before saving it to the workflow instance.
  const validateResult = await sfnClient.send(
    new ValidateStateMachineDefinitionCommand({ definition: JSON.stringify(asl) }),
  );

  if (validateResult.result !== ValidateStateMachineDefinitionResultCode.OK) {
    throw new Error("ASL is invalid.");
  }

  // Upload the ASL to S3.
  const aslS3Key = await uploadAslToS3({
    workflowId,
    organizationId,
    workflowInstanceId,
    asl,
  });

  await updateWorkflowInstanceEntity({
    id: {
      organizationId,
      workflowInstanceId,
    },
    updates: {
      aslS3Key,
    },
    updatedBy: "system",
  });

  return {
    context,
  };
};

export const aslMapperHandler: Handler<AslMapperHandlerInput, CommonWorkflowCreatorContext> =
  wrapWithFailureHandler<AslMapperHandlerInput>(aslMapper);

/**
 * Takes the ASL from the workflow instance and attempts to create a state machine.
 * Sets the state machine ARN on the workflow instance for future reference.
 */
const stateMachineCreator = async (event: StepFunctionPayload<CommonWorkflowCreatorContext>) => {
  const { context } = event.Payload;

  const workflowInstanceEntity = await getWorkflowInstanceEntity({
    organizationId: context.organizationId,
    workflowInstanceId: context.workflow.instanceId,
  });

  if (!workflowInstanceEntity.aslS3Key) {
    throw new Error("No S3 key found");
  }

  const asl = await readAslFromS3(workflowInstanceEntity.aslS3Key);
  const stringifiedAsl = JSON.stringify(asl);

  if (!workflowInstanceEntity.stateMachineArn) {
    const sf = await sfnClient.send(
      new CreateStateMachineCommand({
        name: workflowInstanceEntity.id,
        definition: stringifiedAsl,
        roleArn: process.env.WORKFLOW_ASSUME_ROLE_ARN,
      }),
    );

    if (!sf?.stateMachineArn) {
      throw new Error("Failed to create state machine");
    }

    const { stateMachineArn } = sf;

    await updateWorkflowInstanceEntity({
      id: {
        organizationId: context.organizationId,
        workflowInstanceId: context.workflow.instanceId,
      },
      updates: {
        stateMachineArn,
      },
      updatedBy: "system",
    });
  } else {
    await sfnClient.send(
      new UpdateStateMachineCommand({
        stateMachineArn: workflowInstanceEntity.stateMachineArn,
        definition: stringifiedAsl,
      }),
    );
  }

  return {
    context,
  };
};

export const stateMachineCreatorHandler: Handler<
  StepFunctionPayload<CommonWorkflowCreatorContext>,
  CommonWorkflowCreatorContext
> = wrapWithFailureHandler(stateMachineCreator);

/**
 * Polls the state machine until it is active. Once active, sets the workflow instance to ready.
 * After this step, the workflow instance is ready to be executed.
 */
const stateMachinePoller = async (event: StepFunctionPayload<CommonWorkflowCreatorContext>) => {
  const { context } = event.Payload;

  const { stateMachineArn } = await getWorkflowInstanceEntity({
    organizationId: context.organizationId,
    workflowInstanceId: context.workflow.instanceId,
  });

  let describe = null;

  do {
    await new Promise((r) => setTimeout(r, DRAW_POLLING_INTERVAL));
    describe = await sfnClient.send(new DescribeStateMachineCommand({ stateMachineArn }));
  } while (describe?.status !== StateMachineStatus.ACTIVE);

  await updateWorkflowInstanceEntity({
    id: {
      organizationId: context.organizationId,
      workflowInstanceId: context.workflow.instanceId,
    },
    updates: {
      status: WorkflowInstanceStatus.Ready,
    },
    updatedBy: "system",
  });

  return {
    context,
  };
};

export const stateMachinePollerHandler: Handler<
  StepFunctionPayload<CommonWorkflowCreatorContext>,
  StepFunctionPayload<CommonWorkflowCreatorContext>
> = wrapWithFailureHandler(stateMachinePoller);

export const runStateMachineHandler = async (input: StepFunctionPayload<CommonWorkflowCreatorContext>) => {
  const { context: ourContext } = input.Payload;

  if (!ourContext.workflow.jobId) {
    return;
  }
  try {
    const workflowInstance = await getWorkflowInstanceEntity({
      organizationId: ourContext.organizationId,
      workflowInstanceId: ourContext.workflow.instanceId,
    });

    if (workflowInstance.status !== WorkflowInstanceStatus.Ready) {
      throw new Error("Workflow instance is not ready.");
    }

    const executionInput: WorkflowExecutionInput = {
      context: {
        workflowJobId: ourContext.workflow.jobId,
        organizationId: ourContext.organizationId,
      } as WorkflowNodeContext,
    };

    const { executionArn } = await sfnClient.send(
      new StartExecutionCommand({
        stateMachineArn: workflowInstance.stateMachineArn,
        input: JSON.stringify(executionInput),
      }),
    );

    if (!executionArn) {
      throw new Error("No executionArn was returned when starting the state machine.");
    }

    await updateWorkflowJobEntity({
      id: {
        organizationId: ourContext.organizationId,
        workflowJobId: ourContext.workflow.jobId,
      },
      updates: {
        executionArn,
      },
      updatedBy: "system",
    });
  } catch (err) {
    await setPreExecutionFailureOnJob(input);

    throw new Error("Failed to start state-machine", { cause: err });
  }
};

const setPreExecutionFailureOnJob = async (input: StepFunctionPayload<CommonWorkflowCreatorContext>): Promise<void> => {
  const { context: ourContext } = input.Payload;

  if (!ourContext.workflow.jobId) {
    return;
  }

  if (ourContext.error) {
    await createWorkflowJobLogEntity({
      organizationId: ourContext.organizationId,
      workflowJobId: ourContext.workflow.jobId,
      createdBy: "system",
      level: WorkflowLogLevel.Error,
      messageKey: ourContext.error.messageKey,
      messageParams: ourContext.error.messageParams,
    });
  }

  await updateWorkflowJobEntity({
    id: {
      organizationId: ourContext.organizationId,
      workflowJobId: ourContext.workflow.jobId,
    },
    updates: {
      preExecutionFailure: true,
    },
    updatedBy: "system",
  });
};

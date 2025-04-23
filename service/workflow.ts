import { DynamoDBTypename } from "@/dynamo/dynamo";
import {
  deleteWorkflowInstanceEntities,
  getLatestWorkflowInstanceEntity,
  listAllWorkflowInstanceEntitiesByWorkflowId,
} from "@/dynamo/workflow";
import {
  type WorkflowJobEntity,
  createWorkflowJobEntity,
  getWorkflowJobEntity,
  getWorkflowJobLogEntities,
} from "@/dynamo/workflow-job";
import type { WorkflowStateMachineOrchestratorInput } from "@/service/workflow-orchestrator";
import { getWorkflowExecutionStatus, sfnClient } from "@/step-function/step-function";
import type { Token } from "@aspira-nextgen/core/authn";
import { type QueueWorkflowJobInput, type WorkflowJob, WorkflowJobStatus } from "@aspira-nextgen/graphql/resolvers";
import { StartExecutionCommand } from "@aws-sdk/client-sfn";

export const queueWorkflowJob = async (input: QueueWorkflowJobInput, token: Token): Promise<WorkflowJob> => {
  const { workflowId } = input;

  const {
    claims: { organizationId },
  } = token;

  const workflowInstance = await getLatestWorkflowInstanceEntity({ workflowId, organizationId });

  if (!workflowInstance) {
    throw new Error(`Failed to find a workflow instance for workflow ${workflowId}.`);
  }

  const job = await createWorkflowJobEntity({
    workflowInstanceId: workflowInstance.id,
    organizationId,
    createdBy: token.sub,
  });

  await runWorkflowCreatorStateMachine({
    organizationId,
    workflowId,
    workflowInstanceId: workflowInstance.id,
    workflowJobId: job.id,
  });

  return {
    ...job,
    status: await getWorkflowJobStatus(job),
    __typename: DynamoDBTypename.WORKFLOW_JOB,
  };
};

export const getWorkflowJob = async (workflowJobId: string, token: Token): Promise<WorkflowJob> => {
  const workflowJobEntity = await getWorkflowJobEntity({ workflowJobId, organizationId: token.claims.organizationId });
  const status = await getWorkflowJobStatus(workflowJobEntity);
  const logs = await getWorkflowJobLogEntities({
    workflowJobId,
    organizationId: token.claims.organizationId,
  });

  return {
    ...workflowJobEntity,
    status,
    logs,
    __typename: DynamoDBTypename.WORKFLOW_JOB,
  };
};

const getWorkflowJobStatus = async (workflowJob: WorkflowJobEntity): Promise<WorkflowJobStatus> => {
  if (workflowJob.executionArn) {
    return (await getWorkflowExecutionStatus(workflowJob.executionArn)) as WorkflowJobStatus;
  }

  return workflowJob.preExecutionFailure ? WorkflowJobStatus.Failed : WorkflowJobStatus.WorkflowBuildStarted;
};

const runWorkflowCreatorStateMachine = async (input: {
  organizationId: string;
  workflowId: string;
  workflowInstanceId: string;
  workflowJobId?: string;
}): Promise<void> => {
  const stateMachineInput: WorkflowStateMachineOrchestratorInput = {
    Payload: {
      context: {
        organizationId: input.organizationId,
        workflow: {
          id: input.workflowId,
          instanceId: input.workflowInstanceId,
          jobId: input.workflowJobId,
        },
      },
    },
  };

  try {
    await sfnClient.send(
      new StartExecutionCommand({
        stateMachineArn: process.env.WORKFLOW_ORCHESTRATOR_STATE_MACHINE_ARN,
        input: JSON.stringify(stateMachineInput),
      }),
    );
  } catch (err) {
    throw new Error("Failed to run workflow creator state machine", { cause: err });
  }
};

export const deleteWorkflowInstancesByWorkflowId = async (
  input: { workflowId: string },
  token: Token,
): Promise<void> => {
  const { workflowId } = input;
  const { organizationId } = token.claims;

  const workflowInstances = await listAllWorkflowInstanceEntitiesByWorkflowId({ workflowId, organizationId });
  await deleteWorkflowInstanceEntities(workflowInstances);
};

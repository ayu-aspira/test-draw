import { getWorkflowNodeResultEntity, listWorkflowNodeResultEntities } from "@/dynamo/workflow-node-result";
import type { Token } from "@aspira-nextgen/core/authn";
import type { WorkflowDomain, WorkflowNodeResult, WorkflowNodeResultDownload } from "@aspira-nextgen/graphql/resolvers";
import { getLatestWorkflowJobEntity } from "#dynamo/workflow-job.ts";
import { getLatestWorkflowInstanceEntity } from "#dynamo/workflow.ts";
import { generatePresignedDownloadUrl } from "#s3/s3.ts";
import { getS3KeyFromURI } from "#util/uri.ts";

const getLatestWorkflowJobId = async (input: { workflowId: string; organizationId: string }): Promise<
  string | null
> => {
  const { workflowId, organizationId } = input;
  const { id: workflowInstanceId } = (await getLatestWorkflowInstanceEntity({ workflowId, organizationId })) || {};
  if (!workflowInstanceId) {
    return null;
  }

  const { id: workflowJobId } = (await getLatestWorkflowJobEntity({ organizationId, workflowInstanceId })) || {};
  if (!workflowJobId) {
    return null;
  }

  return workflowJobId;
};

export const listWorkflowNodeResults = async (
  input: { workflowId: string; workflowJobId?: string | null },
  token: Token,
): Promise<Record<string, WorkflowNodeResult[]>> => {
  const { workflowId } = input;
  const { organizationId } = token.claims;

  const workflowJobId = input.workflowJobId || (await getLatestWorkflowJobId({ workflowId, organizationId }));
  if (!workflowJobId) {
    return {};
  }

  const entities = await listWorkflowNodeResultEntities({
    organizationId,
    workflowJobId,
  });

  return entities.reduce(
    (acc, { id, nodeId, results }) => {
      if (!acc[nodeId]) {
        acc[nodeId] = [];
      }
      acc[nodeId].push(
        ...results.map(({ mime }) => ({
          workflowNodeResultId: id,
          workflowNodeId: nodeId,
          ...mime,
        })),
      );
      return acc;
    },
    {} as Record<string, WorkflowNodeResult[]>,
  );
};

export const prepareWorkflowNodeResultDownload = async (
  input: { workflowNodeResultId: string; domain: WorkflowDomain; domainModel: string },
  token: Token,
): Promise<WorkflowNodeResultDownload> => {
  const { workflowNodeResultId, domain, domainModel } = input;
  const { organizationId } = token.claims;

  const entity = await getWorkflowNodeResultEntity({ organizationId, workflowNodeResultId });
  if (!entity) {
    throw new Error(`Workflow node result with id ${workflowNodeResultId} not found.`);
  }

  const result = entity.results.find(({ mime }) => mime.domain === domain && mime.domainModel === domainModel);
  if (!result) {
    throw new Error(
      `Workflow node result with id ${workflowNodeResultId} does not contain a result for ${domain}.${domainModel}.`,
    );
  }

  const s3key = getS3KeyFromURI(new URL(result.uri));
  const url = await generatePresignedDownloadUrl(s3key);

  return {
    workflowNodeResultId,
    workflowJobId: entity.workflowJobId,
    workflowNodeId: entity.nodeId,
    domain,
    domainModel,
    type: result.mime.type,
    url,
  };
};

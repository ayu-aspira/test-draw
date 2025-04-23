import { afterEach } from "node:test";
import { dynamoDBClient } from "@/dynamo/dynamo";
import { listWorkflowNodeResults, prepareWorkflowNodeResultDownload } from "@/service/workflow-node-result";
import { buildTestToken } from "@aspira-nextgen/core/authn-test-utils";
import { deleteAllTableItemsByPk, withConsistencyRetries } from "@aspira-nextgen/core/dynamodb-test-utils";
import { WorkflowDomain } from "@aspira-nextgen/graphql/resolvers";
import { ulid } from "ulidx";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { getDrawTableName } from "#dynamo/dynamo.ts";
import { getDrawDocumentBucketName } from "#s3/s3.ts";
import { createTestWorkflowWithResults, deleteAllS3TestDataByOrg, mockSFNClientForTesting } from "#util/test-utils.ts";

const ORG_ID = `org_${ulid()}`;
const TOKEN = buildTestToken(ORG_ID);

describe("Workflow Node Result", () => {
  const tableName = getDrawTableName();

  beforeEach(() => {
    mockSFNClientForTesting();
  });

  afterEach(async () => {
    vi.clearAllMocks();

    await deleteAllTableItemsByPk(dynamoDBClient, tableName, [ORG_ID]);
    await deleteAllS3TestDataByOrg(ORG_ID);
  });

  describe("listWorkflowNodeResults", () => {
    it("should return results", async () => {
      const { workflow, workflowJob, node } = await createTestWorkflowWithResults(TOKEN);

      const nodeResults = await listWorkflowNodeResults(
        { workflowId: workflow.id, workflowJobId: workflowJob.id },
        TOKEN,
      );

      expect(nodeResults).toEqual({
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

      expect(nodeResults[node.id]).toHaveLength(2);
    });
  });

  describe("prepareWorkflowNodeResultDownload", () => {
    it("should return a download object", async () => {
      const { workflow, workflowJob, node } = await createTestWorkflowWithResults(TOKEN);

      // const nodeResult = nodeResults[node.id][0];
      const nodeResult = await withConsistencyRetries(async () => {
        const nodeResults = await listWorkflowNodeResults(
          { workflowId: workflow.id, workflowJobId: workflowJob.id },
          TOKEN,
        );

        if (!nodeResults[node.id]) {
          throw new Error("Node results not found");
        }

        return nodeResults[node.id][0];
      });

      const download = await prepareWorkflowNodeResultDownload(
        {
          workflowNodeResultId: nodeResult.workflowNodeResultId,
          domain: WorkflowDomain.Draw,
          domainModel: "APPLICANTS",
        },
        TOKEN,
      );

      expect(download).toEqual({
        workflowJobId: workflowJob.id,
        workflowNodeId: node.id,
        workflowNodeResultId: nodeResult.workflowNodeResultId,
        domain: nodeResult.domain,
        domainModel: nodeResult.domainModel,
        type: nodeResult.type,
        url: expect.any(String),
      });

      expect(download.url).toEqual(expect.stringMatching(new RegExp(`^https://${getDrawDocumentBucketName()}`)));
      expect(download.url).toEqual(expect.stringMatching(/x-id=GetObject/));
    });
  });
});

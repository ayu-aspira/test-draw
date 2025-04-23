import { getDocumentEntity } from "@/dynamo/draw-document";
import { dynamoDBClient } from "@/dynamo/dynamo";
import {
  DRAW_APPLICANT_DOMAIN_MODEL,
  DRAW_DRAW_DATA_DOMAIN_MODEL,
  DRAW_METRICS_DOMAIN_MODEL,
} from "@/service/draw-constants";
import { workflowNodeHandler as workflowDrawNodeHandler } from "@/service/workflow-draw-node";
import { findWorkflowDataSource } from "@/step-function/workflow-node-utils";
import { WorkflowMimeDataType } from "@/step-function/workflow-types";
import {
  buildTestDrawFixturePath,
  buildTestWorkflowDrawNodePayload,
  deleteAllS3TestDataByOrg,
  getTestDrawDocumentIdFromS3Uri,
  prepareDrawWorkflowNodeTest,
} from "@/util/test-utils";
import { buildTestToken } from "@aspira-nextgen/core/authn-test-utils";
import { deleteAllTableItemsByPk } from "@aspira-nextgen/core/dynamodb-test-utils";
import { WorkflowDomain } from "@aspira-nextgen/graphql/resolvers";
import type { Context } from "aws-lambda";
import { Table } from "sst/node/table";
import { ulid } from "ulidx";
import { afterEach, describe, expect, it } from "vitest";

const ORG_ID = `org_${ulid()}`;
const TOKEN = buildTestToken(ORG_ID);

const drawTestName = "draw-pref-point-age-scenario";
const commonDrawTestInput = {
  drawDataInputPath: buildTestDrawFixturePath({ testName: drawTestName, filename: "draw-data-input.csv" }),
  applicantInputPath: buildTestDrawFixturePath({ testName: drawTestName, filename: "applicant-input.csv" }),
  sortRulesPath: buildTestDrawFixturePath({ testName: drawTestName, filename: "sort-rules.json" }),
  drawConfigPath: buildTestDrawFixturePath({ testName: drawTestName, filename: "draw-config.json" }),
};

/**
 * Tests in this describe block are focused on the coordination of the draw workflow node handler.
 * If you need to test the draw logic itself without the workflow node handler, consider placing them
 * in workflow-draw-node.unit.test.ts.
 */
describe("Draw Workflow Node Handler Tests", () => {
  afterEach(async () => {
    await deleteAllTableItemsByPk(dynamoDBClient, Table.Draw.tableName, [ORG_ID]);
    await deleteAllS3TestDataByOrg(ORG_ID);
  });

  it("Should run a draw using the workflow node handler.", async () => {
    const drawPrep = await prepareDrawWorkflowNodeTest({
      testDrawInput: commonDrawTestInput,
      token: TOKEN,
    });

    const event = buildTestWorkflowDrawNodePayload({
      organizationId: ORG_ID,
      nodeId: drawPrep.workflowNode.id,
      workflowJobId: drawPrep.workflowJob.id,
      applicantDocumentS3Key: drawPrep.applicantDocument.s3Key,
      drawDataDocumentS3Key: drawPrep.drawDataDocument.s3Key,
      override: true,
    });

    const result = await workflowDrawNodeHandler(event, {} as Context, () => {});

    expect(result).toEqual({
      context: {
        organizationId: ORG_ID,
        workflowJobId: drawPrep.workflowJob.id,
        previousNodeId: drawPrep.workflowNode.id,
        nodeId: event.context.nodeId,
      },
      nodeOverrideData: event.nodeOverrideData,
      nodeResultData: {
        [drawPrep.workflowNode.id]: [
          {
            uri: expect.stringMatching(/s3:\/\/.*\/.*\.csv/),
            mime: {
              type: WorkflowMimeDataType.CSV,
              domain: WorkflowDomain.Draw,
              domainModel: DRAW_APPLICANT_DOMAIN_MODEL,
            },
          },
          {
            uri: expect.stringMatching(/s3:\/\/.*\/.*\.csv/),
            mime: {
              type: WorkflowMimeDataType.CSV,
              domain: WorkflowDomain.Draw,
              domainModel: DRAW_DRAW_DATA_DOMAIN_MODEL,
            },
          },
          {
            uri: expect.stringMatching(/s3:\/\/.*\/.*\.csv/),
            mime: {
              type: WorkflowMimeDataType.CSV,
              domain: WorkflowDomain.Draw,
              domainModel: DRAW_METRICS_DOMAIN_MODEL,
            },
          },
        ],
      },
    });

    const applicantResult = findWorkflowDataSource({
      payload: result,
      domain: WorkflowDomain.Draw,
      domainModel: DRAW_APPLICANT_DOMAIN_MODEL,
    });

    const drawDataResult = findWorkflowDataSource({
      payload: result,
      domain: WorkflowDomain.Draw,
      domainModel: DRAW_DRAW_DATA_DOMAIN_MODEL,
    });

    const drawMetricsResult = findWorkflowDataSource({
      payload: result,
      domain: WorkflowDomain.Draw,
      domainModel: DRAW_METRICS_DOMAIN_MODEL,
    });

    const appResultDocId = getTestDrawDocumentIdFromS3Uri(applicantResult?.uri ?? "");
    const drawDataResultDocId = getTestDrawDocumentIdFromS3Uri(drawDataResult?.uri ?? "");
    const drawMetricsResultDocId = getTestDrawDocumentIdFromS3Uri(drawMetricsResult?.uri ?? "");

    const appDoc = await getDocumentEntity({ organizationId: ORG_ID, documentId: appResultDocId });
    expect(appDoc).toBeDefined();

    const drawDoc = await getDocumentEntity({ organizationId: ORG_ID, documentId: drawDataResultDocId });
    expect(drawDoc).toBeDefined();

    const metricsDoc = await getDocumentEntity({ organizationId: ORG_ID, documentId: drawMetricsResultDocId });
    expect(metricsDoc).toBeDefined();
  });
});

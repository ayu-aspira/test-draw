import { getDrawDocumentCollectionEntity } from "@/dynamo/draw-document";
import { dynamoDBClient } from "@/dynamo/dynamo";
import { getDrawDocumentBucketName } from "@/s3/s3";
import {
  DRAW_APPLICANT_DOMAIN_MODEL,
  DRAW_DRAW_DATA_DOMAIN_MODEL,
  DRAW_RESULT_EXPORT_DOMAIN_MODEL,
} from "@/service/draw-constants";
import { workflowNodeHandler } from "@/service/workflow-export-node";
import { findWorkflowDataSource } from "@/step-function/workflow-node-utils";
import { WorkflowMimeDataType } from "@/step-function/workflow-types";
import {
  buildTestDrawFixturePath,
  createTestWorkflowJobResources,
  deleteAllS3TestDataByOrg,
  getTestDrawDocumentIdFromS3Uri,
  prepareDrawWorkflowNodeTest,
} from "@/util/test-utils";
import { buildS3UriString } from "@/util/uri";
import { buildTestToken } from "@aspira-nextgen/core/authn-test-utils";
import { deleteAllTableItemsByPk } from "@aspira-nextgen/core/dynamodb-test-utils";
import { WorkflowDomain } from "@aspira-nextgen/graphql/resolvers";
import type { Context } from "aws-lambda";
import { Table } from "sst/node/table";
import { ulid } from "ulidx";
import { afterEach, describe, expect, it } from "vitest";

const ORG_ID = `org_${ulid()}`;
const TOKEN = buildTestToken(ORG_ID);

describe("Workflow export node tests", () => {
  afterEach(async () => {
    await deleteAllTableItemsByPk(dynamoDBClient, Table.Draw.tableName, [ORG_ID]);
    await deleteAllS3TestDataByOrg(ORG_ID);
  });

  it("Should export S3 resources", async () => {
    const drawTestName = "draw-pref-point-age-scenario";
    const drawPrep = await prepareDrawWorkflowNodeTest({
      token: TOKEN,
      testDrawInput: {
        drawDataInputPath: buildTestDrawFixturePath({ testName: drawTestName, filename: "draw-data-input.csv" }),
        applicantInputPath: buildTestDrawFixturePath({ testName: drawTestName, filename: "applicant-input.csv" }),
        sortRulesPath: buildTestDrawFixturePath({ testName: drawTestName, filename: "sort-rules.json" }),
        drawConfigPath: buildTestDrawFixturePath({ testName: drawTestName, filename: "draw-config.json" }),
      },
    });

    const nodeId = "WorkflowExportNode";
    const result = await workflowNodeHandler(
      {
        context: {
          organizationId: ORG_ID,
          workflowJobId: drawPrep.workflowJob.id,
          nodeId,
        },
        nodeOverrideData: {
          [nodeId]: [
            {
              uri: buildS3UriString(getDrawDocumentBucketName(), drawPrep.applicantDocument.s3Key),
              mime: {
                type: WorkflowMimeDataType.CSV,
                domain: WorkflowDomain.Draw,
                domainModel: DRAW_APPLICANT_DOMAIN_MODEL,
              },
            },
            {
              uri: buildS3UriString(getDrawDocumentBucketName(), drawPrep.drawDataDocument.s3Key),
              mime: {
                type: WorkflowMimeDataType.CSV,
                domain: WorkflowDomain.Draw,
                domainModel: DRAW_DRAW_DATA_DOMAIN_MODEL,
              },
            },
          ],
        },
        nodeResultData: {},
      },
      {} as Context,
      () => {},
    );

    const exportData = findWorkflowDataSource({
      payload: result,
      domain: WorkflowDomain.Draw,
      domainModel: DRAW_RESULT_EXPORT_DOMAIN_MODEL,
    });

    const drawRunResultCollection = await getDrawDocumentCollectionEntity({
      organizationId: ORG_ID,
      documentCollectionId: getTestDrawDocumentIdFromS3Uri(exportData?.uri ?? ""),
    });

    expect(drawRunResultCollection).toMatchObject({
      name: expect.any(String),
      filename: expect.any(String),
      contentType: "application/zip",
    });
  });

  it("Should export nothing when no data is provided", async () => {
    const { workflowJob } = await createTestWorkflowJobResources(TOKEN);

    const nodeId = "WorkflowExportNode";
    const result = await workflowNodeHandler(
      {
        context: {
          organizationId: ORG_ID,
          workflowJobId: workflowJob.id,
          nodeId,
        },
        nodeOverrideData: {},
        nodeResultData: {},
      },
      {} as Context,
      () => {},
    );

    expect(result.nodeResultData[nodeId]).toHaveLength(0);
  });
});

import fs, { createReadStream } from "node:fs";
import path from "node:path";
import { type DrawDocumentEntity, getDrawDocumentEntities } from "@/dynamo/draw-document";
import { DynamoDBPrefix } from "@/dynamo/dynamo";
import { type WorkflowInstanceEntity, getLatestWorkflowInstanceEntity } from "@/dynamo/workflow";
import { type WorkflowJobEntity, createWorkflowJobEntity } from "@/dynamo/workflow-job";
import { type Applicant, readAllApplicants } from "@/s3/applicant";
import { type HuntCode, readAllHuntCodes } from "@/s3/hunt-code";
import { getDrawDocumentBucketName, writeToS3 } from "@/s3/s3";
import { createDraw } from "@/service/draw";
import { createDrawCategory } from "@/service/draw-category";
import { createDrawConfig } from "@/service/draw-config";
import { DRAW_APPLICANT_DOMAIN_MODEL, DRAW_DRAW_DATA_DOMAIN_MODEL } from "@/service/draw-constants";
import { createDrawDocument } from "@/service/draw-document";
import { createDrawSort } from "@/service/draw-sort";
import { createDrawWorkflow } from "@/service/draw-workflow";
import { workflowNodeHandler as workflowDrawNodeHandler } from "@/service/workflow-draw-node";
import { createWorkflowEdge } from "@/service/workflow-edge";
import { createWorkflowNode } from "@/service/workflow-node";
import { workflowNodeHandler as workflowNoopNodeHandler } from "@/service/workflow-noop-node";
import { findWorkflowDataSource } from "@/step-function/workflow-node-utils";
import { WorkflowMimeDataType, type WorkflowNodePayload } from "@/step-function/workflow-types";
import { type CsvTransformableRecord, transformToCsv } from "@/util/csv";
import { buildS3UriString } from "@/util/uri";
import type { Token } from "@aspira-nextgen/core/authn";
import { withConsistencyRetries } from "@aspira-nextgen/core/dynamodb-test-utils";
import {
  type DrawConfig,
  DrawDocumentProcessingStatus,
  DrawDocumentType,
  DrawSortDirection,
  type DrawSortRuleInput,
  type PrepareDrawUploadInput,
  type Workflow,
  WorkflowDomain,
  type WorkflowNode,
  WorkflowNodeType,
} from "@aspira-nextgen/graphql/resolvers";
import { DeleteObjectCommand, ListObjectsCommand, S3Client } from "@aws-sdk/client-s3";
import {
  DescribeExecutionCommand,
  ExecutionStatus,
  StartExecutionCommand,
  StartSyncExecutionCommand,
} from "@aws-sdk/client-sfn";
import type { Context } from "aws-lambda";
import { parse as csvParser } from "csv-parse";
import { Bucket } from "sst/node/bucket";
import { ulid } from "ulidx";
import { vi } from "vitest";
import { queueWorkflowJob } from "#service/workflow.ts";

const TEST_S3_CLIENT = new S3Client({});

/**
 * @private visible for testing.
 */
export const prepareDrawWorkflowNodeTest = async (input: {
  testDrawInput: TestDrawInputFixtures;
  token: Token;
}): Promise<{
  drawConfig: DrawConfig;
  applicantDocument: DrawDocumentEntity;
  drawDataDocument: DrawDocumentEntity;
  workflow: Workflow;
  workflowInstance: WorkflowInstanceEntity;
  workflowNode: WorkflowNode;
  workflowJob: WorkflowJobEntity;
}> => {
  const { testDrawInput, token } = input;

  const { drawDataDocument, applicantDocument } = await createAndUploadTestDrawInputDocuments(testDrawInput, token);

  const drawSortRules = JSON.parse(fs.readFileSync(testDrawInput.sortRulesPath, "utf-8")) as DrawSortRuleInput[];

  const drawSort = await createTestDrawSort({ rules: drawSortRules }, token);

  const { workflow, workflowInstance, workflowJob } = await createTestWorkflowJobResources(token);

  const workflowNode = await createTestWorkflowNode(
    token,
    { workflowId: workflow.id },
    WorkflowNodeType.WorkflowDrawNode,
  );

  const drawConfig = await createDrawConfig(
    {
      name: `draw-config-${ulid()}`,
      sortId: drawSort.id,
      usePoints: false,
      workflowNodeId: workflowNode.id,
      quotaRules: {
        ruleFlags: [],
      },
    },
    token,
  );

  return {
    drawConfig,
    applicantDocument,
    drawDataDocument,
    workflow,
    workflowInstance,
    workflowNode,
    workflowJob,
  };
};

/**
 * @private visible for testing.
 */
export const runTestWorkflowDrawNode = async (input: {
  testDrawInput: TestDrawInputFixtures;
  token: Token;
}): Promise<{
  drawConfig: DrawConfig;
  workflow: Workflow;
  workflowInstance: WorkflowInstanceEntity;
  workflowJob: WorkflowJobEntity;
  huntCodeDrawResultDocument: DrawDocumentEntity;
  applicantDrawResultDocument: DrawDocumentEntity;
  allApplicantResults: Applicant[];
  allHuntCodeResults: HuntCode[];
}> => {
  const prep = await prepareDrawWorkflowNodeTest(input);

  const organizationId = input.token.claims.organizationId;

  const drawNodeId = "draw-node-id";
  const drawNodeHandlerResult = await workflowDrawNodeHandler(
    buildTestWorkflowDrawNodePayload({
      workflowJobId: prep.workflowJob.id,
      nodeId: drawNodeId,
      organizationId,
      applicantDocumentS3Key: prep.applicantDocument.s3Key,
      drawDataDocumentS3Key: prep.drawDataDocument.s3Key,
      override: true,
    }),
    {} as Context,
    () => {},
  );

  const applicantResult = findWorkflowDataSource({
    payload: drawNodeHandlerResult,
    domain: WorkflowDomain.Draw,
    domainModel: DRAW_APPLICANT_DOMAIN_MODEL,
  });
  const applicantDrawResultDocumentId = getTestDrawDocumentIdFromS3Uri(applicantResult?.uri ?? "");

  const drawDataResult = findWorkflowDataSource({
    payload: drawNodeHandlerResult,
    domain: WorkflowDomain.Draw,
    domainModel: DRAW_DRAW_DATA_DOMAIN_MODEL,
  });
  const drawDataResultDocumentId = getTestDrawDocumentIdFromS3Uri(drawDataResult?.uri ?? "");

  if (!applicantDrawResultDocumentId || !drawDataResultDocumentId) {
    throw new Error("Draw run documents not found.");
  }

  const documents = await getDrawDocumentEntities(
    [applicantDrawResultDocumentId, drawDataResultDocumentId],
    organizationId,
  );

  const huntCodeDrawResultDocument = documents.find((doc) => doc.type === DrawDocumentType.HuntCodes);
  const applicantDrawResultDocument = documents.find((doc) => doc.type === DrawDocumentType.Applicants);

  if (!huntCodeDrawResultDocument || !applicantDrawResultDocument) {
    throw new Error("Draw run documents not found.");
  }

  const allApplicantResults = await readAllApplicants(applicantDrawResultDocument.s3Key);
  const allHuntCodeResults = await readAllHuntCodes(huntCodeDrawResultDocument.s3Key);

  return {
    workflow: prep.workflow,
    workflowInstance: prep.workflowInstance,
    workflowJob: prep.workflowJob,
    drawConfig: prep.drawConfig,
    huntCodeDrawResultDocument,
    applicantDrawResultDocument,
    allApplicantResults,
    allHuntCodeResults,
  };
};

/**
 * @private visible for testing.
 */
export const uploadTestRecordsToS3AsCsv = async (input: {
  s3Key: string;
  records: CsvTransformableRecord[];
}) => {
  const { s3Key, records } = input;

  const csv = transformToCsv(
    Object.keys(records[0]).map((k) => ({ field: k, header: k })),
    records,
  );

  await writeToS3({
    s3Key,
    contentType: "text/csv",
    bytes: Buffer.from(csv),
  });
};

/**
 * @private visible for testing.
 */
export const deleteTestS3Data = async (keys: string[]) => {
  for (const key of new Set(keys)) {
    await TEST_S3_CLIENT.send(new DeleteObjectCommand({ Bucket: Bucket.DrawDocumentBucket.bucketName, Key: key }));
  }
};

/**
 * @private visible for testing.
 */
export const deleteAllS3TestDataByOrg = async (orgId: string) => {
  const { Contents } = await TEST_S3_CLIENT.send(
    new ListObjectsCommand({ Bucket: Bucket.DrawDocumentBucket.bucketName, Prefix: `${orgId}/` }),
  );

  if (!Contents) {
    return;
  }

  const keys = Contents.map((item) => item.Key);
  await deleteTestS3Data(keys.filter((key) => key !== undefined));
};

/**
 * @private visible for testing.
 */
export const createTestDrawCategory = async (token: Token) => {
  const drawCategoryName = `test category ${ulid()}`;
  return await createDrawCategory({ name: drawCategoryName }, token);
};

/**
 * @private visible for testing.
 */
export const createTestDrawDocument = async (
  token: Token,
  input?: {
    filename?: string;
    name?: string;
    type?: DrawDocumentType;
    contentType?: string;
    createdBy?: string;
    processingStatus?: DrawDocumentProcessingStatus;
    workflowNodeId?: string;
  },
) => {
  const {
    filename = "test.csv",
    name = `huntcodes ${ulid()}`,
    type = DrawDocumentType.HuntCodes,
    contentType = "text/csv",
    createdBy = "system",
    processingStatus = DrawDocumentProcessingStatus.ValidationFinished,
    workflowNodeId,
  } = input ?? {};

  const documentRequest: PrepareDrawUploadInput & {
    processingStatus: DrawDocumentProcessingStatus;
    createdBy: string;
  } = {
    filename,
    name,
    type,
    contentType,
    createdBy,
    processingStatus,
    workflowNodeId,
  };

  return await createDrawDocument(documentRequest, token.claims.organizationId);
};

/**
 * @private visible for testing.
 */
export const createTestDrawDocuments = async (token: Token, processingStatus?: DrawDocumentProcessingStatus) => {
  const drawDataDocument = await createTestDrawDocument(token, {
    filename: "test file.csv",
    name: `huntcodes ${ulid()}`,
    type: DrawDocumentType.HuntCodes,
    contentType: "text/csv",
    createdBy: "system",
    processingStatus: processingStatus || DrawDocumentProcessingStatus.ValidationFinished,
  });

  const applicantDocument = await createTestDrawDocument(token, {
    filename: "test.csv",
    name: `applicants ${ulid()}`,
    type: DrawDocumentType.Applicants,
    contentType: "text/csv",
    createdBy: "system",
    processingStatus: processingStatus || DrawDocumentProcessingStatus.ValidationFinished,
  });

  return { drawDataDocument, applicantDocument };
};

/**
 * @private visible for testing.
 */
export type TestDrawInputFixtures = {
  drawDataInputPath: string;
  applicantInputPath: string;
  sortRulesPath: string;
  drawConfigPath: string;
};

export const readAndParseTestDrawCSVFixture = async <T>(
  filepath: string,
  mapperFn: (rows: Record<string, string>[]) => T[],
): Promise<T[]> => {
  const readStream = createReadStream(filepath);
  const rowData: Record<string, string>[] = [];

  const parser = csvParser({
    columns: true,
  });

  await new Promise((resolve, reject) => {
    parser.on("data", (row) => rowData.push(row));

    parser.on("end", () => {
      resolve(rowData);
    });

    parser.on("error", reject);

    readStream.pipe(parser);
  });

  return mapperFn(rowData);
};

export const readAndParseTestDrawJSONFixture = <T>(filepath: string): T => require(filepath) as T;

export const buildTestDrawFixturePath = (input: { testName: string; filename: string }) =>
  path.resolve(__dirname, "../test-fixtures", input.testName, input.filename);

/**
 * @private visible for testing.
 */
export const createAndUploadTestDrawInputDocuments = async (
  testFixtures: TestDrawInputFixtures,
  token: Token,
): Promise<{ drawDataDocument: DrawDocumentEntity; applicantDocument: DrawDocumentEntity }> => {
  const drawDataInputFileContents = fs.readFileSync(testFixtures.drawDataInputPath, "utf-8");
  const applicantInputFileContents = fs.readFileSync(testFixtures.applicantInputPath, "utf-8");

  const { drawDataDocument, applicantDocument } = await createTestDrawDocuments(token);

  await Promise.all([
    writeToS3({
      s3Key: applicantDocument.s3Key,
      contentType: "text/csv",
      bytes: Buffer.from(applicantInputFileContents),
    }),
    writeToS3({
      s3Key: drawDataDocument.s3Key,
      contentType: "text/csv",
      bytes: Buffer.from(drawDataInputFileContents),
    }),
  ]);

  return {
    drawDataDocument,
    applicantDocument,
  };
};

/**
 * @private visible for testing.
 */
export const createTestDraw = async (token: Token, input: { name?: string; drawCategoryId?: string } = {}) => {
  const { drawDataDocument, applicantDocument } = await createTestDrawDocuments(
    token,
    DrawDocumentProcessingStatus.ValidationFinished,
  );

  const drawCategoryId = input.drawCategoryId || (await createTestDrawCategory(token)).id;

  return await createDraw(
    {
      name: input.name || `test draw ${ulid()}`,
      drawDataDocumentId: drawDataDocument.id,
      applicantDocumentId: applicantDocument.id,
      drawCategoryId,
    },
    token,
  );
};

/**
 * @private visible for testing.
 */
export const createTestDrawSort = async (input: { name?: string; rules?: DrawSortRuleInput[] }, token: Token) =>
  await createDrawSort(
    {
      name: input.name ?? `test sort ${ulid()}`,
      rules: input.rules ?? [{ field: "pointBalance", direction: DrawSortDirection.Asc }],
    },
    token,
  );

/**
 * @private visible for testing.
 */
export const createTestDrawWorkflow = async (token: Token, input?: { name?: string; drawCategoryId?: string }) => {
  const name = input?.name || `test workflow ${ulid()}`;
  const drawCategoryId = input?.drawCategoryId || (await createTestDrawCategory(token)).id;

  const drawWorkflow = await createDrawWorkflow({ name, drawCategoryId }, token);

  await withConsistencyRetries(async () => {
    const entity = await getLatestWorkflowInstanceEntity({
      organizationId: token.claims.organizationId,
      workflowId: drawWorkflow.workflow.id,
    });
    if (!entity) {
      throw new Error("Workflow instance not found.");
    }
  });

  return drawWorkflow;
};

/**
 * @private visible for testing.
 */
export const createTestWorkflow = async (
  token: Token,
): Promise<{ workflow: Workflow; workflowInstance: WorkflowInstanceEntity }> => {
  const drawWorkflow = await createTestDrawWorkflow(token);
  let workflowInstance: WorkflowInstanceEntity | undefined;

  await withConsistencyRetries(async () => {
    workflowInstance = await getLatestWorkflowInstanceEntity({
      organizationId: token.claims.organizationId,
      workflowId: drawWorkflow.workflow.id,
    });

    if (!workflowInstance) {
      throw new Error("Failed to find a workflow instance for the created workflow.");
    }
  });

  return { workflow: drawWorkflow.workflow, workflowInstance: workflowInstance as WorkflowInstanceEntity };
};

/**
 * @private visible for testing.
 */
export const createTestWorkflowNode = async (token: Token, id?: { workflowId?: string }, type?: WorkflowNodeType) => {
  const { organizationId } = token.claims;

  let workflowInstance: WorkflowInstanceEntity | undefined;

  if (id?.workflowId) {
    workflowInstance = await getLatestWorkflowInstanceEntity({ organizationId, workflowId: id?.workflowId });
  }

  if (!workflowInstance) {
    const { workflowInstance: newWorkflowInstance } = await createTestWorkflow(token);
    workflowInstance = newWorkflowInstance;
  }

  const input = {
    workflowId: workflowInstance.workflowId,
    position: { x: Math.floor(Math.random() * 100), y: Math.floor(Math.random() * 100) },
    type: type ?? WorkflowNodeType.WorkflowNoopNode,
  };

  return await createWorkflowNode(input, token);
};

/**
 * @private visible for testing.
 */
export const createTestWorkflowEdge = async (
  token: Token,
  input: { workflowId: string; sourceNodeId: string; targetNodeId: string },
) => {
  const { workflowId, sourceNodeId, targetNodeId } = input;

  return await createWorkflowEdge(
    {
      workflowId,
      sourceNodeId,
      targetNodeId,
    },
    token,
  );
};

/**
 * @private visible for testing.
 */
export const generateSample = async <T>(callback: () => Promise<T>, n?: number): Promise<T[]> => {
  const count = n ?? Math.ceil(Math.random() * 10);

  const things: T[] = [];
  for (let i = 0; i < count; i++) {
    const thing: T = await callback();
    things.push(thing);
  }

  return things;
};

/**
 * @private visible for testing.
 */
export const createTestWorkflowJobResources = async (
  token: Token,
): Promise<{
  workflow: Workflow;
  workflowInstance: WorkflowInstanceEntity;
  workflowJob: WorkflowJobEntity;
}> => {
  const { workflow, workflowInstance } = await createTestWorkflow(token);

  const workflowJob = await createWorkflowJobEntity({
    organizationId: token.claims.organizationId,
    workflowInstanceId: workflowInstance.id,
    executionArn: "execution arn",
    createdBy: token.sub,
  });

  return {
    workflow,
    workflowInstance,
    workflowJob,
  };
};

/**
 * @private
 */
export const getTestDrawDocumentIdFromS3Uri = (uri: string): string =>
  new URL(uri ?? "").pathname.substring(1).split("/")[1];

/**
 * @private
 */
export const buildTestWorkflowDrawNodePayload = (input: {
  organizationId: string;
  workflowJobId: string;
  nodeId: string;
  applicantDocumentS3Key: string;
  drawDataDocumentS3Key: string;
  // If provided, will put the data in nodeOverrideData.
  override?: boolean;
}): WorkflowNodePayload => {
  const dataMap = {
    [input.nodeId]: [
      {
        uri: buildS3UriString(getDrawDocumentBucketName(), input.applicantDocumentS3Key),
        mime: {
          type: WorkflowMimeDataType.CSV,
          domain: WorkflowDomain.Draw,
          domainModel: DRAW_APPLICANT_DOMAIN_MODEL,
        },
      },
      {
        uri: buildS3UriString(getDrawDocumentBucketName(), input.drawDataDocumentS3Key),
        mime: {
          type: WorkflowMimeDataType.CSV,
          domain: WorkflowDomain.Draw,
          domainModel: DRAW_DRAW_DATA_DOMAIN_MODEL,
        },
      },
    ],
  };

  return {
    context: {
      workflowJobId: input.workflowJobId,
      organizationId: input.organizationId,
      nodeId: input.nodeId,
    },
    nodeOverrideData: input.override ? dataMap : {},
    nodeResultData: !input.override ? dataMap : {},
  };
};

/**
 * @private visible for testing.
 */
export const createRandomTestWorkflowId = () => `${DynamoDBPrefix.WORKFLOW}_${ulid()}`;

/**
 * @private visible for testing.
 */
export const createRandomTestWorkflowNodeId = () => `${DynamoDBPrefix.WORKFLOW_NODE}_${ulid()}`;

/**
 * @private
 */
export const createTestWorkflowWithResults = async (token: Token) => {
  const { organizationId } = token.claims;

  const { workflow } = await createTestWorkflow(token);
  const node = await createTestWorkflowNode(token, { workflowId: workflow.id });
  const workflowJob = await queueWorkflowJob({ workflowId: workflow.id }, token);
  const drawTestName = "draw-pref-point-age-scenario";
  const commonDrawTestInput = {
    drawDataInputPath: buildTestDrawFixturePath({ testName: drawTestName, filename: "draw-data-input.csv" }),
    applicantInputPath: buildTestDrawFixturePath({ testName: drawTestName, filename: "applicant-input.csv" }),
    sortRulesPath: buildTestDrawFixturePath({ testName: drawTestName, filename: "sort-rules.json" }),
    drawConfigPath: buildTestDrawFixturePath({ testName: drawTestName, filename: "draw-config.json" }),
  };
  const drawPrep = await prepareDrawWorkflowNodeTest({
    testDrawInput: commonDrawTestInput,
    token: token,
  });

  const nodePayload = buildTestWorkflowDrawNodePayload({
    organizationId,
    workflowJobId: workflowJob.id,
    nodeId: node.id,
    applicantDocumentS3Key: drawPrep.applicantDocument.s3Key,
    drawDataDocumentS3Key: drawPrep.drawDataDocument.s3Key,
  });

  const event = {
    ...nodePayload,
    context: {
      ...nodePayload.context,
      previousNodeId: node.id,
    },
  };

  await workflowNoopNodeHandler(event, {} as Context, () => {});

  return { workflow, workflowJob, node };
};

/**
 * @private
 *
 * Mocks the SFNClient for testing.
 * This is useful when you want to test the step function workflow without actually running the step function.
 *
 * Currently works with:
 *
 *    @link createTestWorkflowWithResults
 */
export const mockSFNClientForTesting = () => {
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
};

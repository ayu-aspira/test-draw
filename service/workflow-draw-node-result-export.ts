import { DrawDocumentProcessingStatus, DrawDocumentType } from "@aspira-nextgen/graphql/resolvers";
import type { DrawDocumentEntity } from "#dynamo/draw-document.ts";
import type { Applicant } from "#s3/applicant.ts";
import type { HuntCode } from "#s3/hunt-code.ts";
import { writeToS3 } from "#s3/s3.ts";
import { createDrawDocument } from "#service/draw-document.ts";
import type { DrawMetrics } from "#service/workflow-draw-node-metrics.ts";
import { logInfo } from "#step-function/workflow-node-utils.ts";
import type { WorkflowNodeContext } from "#step-function/workflow-types.ts";
import { type CsvHeaderMapping, type CsvTransformableRecord, transformToCsv } from "#util/csv.ts";

type DrawExportResult = {
  huntCodeDrawResultDocument: DrawDocumentEntity;
  applicantDrawResultDocument: DrawDocumentEntity;
  drawMetricsResultDocument: DrawDocumentEntity;
};

export const exportResults = async (input: {
  context: WorkflowNodeContext;
  huntCodeResults: HuntCode[];
  applicantResults: Applicant[];
  drawNumChoices: number;
  drawMetricsResults: DrawMetrics[];
}): Promise<DrawExportResult> => {
  const { applicantResults, huntCodeResults, drawNumChoices, drawMetricsResults, context } = input;

  logInfo(context, "Exporting draw results.");

  const { huntCodeDrawResultDocument, applicantDrawResultDocument, drawMetricsResultDocument } =
    await exportResultsToCSV({
      context,
      huntCodeResults,
      applicantResults,
      drawNumChoices,
      drawMetricsResults,
    });

  logInfo(context, "Draw result export finished.");

  return {
    huntCodeDrawResultDocument,
    applicantDrawResultDocument,
    drawMetricsResultDocument,
  };
};

export const exportResultsToCSV = async (input: {
  context: WorkflowNodeContext;
  huntCodeResults: HuntCode[];
  applicantResults: Applicant[];
  drawNumChoices: number;
  drawMetricsResults: DrawMetrics[];
}): Promise<DrawExportResult> => {
  const { context, huntCodeResults, applicantResults, drawNumChoices, drawMetricsResults } = input;

  const huntCodeDrawResultDocument = await exportItemsToCsv<HuntCode>({
    items: huntCodeResults,
    headerMappings: [
      { field: "huntCode", header: "hunt_code" },
      { field: "isValid", header: "is_valid" },
      { field: "isInDraw", header: "in_the_draw" },
      { field: "totalQuota", header: "total_quota" },
      {
        field: "nonResidents",
        header: "nr_cap_chk",
        valueExtractor: (record) => (record.nonResidents as HuntCode["nonResidents"]).hasHardCap,
      },
      {
        field: "nonResidents",
        header: "nrcap_prect",
        valueExtractor: (record) => (record.nonResidents as HuntCode["nonResidents"]).capPercent,
      },
      {
        field: "nonResidents",
        header: "nrcap_amt",
        valueExtractor: (record) => (record.nonResidents as HuntCode["nonResidents"]).totalQuota,
      },
      {
        field: "nonResidents",
        header: "nrcap_bal",
        valueExtractor: (record) => (record.nonResidents as HuntCode["nonResidents"]).quotaBalance,
      },
      {
        field: "wpRes",
        header: "unLPP_prect",
        valueExtractor: (record) => (record.wpRes as HuntCode["wpRes"]).capPercent,
      },
      {
        field: "wpRes",
        header: "unLPP_amt",
        valueExtractor: (record) => (record.wpRes as HuntCode["wpRes"]).totalQuota,
      },
      {
        field: "wpRes",
        header: "unLPP_bal",
        valueExtractor: (record) => (record.wpRes as HuntCode["wpRes"]).quotaBalance,
      },
      { field: "previousQuotaBalance", header: "previous_quota_balance" },
      { field: "totalQuotaInThisDraw", header: "total_quota_in_this_draw" },
      { field: "quotaBalance", header: "quota_balance" },
      { field: "quotaBalanceInThisDraw", header: "quota_balance_in_this_draw" },
      { field: "totalQuotaAwarded", header: "total_quota_awarded" },
      { field: "quotaAwardedInThisDraw", header: "quota_awarded_in_this_draw" },
      {
        field: "nonResidents",
        header: "nr_quota_awarded_in_this_draw",
        valueExtractor: (record) => (record.nonResidents as HuntCode["nonResidents"]).quotaAwardedInThisDraw,
      },
      {
        field: "nonResidents",
        header: "nr_total_quota_awarded",
        valueExtractor: (record) => (record.nonResidents as HuntCode["nonResidents"]).totalQuotaAwarded,
      },
      {
        field: "residents",
        header: "r_quota_awarded_in_this_draw",
        valueExtractor: (record) => (record.residents as HuntCode["residents"]).quotaAwardedInThisDraw,
      },
      {
        field: "residents",
        header: "r_total_quota_awarded",
        valueExtractor: (record) => (record.residents as HuntCode["residents"]).totalQuotaAwarded,
      },
    ],
    documentName: `${context.workflowJobId}_${context.nodeId}_hunt_code_results`,
    documentType: DrawDocumentType.HuntCodes,
    organizationId: context.organizationId,
  });

  const applicantHeaderMappings: CsvHeaderMapping<Applicant>[] = [
    { field: "applicationNumber", header: "application_number" },
    { field: "age", header: "age" },
    { field: "residency", header: "residency" },
    { field: "pointBalance", header: "point_balance" },
  ];

  for (let i = 1; i <= drawNumChoices; i++) {
    applicantHeaderMappings.push({
      field: "choices",
      header: `choice${i}`,
      valueExtractor: (record) => {
        const choices = record.choices as string[] | undefined;
        return choices ? (choices[i - 1] ?? "") : "";
      },
    });
  }

  applicantHeaderMappings.push(
    { field: "drawOutcome", header: "draw_outcome" },
    { field: "choiceOrdinalAwarded", header: "choice_ordinal_awarded" },
    { field: "choiceAwarded", header: "choice_awarded" },
  );

  const applicantDrawResultDocument = await exportItemsToCsv<Applicant>({
    items: applicantResults,
    headerMappings: applicantHeaderMappings,
    documentName: `${context.workflowJobId}_${context.nodeId}_applicant_results`,
    documentType: DrawDocumentType.Applicants,
    organizationId: context.organizationId,
  });

  const drawMetricsHeaderMappings: CsvHeaderMapping<DrawMetrics>[] = [
    { field: "metric", header: "metric" },
    { field: "value", header: "value" },
  ];

  const drawMetricsResultDocument = await exportItemsToCsv<DrawMetrics>({
    items: drawMetricsResults,
    headerMappings: drawMetricsHeaderMappings,
    documentName: `${context.workflowJobId}_${context.nodeId}_draw_metrics`,
    documentType: DrawDocumentType.DrawMetrics,
    organizationId: context.organizationId,
  });

  return {
    huntCodeDrawResultDocument,
    applicantDrawResultDocument,
    drawMetricsResultDocument,
  };
};

const exportItemsToCsv = async <T extends CsvTransformableRecord>(input: {
  items: T[];
  headerMappings: CsvHeaderMapping<T>[];
  documentName: string;
  documentType: DrawDocumentType;
  organizationId: string;
}): Promise<DrawDocumentEntity> => {
  const { items, organizationId, headerMappings, documentName, documentType } = input;

  const drawDocumentEntity = await createDrawDocument(
    {
      name: `${documentName}`,
      filename: `${documentName}.csv`,
      contentType: "text/csv",
      createdBy: "system",
      processingStatus: DrawDocumentProcessingStatus.ValidationFinished,
      type: documentType,
    },
    organizationId,
  );

  const csv = transformToCsv(headerMappings, items);

  await writeToS3({
    s3Key: drawDocumentEntity.s3Key,
    contentType: drawDocumentEntity.contentType,
    bytes: Buffer.from(csv),
  });

  return drawDocumentEntity;
};

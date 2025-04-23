import { type DrawDocumentEntity, getDocumentEntity, updateProcessingStatus } from "@/dynamo/draw-document";
import { validateApplicantRows } from "@/s3/applicant";
import { getOrganizationAndDocumentIdsFromKey, isKeyFormatValid } from "@/s3/draw-document";
import { validateHuntCodeRows } from "@/s3/hunt-code";
import { readCSVFromS3 } from "@/s3/s3";
import { DrawDocumentProcessingStatus, DrawDocumentType } from "@aspira-nextgen/graphql/resolvers";
import type { S3Event, SQSEvent } from "aws-lambda";

export const validateCsvHandler = async (event: SQSEvent): Promise<void> => {
  for (const sqsRecord of event.Records) {
    const s3Event = JSON.parse(sqsRecord.body) as S3Event;

    if (!s3Event.Records) {
      continue;
    }

    for (const s3Record of s3Event.Records) {
      const s3Key = decodeURIComponent(s3Record.s3.object.key.replace(/\+/g, " "));

      if (!isKeyFormatValid(s3Key)) {
        continue;
      }

      const { organizationId, documentId } = getOrganizationAndDocumentIdsFromKey(s3Key);

      const drawDocumentEntity = await getDocumentEntity({ organizationId, documentId });

      if (drawDocumentEntity.processingStatus === DrawDocumentProcessingStatus.ValidationFinished) {
        continue;
      }

      try {
        await updateProcessingStatus(organizationId, documentId, DrawDocumentProcessingStatus.ValidationStarted);
        await validateCsv(drawDocumentEntity);
        await updateProcessingStatus(organizationId, documentId, DrawDocumentProcessingStatus.ValidationFinished);
      } catch (err) {
        const errCast = err as Error;

        await updateProcessingStatus(
          organizationId,
          documentId,
          DrawDocumentProcessingStatus.ValidationFailed,
          JSON.stringify({
            message: errCast.message,
            stack: errCast.stack,
          }),
        );
      }
    }
  }
};

type ValidationHandlerFn = (rows: Record<string, string>[]) => void;

const VALIDATION_FN_BY_DRAW_DOCUMENT_TYPE: Record<DrawDocumentType, ValidationHandlerFn | null> = {
  [DrawDocumentType.HuntCodes]: validateHuntCodeRows,
  [DrawDocumentType.Applicants]: validateApplicantRows,
  [DrawDocumentType.DrawMetrics]: null,
};

const SUPPORTED_DRAW_DOCUMENT_TYPES = [DrawDocumentType.HuntCodes, DrawDocumentType.Applicants];

const validateCsv = async (drawDocumentEntity: DrawDocumentEntity): Promise<void> => {
  const { type: drawDocumentType, s3Key } = drawDocumentEntity;

  if (!SUPPORTED_DRAW_DOCUMENT_TYPES.includes(drawDocumentType)) {
    throw new Error(`Unsupported draw document type: ${drawDocumentType}`);
  }

  const validateFn = VALIDATION_FN_BY_DRAW_DOCUMENT_TYPE[drawDocumentType];

  validateFn?.(await readCSVFromS3(s3Key));
};

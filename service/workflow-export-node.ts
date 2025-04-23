import { PassThrough, type Readable } from "node:stream";
import type { DrawDocumentCollectionEntity } from "@/dynamo/draw-document";
import { createS3ReadStream, createS3UploadStream } from "@/s3/s3";
import { getDrawDocumentBucketName } from "@/s3/s3";
import { DRAW_RESULT_EXPORT_DOMAIN_MODEL } from "@/service/draw-constants";
import { createDrawDocumentCollection } from "@/service/draw-document";
import { wrapWorkflowHandler } from "@/step-function/workflow-node-utils";
import { WorkflowMimeDataType, type WorkflowNodeHandler } from "@/step-function/workflow-types";
import { buildS3UriString, getS3KeyFromURI, isS3Uri, parseUri } from "@/util/uri";
import { WorkflowDomain } from "@aspira-nextgen/graphql/resolvers";
import archiver from "archiver";

const FILE_EXTENSION_BY_MIME_TYPE: { [key in WorkflowMimeDataType]: string | undefined } = {
  [WorkflowMimeDataType.ANY]: undefined,
  [WorkflowMimeDataType.CSV]: "csv",
  [WorkflowMimeDataType.JSON]: "json",
  [WorkflowMimeDataType.ZIP]: "zip",
};

const workflowNodeFn: WorkflowNodeHandler = async (event) => {
  const { context, nodeResultData, nodeOverrideData } = event;

  const files = [];

  const resultData = Object.values(nodeResultData).flat();

  // Handling the case where a data node could be attached
  // to this export node.
  const overrideData = Object.values(nodeOverrideData[context.nodeId] ?? []).flat();

  const s3SourceFiles = Object.values([...resultData, ...overrideData])
    .flat()
    .filter((d) => isS3Uri(parseUri(d.uri)));

  if (s3SourceFiles.length === 0) {
    return [];
  }

  for (const [index, dataEntry] of s3SourceFiles.entries()) {
    const { mime, uri } = dataEntry;

    const extension = FILE_EXTENSION_BY_MIME_TYPE[mime.type] ? `.${FILE_EXTENSION_BY_MIME_TYPE[mime.type]}` : "";
    files.push({
      // TODO: where can we get a better name?
      filename: `${mime.domain.toLowerCase()}_${mime.domainModel.toLowerCase()}_${index}${extension}`,
      readableStream: await createS3ReadStream(getS3KeyFromURI(parseUri(uri))),
    });
  }

  const drawDocumentCollectionEntity = await zipFiles({
    files,
    organizationId: context.organizationId,
    // TODO: this should come from the export node config once available.
    collectionName: `${context.workflowJobId}_${context.nodeId}`,
  });

  return [
    {
      mime: {
        type: WorkflowMimeDataType.ZIP,
        domain: WorkflowDomain.Draw,
        domainModel: DRAW_RESULT_EXPORT_DOMAIN_MODEL,
      },
      uri: buildS3UriString(getDrawDocumentBucketName(), drawDocumentCollectionEntity.s3Key),
    },
  ];
};

export const workflowNodeHandler = wrapWorkflowHandler(workflowNodeFn);

const zipFiles = async (input: {
  files: { filename: string; readableStream: Readable }[];
  organizationId: string;
  collectionName: string;
}): Promise<DrawDocumentCollectionEntity> => {
  const { files, organizationId, collectionName } = input;

  const archive = archiver("zip", { zlib: { level: 9 } });

  files.forEach(({ readableStream, filename }) => {
    archive.append(readableStream, { name: filename });
  });

  const drawDocumentCollectionName = `${collectionName}_draw_results`;

  const drawDocumentCollectionEntity = await createDrawDocumentCollection(
    {
      name: `${drawDocumentCollectionName}`,
      filename: `${drawDocumentCollectionName}.zip`,
      contentType: "application/zip",
    },
    organizationId,
  );

  const passThroughStream = new PassThrough();

  const uploadStream = createS3UploadStream({
    s3Key: drawDocumentCollectionEntity.s3Key,
    contentType: drawDocumentCollectionEntity.contentType,
    readStream: passThroughStream,
  });

  archive.pipe(passThroughStream);

  archive.finalize();

  await uploadStream.done();

  return drawDocumentCollectionEntity;
};

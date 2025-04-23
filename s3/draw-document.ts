import { DynamoDBPrefix } from "@/dynamo/dynamo";

export const generateDocumentS3Key = (organizationId: string, documentId: string, filename: string): string => {
  // Presigned S3 URLs has a limitation where the name of the file being uploaded
  // must match the target filename in the key.
  // So we set up a directory structure to ensure the organizationId and documentId is part of the key.
  return `${organizationId}/${documentId}/${filename}`;
};

const KEY_FORMAT = new RegExp(`^org_[a-zA-Z0-9-_]+\/${DynamoDBPrefix.DRAW_DOCUMENT}_[a-zA-Z0-9-_]+\/.+`);

/**
 * @private visible for testing.
 */
export const isKeyFormatValid = (s3Key: string): boolean => {
  if (s3Key.match(KEY_FORMAT)) {
    return true;
  }

  return false;
};

export const getOrganizationAndDocumentIdsFromKey = (s3Key: string): { organizationId: string; documentId: string } => {
  if (!isKeyFormatValid(s3Key)) {
    throw new Error(`Invalid S3 key format: ${s3Key}`);
  }

  const split = s3Key.split("/");
  return {
    organizationId: split[0],
    documentId: split[1],
  };
};

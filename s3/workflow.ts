import { readAllFromS3, writeToS3 } from "@/s3/s3";
import type { StateMachine } from "asl-types";

export const uploadAslToS3 = async (input: {
  asl: StateMachine;
  organizationId: string;
  workflowId: string;
  workflowInstanceId: string;
}): Promise<string> => {
  const { asl, organizationId, workflowId, workflowInstanceId } = input;

  const s3Key = `${organizationId}/${workflowId}/${workflowInstanceId}/asl.json`;

  await writeToS3({
    s3Key,
    contentType: "application/json",
    bytes: Buffer.from(JSON.stringify(asl)),
  });

  return s3Key;
};

export const readAslFromS3 = async (s3Key: string): Promise<StateMachine> => {
  const result = await readAllFromS3(s3Key);
  return JSON.parse(result.toString()) as StateMachine;
};

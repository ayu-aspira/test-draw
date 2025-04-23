import { DescribeExecutionCommand, type ExecutionStatus, SFNClient } from "@aws-sdk/client-sfn";

export const sfnClient = new SFNClient({ region: process.env.AWS_REGION });

export const getWorkflowExecutionStatus = async (executionArn: string): Promise<ExecutionStatus> => {
  const result = await sfnClient.send(new DescribeExecutionCommand({ executionArn }));

  if (!result || !result.status) {
    throw new Error(`Failed to get status for execution ${executionArn}`);
  }

  return result.status;
};

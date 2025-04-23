import { DrawConfigQuotaRule } from "@aspira-nextgen/graphql/resolvers";
import { z } from "zod";
import { DynamoDBPrefix } from "#dynamo/dynamo.ts";
import { idStringPrefix, trimmedString } from "#validation/common.ts";

export const CreateDrawConfigInputSchema = z.object({
  workflowNodeId: z.string(),
  name: trimmedString(),
  sortId: z.string().optional(),
  usePoints: z.boolean(),
  quotaRules: z.object({
    ruleFlags: z.array(z.nativeEnum(DrawConfigQuotaRule)),
  }),
  applicants: z.array(z.string()).optional(),
});

export const DrawConfigID = idStringPrefix(DynamoDBPrefix.DRAW_CONFIG);

export const UpdateDrawConfigInputSchema = CreateDrawConfigInputSchema.partial().omit({ workflowNodeId: true }).extend({
  drawConfigId: DrawConfigID,
});

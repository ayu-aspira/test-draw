import { ApplicantSortOptions } from "@/s3/applicant";
import { DrawSortDirection, type DrawSortRuleInput } from "@aspira-nextgen/graphql/resolvers";
import { z } from "zod";
import { DynamoDBPrefix } from "#dynamo/dynamo.ts";
import { idStringPrefix, trimmedString } from "#validation/common.ts";

export const DrawSortID = idStringPrefix(DynamoDBPrefix.DRAW_SORT);

const sortRuleValidator = z
  .array(z.object({ field: z.string(), direction: z.nativeEnum(DrawSortDirection) }))
  .min(1)
  .refine((rules) => containsUniqueSortFields(rules), {
    message: "Sorting multiple times on the same field is not allowed.",
  })
  .refine((rules) => containsValidSortFields(rules), { message: "Invalid sort field." });

export const CreateDrawSortInputSchema = z.object({
  name: trimmedString(),
  rules: sortRuleValidator,
});

export const UpdateDrawSortInputSchema = z.object({
  id: DrawSortID,
  name: trimmedString().optional(),
  rules: sortRuleValidator.optional(),
});

const containsUniqueSortFields = (sortRules: DrawSortRuleInput[]): boolean =>
  new Set(sortRules.map((rule) => rule.field)).size === sortRules.length;

const containsValidSortFields = (sortRules: DrawSortRuleInput[]): boolean =>
  sortRules.length === 0 ||
  sortRules.some((rule) => ApplicantSortOptions.find((option) => option.field === rule.field));

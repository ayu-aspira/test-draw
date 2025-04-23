import { nullablePositiveNumber, positiveNumberWithDefault } from "@/s3/common-zod-validators";
import { readCSVFromS3 } from "@/s3/s3";
import { z } from "zod";

export type HuntCode = {
  readonly huntCode: string;

  /**
   * Whether this hunt code is valid.
   */
  readonly isValid: boolean;

  /**
   * Whether this hunt code is in the draw.
   */
  readonly isInDraw: boolean;

  /**
   * The total quota associated with this hunt code. If null, no
   * quota should be awarded for this hunt code.
   */
  readonly totalQuota: number | null;

  /**
   * The total quota associated with this hunt code in the current draw.
   * Null to indicate that the total quota for this draw is not set due to
   * inqualification.
   */
  totalQuotaInThisDraw: number | null;

  /**
   * A running total of the number of times this code
   * has been awarded.
   */
  totalQuotaAwarded: number;

  readonly previousQuotaBalance: number | null;

  /**
   * The overall quota balance for this hunt code.
   * Null to match a potential missing total quota.
   */
  quotaBalance: number | null;

  /**
   * The quota balance for this hunt code in the current draw.
   * Null to indicate that the quota balance for this draw is not set due to
   * inqualification.
   */
  quotaBalanceInThisDraw: number | null;

  /**
   * The number of times this code has been awarded in the draw
   * in which it was last used.
   */
  quotaAwardedInThisDraw: number;

  nonResidents: {
    /**
     * Whether this hunt code has a non-resident hard cap.
     */
    readonly hasHardCap: boolean;

    /**
     * If this hunt code has a non-resident cap, this represents the percentage of the total quota
     * that can be awarded to non-residents. It is represented as a number between 0 and 100.
     */
    readonly capPercent: number | null;

    /**
     * If this hunt code has a non-resident cap, this represents the maximum number of non-resident
     * quota that can be awarded.
     */
    totalQuota: number | null;

    quotaBalance: number | null;

    quotaAwardedInThisDraw: number;

    totalQuotaAwarded: number;
  };

  residents: {
    quotaAwardedInThisDraw: number;

    totalQuotaAwarded: number;
  };

  /**
   * A temporary quota allocation pool for demo purposes. Quota in this pool is only awarded
   * if the quota rule WP_RES_QUOTA is enabled.
   */
  wpRes: {
    readonly isAllocEnabled: boolean;

    readonly capPercent: number | null;

    totalQuota: number | null;

    quotaBalance: number | null;
  };
};

const HuntCodeCsvRowZodDefintion = z.object({
  hunt_code: z.string({ message: "hunt_code is required." }).min(1, { message: "hunt_code is required." }),
  is_valid: z.enum(["Y", "N"], { message: "is_valid must be either 'Y' or 'N'." }),
  in_the_draw: z.enum(["Y", "N"], { message: "in_the_draw must be either 'Y' or 'N'." }),
  total_quota: nullablePositiveNumber("total_quota"),
  total_quota_in_this_draw: nullablePositiveNumber("total_quota_in_this_draw"),
  total_quota_awarded: positiveNumberWithDefault("total_quota_awarded", 0),
  previous_quota_balance: nullablePositiveNumber("previous_quota_balance"),
  quota_balance: nullablePositiveNumber("quota_balance"),
  quota_balance_in_this_draw: nullablePositiveNumber("quota_balance_in_this_draw"),
  quota_awarded_in_this_draw: positiveNumberWithDefault("quota_awarded_in_this_draw", 0),
  nr_cap_chk: z.preprocess(
    (val) => (val ? val : undefined),
    z.enum(["Y", "N"], { message: "nr_cap_chk must be either 'Y' or 'N'." }).default("N"),
  ),
  // "prect" is not a typo. This is how it is spelled in the CORIS CSV.
  nrcap_prect: nullablePositiveNumber("nrcap_prect"),
  nrcap_amt: nullablePositiveNumber("nrcap_amt"),
  nrcap_bal: nullablePositiveNumber("nrcap_bal"),
  nr_quota_awarded_in_this_draw: positiveNumberWithDefault("nr_quota_awarded_in_this_draw", 0),
  nr_total_quota_awarded: positiveNumberWithDefault("nr_total_quota_awarded", 0),
  r_quota_awarded_in_this_draw: positiveNumberWithDefault("r_quota_awarded_in_this_draw", 0),
  r_total_quota_awarded: positiveNumberWithDefault("r_total_quota_awarded", 0),
  // The following are temporary fields for the WP_RES_QUOTA rule.
  // "prect" is not a typo. This is how it is spelled in the CORIS CSV.
  unLPP_prect: nullablePositiveNumber("unLPP_prect"),
  unLPP_amt: nullablePositiveNumber("unLPP_amt"),
  unLPP_bal: nullablePositiveNumber("unLPP_bal"),
});

export type HuntCodeCsvRow = z.infer<typeof HuntCodeCsvRowZodDefintion>;

export const validateHuntCodeRows = (rows: Record<string, string>[]): void => rows.forEach(validateHuntCodeRow);

const validateHuntCodeRow = (row: Record<string, string>): HuntCodeCsvRow => {
  const result = HuntCodeCsvRowZodDefintion.safeParse(row);

  if (!result.success) {
    const huntCode = row.hunt_code;

    const messagePrefix = huntCode ? `Record ${huntCode} has the following errors:` : "The following errors occurred:";

    const flattenedErrors = result.error.flatten();

    const errors = [...Object.values(flattenedErrors.fieldErrors).flat(), ...flattenedErrors.formErrors];

    throw new Error(`${messagePrefix}\n${errors.join("\n")}`);
  }

  return result.data;
};

export const readAllHuntCodes = async (s3Key: string): Promise<HuntCode[]> =>
  mapHuntCodeCsvRowsToHuntCodes(await readCSVFromS3(s3Key));

const isYes = (value: string): boolean => value === "Y";

/**
 * @private visible for testing
 */
export const mapHuntCodeCsvRowsToHuntCodes = (rows: Record<string, string>[]): HuntCode[] =>
  rows.map((row) => {
    const huntCodeCsvRow = validateHuntCodeRow(row);
    return {
      huntCode: huntCodeCsvRow.hunt_code.trim(),
      isValid: isYes(huntCodeCsvRow.is_valid),
      isInDraw: isYes(huntCodeCsvRow.in_the_draw),
      totalQuota: huntCodeCsvRow.total_quota,
      totalQuotaInThisDraw: huntCodeCsvRow.total_quota_in_this_draw,
      totalQuotaAwarded: huntCodeCsvRow.total_quota_awarded,
      previousQuotaBalance: huntCodeCsvRow.previous_quota_balance,
      quotaBalance: huntCodeCsvRow.quota_balance,
      quotaBalanceInThisDraw: huntCodeCsvRow.quota_balance_in_this_draw,
      quotaAwardedInThisDraw: huntCodeCsvRow.quota_awarded_in_this_draw,
      nonResidents: {
        hasHardCap: isYes(huntCodeCsvRow.nr_cap_chk) && huntCodeCsvRow.nrcap_prect !== null,
        capPercent: huntCodeCsvRow.nrcap_prect,
        totalQuota: huntCodeCsvRow.nrcap_amt,
        quotaBalance: huntCodeCsvRow.nrcap_bal,
        quotaAwardedInThisDraw: huntCodeCsvRow.nr_quota_awarded_in_this_draw,
        totalQuotaAwarded: huntCodeCsvRow.nr_total_quota_awarded,
      },
      residents: {
        quotaAwardedInThisDraw: huntCodeCsvRow.r_quota_awarded_in_this_draw,
        totalQuotaAwarded: huntCodeCsvRow.r_total_quota_awarded,
      },
      wpRes: {
        isAllocEnabled: huntCodeCsvRow.unLPP_prect !== null,
        capPercent: huntCodeCsvRow.unLPP_prect,
        totalQuota: huntCodeCsvRow.unLPP_amt,
        quotaBalance: huntCodeCsvRow.unLPP_bal,
      },
    };
  });

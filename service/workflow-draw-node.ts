import assert from "node:assert";
import { type Applicant, ApplicantDrawOutcome, ApplicantResidency, readAllApplicants } from "@/s3/applicant";
import { type HuntCode, readAllHuntCodes } from "@/s3/hunt-code";
import { getDrawDocumentBucketName } from "@/s3/s3";
import { getDrawConfigForWorkflowNode } from "@/service/draw-config";
import {
  DRAW_APPLICANT_DOMAIN_MODEL,
  DRAW_DRAW_DATA_DOMAIN_MODEL,
  DRAW_METRICS_DOMAIN_MODEL,
} from "@/service/draw-constants";
import { getDrawSort } from "@/service/draw-sort";
import { GraphQLTypename } from "@/service/graphql";
import { findWorkflowDataSource, logInfo, wrapWorkflowHandler } from "@/step-function/workflow-node-utils";
import {
  WorkflowMimeDataType,
  type WorkflowNodeContext,
  type WorkflowNodeHandler,
  type WorkflowNodePayload,
} from "@/step-function/workflow-types";
import { buildS3UriString, getS3KeyFromURI, parseUri } from "@/util/uri";
import {
  type DrawConfig,
  DrawConfigQuotaRule,
  type DrawConfigQuotaRules,
  type DrawSort,
  type DrawSortRuleInput,
  WorkflowDomain,
  type WorkflowFunctionalNodeDefinition,
  WorkflowNodeType,
} from "@aspira-nextgen/graphql/resolvers";
import { generateDrawMetrics } from "#service/workflow-draw-node-metrics.ts";
import { exportResults } from "#service/workflow-draw-node-result-export.ts";
import { sortApplicants } from "#service/workflow-draw-node-sort.ts";

export const WorkflowDrawNodeDefinition: WorkflowFunctionalNodeDefinition = {
  type: WorkflowNodeType.WorkflowDrawNode,
  inputs: [
    {
      domain: WorkflowDomain.Draw,
      domainModel: DRAW_APPLICANT_DOMAIN_MODEL,
      mimetypes: [WorkflowMimeDataType.CSV],
    },
    {
      domain: WorkflowDomain.Draw,
      domainModel: DRAW_DRAW_DATA_DOMAIN_MODEL,
      mimetypes: [WorkflowMimeDataType.CSV],
    },
  ],
  outputs: [
    {
      domain: WorkflowDomain.Draw,
      domainModel: DRAW_APPLICANT_DOMAIN_MODEL,
      mimetypes: [WorkflowMimeDataType.CSV],
    },
    {
      domain: WorkflowDomain.Draw,
      domainModel: DRAW_DRAW_DATA_DOMAIN_MODEL,
      mimetypes: [WorkflowMimeDataType.CSV],
    },
  ],
  __typename: GraphQLTypename.WORKFLOW_FUNCTIONAL_NODE_DEFINITION,
};

// TODO: this should either come from a configuration or be inferred from the document itself.
// Until we get that configuration path built out, we'll just hardcode it.
export const DRAW_RUN_NUM_CHOICES = 4;

/**
 * Represents a hunt-code that has passed through the sanitization process. This type should
 * be leveraged when processing the draw to ensure that the data is in a consistent state.
 */
type SanitizedHuntCode = Omit<
  HuntCode,
  "totalQuota" | "totalQuotaInThisDraw" | "quotaBalance" | "quotaBalanceInThisDraw" | "isInDraw" | "isValid"
> & {
  readonly totalQuota: number;

  readonly totalQuotaInThisDraw: number;

  readonly previousQuotaBalance: number;

  quotaBalance: number;

  quotaBalanceInThisDraw: number;
};

type DrawBucket = {
  /**
   * The hunt-code context for this bucket.
   */
  readonly huntCode: SanitizedHuntCode;

  /**
   * The applicant entities read from DynamoDB. These are not mutated.
   */
  readonly applicants: Applicant[];

  /**
   * The choice ordinal for this bucket. Primarily used to associate
   * the choice with the applicants optimally.
   */
  readonly choiceOrdinal: number;
};

type DrawBucketResult = {
  readonly quotaAwarded: {
    /**
     * The total amount of quota awarded to residents.
     */
    readonly resident: number;

    /**
     * The total amount of quota awarded to non-residents.
     */
    readonly nonResident: number;

    /**
     * The total amount of quota awarded by this bucket.
     */
    readonly total: number;
  };

  readonly applicantResults: Applicant[];
};

const findApplicantS3Source = (payload: WorkflowNodePayload): string => {
  const applicantSource = findWorkflowDataSource({
    payload,
    domain: WorkflowDomain.Draw,
    domainModel: DRAW_APPLICANT_DOMAIN_MODEL,
  });

  if (!applicantSource) {
    throw new Error("Draw applicant data not provided.");
  }

  return getS3KeyFromURI(parseUri(applicantSource.uri));
};

const findDrawDataS3Source = (payload: WorkflowNodePayload): string => {
  const drawSource = findWorkflowDataSource({
    payload,
    domain: WorkflowDomain.Draw,
    domainModel: DRAW_DRAW_DATA_DOMAIN_MODEL,
  });

  if (!drawSource) {
    throw new Error("Hunt code data source not provided.");
  }

  return getS3KeyFromURI(parseUri(drawSource.uri));
};

/**
 * The responsibility of this function is to handle gathering the necessary data
 * to process the draw and orchestrate the i/o process within the context of a
 * workflow node.
 */
const workflowNodeFn: WorkflowNodeHandler = async (event) => {
  const { context } = event;

  const drawConfig = await getDrawConfigForWorkflowNode({
    organizationId: context.organizationId,
    workflowNodeId: context.nodeId,
  });

  if (!drawConfig) {
    throw new Error(`Draw configuration not found for workflow node ${context.nodeId}.`);
  }

  if (!drawConfig.sortId) {
    throw new Error(`Draw configuration ${drawConfig.id} does not have a sort rule defined.`);
  }

  const drawSort = await getDrawSort({
    id: drawConfig.sortId,
    organizationId: context.organizationId,
  });

  if (!drawSort) {
    throw new Error(`Draw sort rule ${drawConfig.sortId} not found.`);
  }

  if (drawSort.rules.length < 1) {
    throw new Error(
      `Draw sort rule ${drawSort.id} does not have any sort rules defined. At least one sort rule is required.`,
    );
  }

  const [applicants, huntCodes] = await Promise.all([
    readAllApplicants(findApplicantS3Source(event)),
    readAllHuntCodes(findDrawDataS3Source(event)),
  ]);

  const { huntCodeResults, applicantResults, huntCodesUsedInDraw } = await processDraw({
    context,
    drawSort,
    applicants,
    drawConfig,
    huntCodes,
  });

  const drawMetricsResults = generateDrawMetrics({
    huntCodesUsedInDraw,
    huntCodeResults,
  });

  const { huntCodeDrawResultDocument, applicantDrawResultDocument, drawMetricsResultDocument } = await exportResults({
    context,
    applicantResults,
    huntCodeResults,
    drawNumChoices: DRAW_RUN_NUM_CHOICES,
    drawMetricsResults,
  });

  return [
    {
      uri: buildS3UriString(getDrawDocumentBucketName(), applicantDrawResultDocument.s3Key),
      mime: {
        type: WorkflowMimeDataType.CSV,
        domain: WorkflowDomain.Draw,
        domainModel: DRAW_APPLICANT_DOMAIN_MODEL,
      },
    },
    {
      uri: buildS3UriString(getDrawDocumentBucketName(), huntCodeDrawResultDocument.s3Key),
      mime: {
        type: WorkflowMimeDataType.CSV,
        domain: WorkflowDomain.Draw,
        domainModel: DRAW_DRAW_DATA_DOMAIN_MODEL,
      },
    },
    {
      uri: buildS3UriString(getDrawDocumentBucketName(), drawMetricsResultDocument.s3Key),
      mime: {
        type: WorkflowMimeDataType.CSV,
        domain: WorkflowDomain.Draw,
        domainModel: DRAW_METRICS_DOMAIN_MODEL,
      },
    },
  ];
};

export const workflowNodeHandler = wrapWorkflowHandler(workflowNodeFn);

/**
 * Handles processing the draw for the given applicants, hunt-codes, and draw-configuration rules.
 *
 * Steps:
 * 1. Adjust data based on configuration rules.
 *    - For hunt-codes:
 *      - Adjusting total quota based on quota remaining.
 *    - For applicants:
 *      - Filter out applicants that have already been awarded.
 *      - Cleaning previous draw outcomes.
 * 2. Sort applicants based on the sort rules.
 * 3. Process each bucket by applying the sort rules and awarding applicants based on the take logic.
 *    - Currently, we are using a simple "take off the top" logic.
 * 4. Collect and return results
 */
export const processDraw = async (input: {
  context: WorkflowNodeContext;
  huntCodes: HuntCode[];
  applicants: Applicant[];
  drawSort: DrawSort;
  drawConfig: DrawConfig;
}): Promise<{ huntCodeResults: HuntCode[]; huntCodesUsedInDraw: Set<string>; applicantResults: Applicant[] }> => {
  const { huntCodes: dirtyHuntCodes, applicants: dirtyApplicants, drawSort, drawConfig, context } = input;
  const { rules: sortRules } = drawSort;

  logInfo(
    context,
    `Filtering and resetting data for draw ${context.nodeId} with ${dirtyApplicants.length} applicants and ${dirtyHuntCodes.length} hunt codes.`,
  );

  const huntCodes = sanitizeHuntCodes(dirtyHuntCodes, drawConfig).map((huntCode) =>
    calculateHuntCodeQuotas(huntCode, drawConfig),
  );
  const applicants = filterApplicants(dirtyApplicants).map(resetApplicantResultFields);

  logInfo(
    context,
    `Processing draw ${context.nodeId} with ${applicants.length} filtered applicants and ${huntCodes.length} filtered hunt codes.`,
  );

  const sanitizedHuntCodesByHuntCode = buildSanitizedHuntCodeByHuntCode(huntCodes);

  // Used to ignore applicants that have been awarded previously
  // when processing subsequent choice ordinals.
  const awardedApplicantApplicationNumbers = new Set<number>();

  const applicantDrawResults: Applicant[] = [];

  for (let choiceOrdinal = 1; choiceOrdinal <= DRAW_RUN_NUM_CHOICES; choiceOrdinal++) {
    const buckets = bucketApplicantsByHuntCode(sanitizedHuntCodesByHuntCode, applicants, choiceOrdinal);

    const results = await Promise.all(
      buckets.map((bucket) => processBucket(bucket, sortRules, drawConfig, awardedApplicantApplicationNumbers)),
    );

    results.forEach(({ applicantResults, quotaAwarded }, index) => {
      const huntCode = buckets[index].huntCode;

      applicantResults.forEach((applicantResult) => {
        if (applicantResult.drawOutcome === ApplicantDrawOutcome.Awarded) {
          awardedApplicantApplicationNumbers.add(applicantResult.applicationNumber);
        }

        applicantDrawResults.push(applicantResult);
      });

      huntCode.nonResidents.quotaAwardedInThisDraw += quotaAwarded.nonResident;
      huntCode.nonResidents.totalQuotaAwarded += quotaAwarded.nonResident;
      if (
        isQuotaRuleEnabled(drawConfig.quotaRules, DrawConfigQuotaRule.NonResidentCapEnforcement) &&
        huntCode.nonResidents.quotaBalance !== null
      ) {
        huntCode.nonResidents.quotaBalance -= quotaAwarded.nonResident;
      }

      huntCode.residents.quotaAwardedInThisDraw += quotaAwarded.resident;
      huntCode.residents.totalQuotaAwarded += quotaAwarded.resident;

      if (
        isQuotaRuleEnabled(drawConfig.quotaRules, DrawConfigQuotaRule.WpResQuota) &&
        huntCode.wpRes.quotaBalance !== null
      ) {
        huntCode.wpRes.quotaBalance -= quotaAwarded.resident;
      }

      huntCode.totalQuotaAwarded += quotaAwarded.total;
      huntCode.quotaAwardedInThisDraw += quotaAwarded.total;
      huntCode.quotaBalance -= quotaAwarded.total;
      huntCode.quotaBalanceInThisDraw -= quotaAwarded.total;
    });

    logInfo(context, `Processed ${buckets.length} buckets for choice ordinal ${choiceOrdinal}.`);
  }

  // Action all applicants that were not awarded to "Not Awarded".
  applicants
    .filter((applicant) => !awardedApplicantApplicationNumbers.has(applicant.applicationNumber))
    .forEach((applicant) => {
      applicantDrawResults.push({
        ...applicant,
        drawOutcome: ApplicantDrawOutcome.NotAwarded,
        choiceOrdinalAwarded: null,
        choiceAwarded: null,
      });
    });

  // Sort the applicant results by the same sort rules that were used to process the draw.
  await sortApplicants(applicantDrawResults, sortRules);

  flowQuotaIfApplicable(drawConfig, huntCodes);

  const huntCodeResults = mergeHuntCodes(dirtyHuntCodes, huntCodes);
  huntCodeResults.sort((a, b) => (a.huntCode < b.huntCode ? -1 : 1));

  return {
    huntCodeResults,
    huntCodesUsedInDraw: new Set(huntCodes.map((huntCode) => huntCode.huntCode)),
    applicantResults: applicantDrawResults,
  };
};

type QuotaBalance = {
  total: number;
  nonResident: number | null;
  wpRes: number | null;
};

type QuotaAwards = {
  resident: number;
  nonResident: number;
  wpRes: number;
};

const processBucket = async (
  bucket: DrawBucket,
  sortRules: DrawSortRuleInput[],
  drawConfig: DrawConfig,
  awardedApplicantApplicationNumbers: Set<number>,
): Promise<DrawBucketResult> => {
  const applicantResults: Applicant[] = [];

  const { huntCode } = bucket;

  if (huntCode.quotaBalance === 0) {
    return emptyDrawBucketResult();
  }

  let quotaAwards: QuotaAwards = {
    resident: 0,
    nonResident: 0,
    wpRes: 0,
  };

  let quotaBalance: QuotaBalance = {
    total: huntCode.quotaBalanceInThisDraw,
    nonResident: huntCode.nonResidents.quotaBalance,
    wpRes: huntCode.wpRes.quotaBalance,
  };

  // Sorted in-place.
  await sortApplicants(bucket.applicants, sortRules);

  for (const applicant of bucket.applicants) {
    if (quotaBalance.total === 0) {
      break;
    }

    if (
      !shouldAwardApplicant({
        applicant,
        huntCode,
        quotaBalance,
        drawConfig,
        awardedApplicantApplicationNumbers,
      })
    ) {
      continue;
    }

    applicantResults.push({
      ...applicant,
      drawOutcome: ApplicantDrawOutcome.Awarded,
      choiceOrdinalAwarded: bucket.choiceOrdinal,
      choiceAwarded: huntCode.huntCode,
    });

    const updatedBucketQuotas = updateBucketQuotas(applicant, quotaAwards, quotaBalance, drawConfig);

    quotaAwards = updatedBucketQuotas.awards;
    quotaBalance = updatedBucketQuotas.balance;
  }

  return {
    quotaAwarded: {
      ...quotaAwards,
      total: quotaAwards.resident + quotaAwards.nonResident,
    },
    applicantResults,
  };
};

const emptyDrawBucketResult = (): DrawBucketResult => ({
  quotaAwarded: {
    resident: 0,
    nonResident: 0,
    total: 0,
  },
  applicantResults: [],
});

const shouldAwardApplicant = (input: {
  applicant: Applicant;
  huntCode: SanitizedHuntCode;
  quotaBalance: QuotaBalance;
  drawConfig: DrawConfig;
  awardedApplicantApplicationNumbers: Set<number>;
}): boolean => {
  const { applicant, huntCode, quotaBalance, drawConfig, awardedApplicantApplicationNumbers } = input;

  if (awardedApplicantApplicationNumbers.has(applicant.applicationNumber) || quotaBalance.total === 0) {
    return false;
  }

  const quotaRules = drawConfig.quotaRules;

  if (isQuotaRuleEnabled(quotaRules, DrawConfigQuotaRule.WpResQuota)) {
    return isWpAllocCheckPassed(quotaBalance, applicant);
  }

  if (isQuotaRuleEnabled(quotaRules, DrawConfigQuotaRule.NonResidentCapEnforcement)) {
    return isNonResidentCapCheckPassed(huntCode, quotaBalance, applicant);
  }

  return true;
};

const isQuotaRuleEnabled = (quotaRules: DrawConfigQuotaRules, rule: DrawConfigQuotaRule): boolean =>
  quotaRules.ruleFlags.includes(rule);

const isNonResidentCapCheckPassed = (
  huntCode: SanitizedHuntCode,
  quotaBalance: QuotaBalance,
  applicant: Applicant,
): boolean =>
  // If the applicant is a resident, then there is nothing to check.
  isApplicantResident(applicant) ||
  // If the hunt-code doesn't have hard cap, then there is nothing to check.
  !huntCode.nonResidents.hasHardCap ||
  // If the hunt-code has hard cap, then we need to check if the quota balance is not zero.
  (quotaBalance.nonResident !== null && quotaBalance.nonResident !== 0);

const isWpAllocCheckPassed = (quotaBalance: QuotaBalance, applicant: Applicant): boolean =>
  // The applicant must be a resident.
  isApplicantResident(applicant) &&
  // There must be a wpRes allocation to give.
  quotaBalance.wpRes !== null &&
  quotaBalance.wpRes !== 0;

const updateBucketQuotas = (
  applicant: Applicant,
  awards: QuotaAwards,
  balance: QuotaBalance,
  drawConfig: DrawConfig,
): { awards: QuotaAwards; balance: QuotaBalance } => {
  const { residency } = applicant;

  const updatedAwards = { ...awards };
  const updatedBalance = { ...balance };

  if (isApplicantNonresident(applicant)) {
    updatedAwards.nonResident++;

    if (updatedBalance.nonResident !== null) {
      updatedBalance.nonResident--;
    }
  } else if (isApplicantResident(applicant)) {
    updatedAwards.resident++;

    if (isQuotaRuleEnabled(drawConfig.quotaRules, DrawConfigQuotaRule.WpResQuota) && updatedBalance.wpRes !== null) {
      updatedAwards.wpRes++;
      updatedBalance.wpRes--;
    }
  } else {
    throw new Error(`Unhandled residency type ${residency}.`);
  }

  updatedBalance.total--;

  return {
    awards: updatedAwards,
    balance: updatedBalance,
  };
};

const isApplicantResident = (applicant: Applicant): boolean => applicant.residency === ApplicantResidency.Resident;
const isApplicantNonresident = (applicant: Applicant): boolean =>
  applicant.residency === ApplicantResidency.NonResident;

const buildSanitizedHuntCodeByHuntCode = (huntCodes: SanitizedHuntCode[]): Record<string, SanitizedHuntCode> => {
  return huntCodes.reduce(
    (acc, huntCode) => {
      acc[huntCode.huntCode] = huntCode;
      return acc;
    },
    {} as Record<string, SanitizedHuntCode>,
  );
};

const isHuntCodeValid = (huntCode: HuntCode): boolean =>
  huntCode.isValid && huntCode.isInDraw && huntCode.totalQuota !== null;

const sanitizeHuntCodes = (huntCodes: HuntCode[], drawConfig: DrawConfig): SanitizedHuntCode[] => {
  let filteredHuntCodes = huntCodes.filter((huntCode) => isHuntCodeValid(huntCode));

  if (isQuotaRuleEnabled(drawConfig.quotaRules, DrawConfigQuotaRule.WpResQuota)) {
    filteredHuntCodes = filteredHuntCodes.filter((huntCode) => huntCode.wpRes.isAllocEnabled);
  }

  return filteredHuntCodes.map((huntCode) => {
    const quotaBalance = huntCode.quotaBalance ?? huntCode.totalQuota!;

    return {
      ...huntCode,
      totalQuota: huntCode.totalQuota!,
      totalQuotaInThisDraw: quotaBalance,
      previousQuotaBalance: quotaBalance,
      quotaBalance,
      quotaBalanceInThisDraw: quotaBalance,
      quotaAwardedInThisDraw: 0,
      nonResidents: {
        ...huntCode.nonResidents,
        totalQuota: null,
        quotaBalance: null,
        quotaAwardedInThisDraw: 0,
      },
      residents: {
        ...huntCode.residents,
        quotaAwardedInThisDraw: 0,
      },
      // ** Temporary quota allocation **
      // These are recalculated within each draw-node if the WP_RES_QUOTA setting is enabled, thus we reset the values
      // to null and 0.
      wpRes: {
        ...huntCode.wpRes,
        totalQuota: null,
        quotaBalance: null,
      },
    };
  });
};

const calculateHuntCodeQuotas = (huntCode: SanitizedHuntCode, drawConfig: DrawConfig): SanitizedHuntCode => {
  let totalQuotaInThisDraw = huntCode.totalQuotaInThisDraw;

  const nonResidents = { ...huntCode.nonResidents };
  if (
    isQuotaRuleEnabled(drawConfig.quotaRules, DrawConfigQuotaRule.NonResidentCapEnforcement) &&
    nonResidents.hasHardCap
  ) {
    assert(nonResidents.capPercent !== null, `nonResidents.capPercent is null for ${huntCode.huntCode}.`);

    nonResidents.totalQuota = Math.floor((nonResidents.capPercent! * huntCode.quotaBalance) / 100);
    nonResidents.quotaBalance = nonResidents.totalQuota;
  }

  // ** Temporary quota allocation **
  // It is recalculated within each draw-node if the setting is enabled.
  const wpRes = { ...huntCode.wpRes };
  if (isQuotaRuleEnabled(drawConfig.quotaRules, DrawConfigQuotaRule.WpResQuota) && wpRes.isAllocEnabled) {
    assert(wpRes.capPercent !== null, `wpRes.capPercent is null for ${huntCode.huntCode}.`);

    wpRes.totalQuota = Math.floor((wpRes.capPercent! * huntCode.quotaBalance) / 100);
    wpRes.quotaBalance = wpRes.totalQuota;

    totalQuotaInThisDraw = wpRes.totalQuota;
  }

  return {
    ...huntCode,
    nonResidents,
    wpRes,
    totalQuotaInThisDraw,
    quotaBalanceInThisDraw: totalQuotaInThisDraw,
  };
};

const flowQuotaIfApplicable = (drawConfig: DrawConfig, huntCodes: SanitizedHuntCode[]): void => {
  if (
    // Only matters if the WP quota rule is enabled.
    !isQuotaRuleEnabled(drawConfig.quotaRules, DrawConfigQuotaRule.WpResQuota) ||
    // If flow quota is enabled, then the quota balance is already in a good state.
    isQuotaRuleEnabled(drawConfig.quotaRules, DrawConfigQuotaRule.FlowQuota)
  ) {
    return;
  }

  huntCodes.forEach((huntCode) => {
    if (!huntCode.wpRes.isAllocEnabled) {
      return;
    }

    // Remove the remaining allocation from the overall quota balance.
    assert(
      huntCode.wpRes.quotaBalance === huntCode.quotaBalanceInThisDraw,
      "wpRes.quotaBalance & huntCode.quotaBalanceInThisDraw is not in sync.",
    );
    huntCode.quotaBalance -= huntCode.wpRes.quotaBalance ?? 0;
    huntCode.quotaBalanceInThisDraw = 0;
  });
};

const mergeHuntCodes = (dirtyHuntCodes: HuntCode[], sanitizedHuntCodes: SanitizedHuntCode[]): HuntCode[] => {
  const sanitizedHuntCodeByHuntCode = buildSanitizedHuntCodeByHuntCode(sanitizedHuntCodes);

  return dirtyHuntCodes.map((dirtyHuntCode) => {
    const sanitizedHuntCode = sanitizedHuntCodeByHuntCode[dirtyHuntCode.huntCode];

    if (!sanitizedHuntCode) {
      return dirtyHuntCode;
    }

    return {
      ...dirtyHuntCode,
      ...sanitizedHuntCode,
    };
  });
};

const filterApplicants = (applicants: Applicant[]): Applicant[] =>
  applicants.filter((applicant) => applicant.drawOutcome !== ApplicantDrawOutcome.Awarded);

const resetApplicantResultFields = (applicant: Applicant): Applicant => ({
  ...applicant,
  drawOutcome: null,
  choiceOrdinalAwarded: null,
  choiceAwarded: null,
});

/**
 * @private Visible for testing
 */
export const bucketApplicantsByHuntCode = (
  huntCodeEntitiesByHuntCode: Record<string, SanitizedHuntCode>,
  applicants: Applicant[],
  choiceOrdinal: number,
): DrawBucket[] => {
  const buckets: Record<string, DrawBucket> = {};

  applicants.forEach((applicant) => {
    const choice = applicant.choices[choiceOrdinal - 1];

    if (!choice) {
      return;
    }

    const huntCode = huntCodeEntitiesByHuntCode[choice];

    if (!huntCode) {
      return;
    }

    const bucket = buckets[huntCode.huntCode];

    if (!bucket) {
      buckets[huntCode.huntCode] = {
        huntCode,
        applicants: [applicant],
        choiceOrdinal,
      };

      return;
    }

    bucket.applicants.push(applicant);
  });

  return Object.values(buckets);
};

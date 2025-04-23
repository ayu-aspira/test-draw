import type { HuntCode } from "#s3/hunt-code.ts";

export type DrawMetrics = {
  metric: string;
  value: number;
};

export enum MetricTitle {
  // The following are aggregates for the current draw only.
  TotalQuotaInThisDraw = "Total Quota In This Draw",
  TotalQuotaAwardedInThisDraw = "Total Quota Awarded In This Draw",
  TotalQuotaBalanceForNextDraw = "Total Quota Balance For Next Draw",

  TotalNonResidentsAwardedInThisDraw = "Total Non-Residents Awarded In This Draw",
  TotalPercentageNonResidentsAwardedInThisDraw = "Total Percentage Non-Residents Awarded In This Draw",

  TotalResidentsAwardedInThisDraw = "Total Residents Awarded In This Draw",
  TotalPercentageResidentsAwardedInThisDraw = "Total Percentage Residents Awarded In This Draw",

  // The following are aggregates across all draws + hunt-codes regardless of the current draw.
  TotalQuota = "Total Quota",
  TotalQuotaAwarded = "Total Quota Awarded",
  TotalQuotaBalance = "Total Quota Balance",

  TotalNonResidentsAwarded = "Total Non-Residents Awarded",
  TotalPercentageNonResidentsAwarded = "Total Percentage Non-Residents Awarded",

  TotalResidentsAwarded = "Total Residents Awarded",
  TotalPercentageResidentsAwarded = "Total Percentage Residents Awarded",
}

const percent = (part: number, total: number) => (total === 0 ? 0 : Number(((100 * part) / total).toFixed(2)));

const createMetric = (metric: string, value: number) => ({ metric, value });

const isValidHuntCode = (huntCode: HuntCode) => huntCode.isInDraw && huntCode.isValid;

const aggregateHuntCodesInDraw = (aggregates: Record<keyof typeof MetricTitle, number>, huntCodes: HuntCode[]) => {
  for (const hc of huntCodes) {
    if (!isValidHuntCode(hc)) {
      continue;
    }

    const totalQuotaInThisDraw = hc.totalQuotaInThisDraw ?? 0;
    const totalQuotaBalanceInThisDraw = hc.quotaBalanceInThisDraw ?? 0;
    const totalNonResidentsAwardedInThisDraw = hc.nonResidents?.quotaAwardedInThisDraw ?? 0;
    const totalResidentsAwardedInThisDraw = hc.residents?.quotaAwardedInThisDraw ?? 0;
    const totalQuotaAwardedInThisDraw = hc.quotaAwardedInThisDraw ?? 0;

    aggregates.TotalQuotaInThisDraw += totalQuotaInThisDraw;
    aggregates.TotalQuotaBalanceForNextDraw += totalQuotaBalanceInThisDraw;
    aggregates.TotalNonResidentsAwardedInThisDraw += totalNonResidentsAwardedInThisDraw;
    aggregates.TotalResidentsAwardedInThisDraw += totalResidentsAwardedInThisDraw;
    aggregates.TotalQuotaAwardedInThisDraw += totalQuotaAwardedInThisDraw;
  }
};

const aggregateAllHuntCodes = (aggregates: Record<keyof typeof MetricTitle, number>, huntCodes: HuntCode[]) => {
  for (const hc of huntCodes) {
    if (!isValidHuntCode(hc)) {
      continue;
    }

    const totalNonResidentsAwarded = hc.nonResidents?.totalQuotaAwarded ?? 0;
    const totalResidentsAwarded = hc.residents?.totalQuotaAwarded ?? 0;
    const totalQuotaAwarded = hc.totalQuotaAwarded ?? 0;
    const totalQuota = hc.totalQuota ?? 0;
    const totalQuotaBalance = hc.quotaBalance ?? totalQuota;

    aggregates.TotalNonResidentsAwarded += totalNonResidentsAwarded;
    aggregates.TotalResidentsAwarded += totalResidentsAwarded;
    aggregates.TotalQuotaAwarded += totalQuotaAwarded;
    aggregates.TotalQuota += totalQuota;
    aggregates.TotalQuotaBalance += totalQuotaBalance;
  }
};

export const generateDrawMetrics = (input: {
  huntCodeResults: HuntCode[];
  huntCodesUsedInDraw: Set<string>;
}): DrawMetrics[] => {
  const { huntCodeResults, huntCodesUsedInDraw } = input;

  const aggregates: Record<keyof typeof MetricTitle, number> = {
    TotalQuota: 0,
    TotalQuotaInThisDraw: 0,

    TotalQuotaAwarded: 0,
    TotalQuotaAwardedInThisDraw: 0,

    TotalQuotaBalance: 0,
    TotalQuotaBalanceForNextDraw: 0,

    TotalNonResidentsAwarded: 0,
    TotalNonResidentsAwardedInThisDraw: 0,

    TotalPercentageNonResidentsAwarded: 0,
    TotalPercentageNonResidentsAwardedInThisDraw: 0,

    TotalResidentsAwarded: 0,
    TotalResidentsAwardedInThisDraw: 0,

    TotalPercentageResidentsAwarded: 0,
    TotalPercentageResidentsAwardedInThisDraw: 0,
  };

  aggregateHuntCodesInDraw(
    aggregates,
    huntCodeResults.filter((hc) => huntCodesUsedInDraw.has(hc.huntCode)),
  );
  aggregateAllHuntCodes(aggregates, huntCodeResults);

  aggregates.TotalPercentageNonResidentsAwarded = percent(
    aggregates.TotalNonResidentsAwarded,
    aggregates.TotalQuotaAwarded,
  );
  aggregates.TotalPercentageNonResidentsAwardedInThisDraw = percent(
    aggregates.TotalNonResidentsAwardedInThisDraw,
    aggregates.TotalQuotaAwardedInThisDraw,
  );
  aggregates.TotalPercentageResidentsAwarded = percent(aggregates.TotalResidentsAwarded, aggregates.TotalQuotaAwarded);
  aggregates.TotalPercentageResidentsAwardedInThisDraw = percent(
    aggregates.TotalResidentsAwardedInThisDraw,
    aggregates.TotalQuotaAwardedInThisDraw,
  );

  return Object.keys(aggregates).map((key) => {
    const metricTitle = MetricTitle[key as keyof typeof MetricTitle];
    return createMetric(metricTitle, aggregates[key as keyof typeof aggregates]);
  });
};

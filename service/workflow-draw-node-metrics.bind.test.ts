import { beforeAll, describe, expect, it } from "vitest";
import { type HuntCode, mapHuntCodeCsvRowsToHuntCodes } from "#s3/hunt-code.ts";
import { MetricTitle, generateDrawMetrics } from "#service/workflow-draw-node-metrics.ts";
import { buildTestDrawFixturePath, readAndParseTestDrawCSVFixture } from "#util/test-utils.ts";

describe("generateDrawMetrics", () => {
  const expectedMetrics = expect.arrayContaining([
    {
      metric: MetricTitle.TotalNonResidentsAwarded,
      value: 3,
    },
    {
      metric: MetricTitle.TotalNonResidentsAwardedInThisDraw,
      value: 3,
    },
    {
      metric: MetricTitle.TotalPercentageNonResidentsAwarded,
      value: 33.33,
    },
    {
      metric: MetricTitle.TotalPercentageNonResidentsAwardedInThisDraw,
      value: 33.33,
    },
    {
      metric: MetricTitle.TotalResidentsAwarded,
      value: 6,
    },
    {
      metric: MetricTitle.TotalResidentsAwardedInThisDraw,
      value: 6,
    },
    {
      metric: MetricTitle.TotalPercentageResidentsAwarded,
      value: 66.67,
    },
    {
      metric: MetricTitle.TotalPercentageResidentsAwardedInThisDraw,
      value: 66.67,
    },
    {
      metric: MetricTitle.TotalQuotaAwarded,
      value: 9,
    },
    {
      metric: MetricTitle.TotalQuotaAwardedInThisDraw,
      value: 9,
    },
    {
      metric: MetricTitle.TotalQuota,
      value: 20,
    },
  ]);

  let huntCodeResults: HuntCode[];
  let huntCodesUsedInDraw: Set<string>;

  beforeAll(async () => {
    const huntCodesPath = buildTestDrawFixturePath({
      testName: "draw-nr-cap-scenario",
      filename: "draw-data-output.csv",
    });

    huntCodeResults = await readAndParseTestDrawCSVFixture<HuntCode>(huntCodesPath, mapHuntCodeCsvRowsToHuntCodes);
    huntCodesUsedInDraw = new Set(huntCodeResults.map((hc) => hc.huntCode));
  });

  it("should generate metrics", () => {
    const drawMetricsResults = generateDrawMetrics({ huntCodeResults, huntCodesUsedInDraw });

    expect(drawMetricsResults).toHaveLength(Object.keys(MetricTitle).length);
    expect(drawMetricsResults).toEqual(expectedMetrics);
  });

  it("should filter invalid draw metrics", () => {
    huntCodeResults.push({
      huntCode: "huntCode3",
      isValid: false,
      isInDraw: true,
      totalQuota: 10,
      totalQuotaInThisDraw: 10,
      totalQuotaAwarded: 4,
      previousQuotaBalance: 10,
      quotaBalance: 6,
      quotaBalanceInThisDraw: 6,
      quotaAwardedInThisDraw: 4,
      nonResidents: {
        hasHardCap: false,
        capPercent: null,
        totalQuota: null,
        quotaBalance: null,
        quotaAwardedInThisDraw: 1,
        totalQuotaAwarded: 1,
      },
      residents: {
        quotaAwardedInThisDraw: 3,
        totalQuotaAwarded: 3,
      },
      wpRes: {
        isAllocEnabled: false,
        capPercent: null,
        totalQuota: null,
        quotaBalance: null,
      },
    });

    huntCodesUsedInDraw.add("huntCode3");

    const drawMetricsResults = generateDrawMetrics({ huntCodeResults, huntCodesUsedInDraw });

    expect(drawMetricsResults).toEqual(expectedMetrics);
  });

  it("should filter draw metrics not in draw", () => {
    huntCodeResults.push({
      huntCode: "huntCode3",
      isValid: true,
      isInDraw: false,
      totalQuota: 10,
      totalQuotaInThisDraw: 10,
      totalQuotaAwarded: 4,
      previousQuotaBalance: 10,
      quotaBalance: 6,
      quotaBalanceInThisDraw: 6,
      quotaAwardedInThisDraw: 4,
      nonResidents: {
        hasHardCap: false,
        capPercent: null,
        totalQuota: null,
        quotaBalance: null,
        quotaAwardedInThisDraw: 1,
        totalQuotaAwarded: 1,
      },
      residents: {
        quotaAwardedInThisDraw: 3,
        totalQuotaAwarded: 3,
      },
      wpRes: {
        isAllocEnabled: false,
        capPercent: null,
        totalQuota: null,
        quotaBalance: null,
      },
    });

    const drawMetricsResults = generateDrawMetrics({ huntCodeResults, huntCodesUsedInDraw });

    expect(drawMetricsResults).toEqual(expectedMetrics);
  });
});

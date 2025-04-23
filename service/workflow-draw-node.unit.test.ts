import { type Applicant, ApplicantResidency, mapApplicantCsvRowsToApplicants } from "@/s3/applicant";
import { type HuntCode, mapHuntCodeCsvRowsToHuntCodes } from "@/s3/hunt-code";
import { bucketApplicantsByHuntCode, processDraw } from "@/service/workflow-draw-node";
import {
  type TestDrawInputFixtures,
  buildTestDrawFixturePath,
  readAndParseTestDrawCSVFixture,
  readAndParseTestDrawJSONFixture,
} from "@/util/test-utils";
import type { DrawConfig, DrawSortRule } from "@aspira-nextgen/graphql/resolvers";
import { describe, expect, it, vi } from "vitest";

vi.mock("@aspira-nextgen/core/dynamodb", () => {
  return {
    createDynamoDBClient: vi.fn(),
  };
});

vi.mock("@aspira-nextgen/core/logger", () => ({
  buildTracer: vi.fn(),
  buildLogger: vi.fn().mockImplementation(() => ({
    info: () => {},
  })),
}));

const runDrawUnitTest = async (testName: string) => {
  const testInputFixtures: TestDrawInputFixtures = {
    drawDataInputPath: buildTestDrawFixturePath({ testName, filename: "draw-data-input.csv" }),
    applicantInputPath: buildTestDrawFixturePath({ testName, filename: "applicant-input.csv" }),
    sortRulesPath: buildTestDrawFixturePath({ testName, filename: "sort-rules.json" }),
    drawConfigPath: buildTestDrawFixturePath({ testName, filename: "draw-config.json" }),
  };

  const [drawData, applicants, sortRules, drawConfig] = await Promise.all([
    readAndParseTestDrawCSVFixture<HuntCode>(testInputFixtures.drawDataInputPath, mapHuntCodeCsvRowsToHuntCodes),
    readAndParseTestDrawCSVFixture<Applicant>(testInputFixtures.applicantInputPath, mapApplicantCsvRowsToApplicants),
    readAndParseTestDrawJSONFixture<DrawSortRule[]>(testInputFixtures.sortRulesPath),
    readAndParseTestDrawJSONFixture<DrawConfig>(testInputFixtures.drawConfigPath),
  ]);

  const drawResult = await processDraw({
    context: {
      workflowJobId: "job-id",
      nodeId: "node-id",
      organizationId: "org-id",
    },
    drawSort: {
      id: "sort-id",
      name: "sort-name",
      rules: sortRules,
    },
    drawConfig,
    huntCodes: drawData,
    applicants: applicants,
  });

  const [expectedDrawDataResults, expectedApplicantResults] = await Promise.all([
    readAndParseTestDrawCSVFixture<HuntCode>(
      buildTestDrawFixturePath({ testName, filename: "draw-data-output.csv" }),
      mapHuntCodeCsvRowsToHuntCodes,
    ),
    readAndParseTestDrawCSVFixture<Applicant>(
      buildTestDrawFixturePath({ testName, filename: "applicant-output.csv" }),
      mapApplicantCsvRowsToApplicants,
    ),
  ]);

  expectedApplicantResults.forEach((expected) => {
    const actual = drawResult.applicantResults.find(
      (r) => r.applicationNumber === expected.applicationNumber,
    ) as Applicant;
    expect(actual).toEqual(expected);
  });

  expectedDrawDataResults.forEach((expected) => {
    const actual = drawResult.huntCodeResults.find((r) => r.huntCode === expected.huntCode) as HuntCode;
    expect(actual).toEqual(expected);
  });
};

const buildApplicant = (input: {
  applicationNumber: number;
  age: number;
  pointBalance: number;
  choices?: string[];
}) => {
  const { choices } = input;

  return {
    ...input,
    residency: ApplicantResidency.Resident,
    choices: choices ?? [],
    drawOutcome: null,
    choiceOrdinalAwarded: null,
    choiceAwarded: null,
  };
};

const buildHuntCode = (input: {
  huntCode: string;
  year: number;
  totalQuota: number;
}) => ({
  ...input,
  totalQuotaInThisDraw: input.totalQuota,
  totalQuotaAwarded: 0,
  previousQuotaBalance: input.totalQuota,
  quotaBalance: input.totalQuota,
  quotaBalanceInThisDraw: input.totalQuota,
  quotaAwardedInThisDraw: 0,
  isValid: true,
  isInDraw: true,
  nonResidents: {
    hasHardCap: false,
    totalQuota: 0,
    capPercent: 0,
    quotaBalance: 0,
    quotaAwardedInThisDraw: 0,
    totalQuotaAwarded: 0,
  },
  residents: {
    totalQuotaAwarded: 0,
    quotaAwardedInThisDraw: 0,
  },
  wpRes: {
    isAllocEnabled: false,
    capPercent: 0,
    totalQuota: 0,
    quotaBalance: 0,
    quotaAwardedInThisDraw: 0,
    totalQuotaAwarded: 0,
  },
});

describe("Bucket Applicants", () => {
  it("Should bucket applicants by choice ordinal", () => {
    const huntCode1 = buildHuntCode({
      huntCode: "huntCode1",
      year: 2022,
      totalQuota: 10,
    });

    const huntCode2 = buildHuntCode({
      huntCode: "huntCode2",
      year: 2022,
      totalQuota: 10,
    });

    const applicant1 = buildApplicant({
      applicationNumber: 1,
      age: 30,
      pointBalance: 8,
      choices: [huntCode1.huntCode],
    });

    const applicant2 = buildApplicant({
      applicationNumber: 2,
      age: 25,
      pointBalance: 9,
      choices: [huntCode2.huntCode, huntCode1.huntCode],
    });

    const applicant3 = buildApplicant({
      applicationNumber: 3,
      age: 35,
      pointBalance: 7,
      choices: [huntCode2.huntCode],
    });

    const applicant4 = buildApplicant({
      applicationNumber: 4,
      age: 25,
      pointBalance: 10,
      choices: [huntCode1.huntCode, huntCode2.huntCode],
    });

    const applicants: Applicant[] = [applicant1, applicant2, applicant3, applicant4];

    const huntCodeEntitiesByHuntCode = {
      [huntCode1.huntCode]: huntCode1,
      [huntCode2.huntCode]: huntCode2,
    };

    let buckets = bucketApplicantsByHuntCode(huntCodeEntitiesByHuntCode, applicants, 1);

    expect(buckets).toEqual([
      {
        huntCode: huntCode1,
        applicants: [applicant1, applicant4],
        choiceOrdinal: 1,
      },
      {
        huntCode: huntCode2,
        applicants: [applicant2, applicant3],
        choiceOrdinal: 1,
      },
    ]);

    buckets = bucketApplicantsByHuntCode(huntCodeEntitiesByHuntCode, applicants, 2);

    expect(buckets).toEqual([
      {
        huntCode: huntCode1,
        applicants: [applicant2],
        choiceOrdinal: 2,
      },
      {
        huntCode: huntCode2,
        applicants: [applicant4],
        choiceOrdinal: 2,
      },
    ]);
  });
});

describe("Draw Unit Tests", () => {
  it.each([
    "draw-pref-point-age-scenario",
    "draw-data-applicant-filtering",
    "draw-nr-cap-scenario",
    "draw-wp-res-no-flow-quota-scenario",
    "draw-wp-res-flow-quota-scenario",
  ])("Draw Run Test: %s", async (testName) => await runDrawUnitTest(testName));
});

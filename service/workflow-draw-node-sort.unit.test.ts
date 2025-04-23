import { DrawSortDirection, type DrawSortRuleInput } from "@aspira-nextgen/graphql/resolvers";
import { describe, expect, it } from "vitest";
import { type Applicant, ApplicantResidency } from "#s3/applicant.ts";
import { sortApplicants } from "#service/workflow-draw-node-sort.ts";

const buildApplicant = (input: {
  applicationNumber: number;
  age: number;
  pointBalance: number;
  choices?: string[];
}): Applicant => {
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

describe("Sort Applicants", () => {
  it("Should sort applicants by multiple sort rules", async () => {
    const applicant1 = buildApplicant({
      applicationNumber: 1,
      age: 30,
      pointBalance: 8,
    });

    const applicant2 = buildApplicant({
      applicationNumber: 2,
      age: 25,
      pointBalance: 9,
    });

    const applicant3 = buildApplicant({
      applicationNumber: 3,
      age: 35,
      pointBalance: 7,
    });

    const applicant4 = buildApplicant({
      applicationNumber: 4,
      age: 25,
      pointBalance: 10,
    });

    const applicants: Applicant[] = [applicant1, applicant2, applicant3, applicant4];

    const sortRules: DrawSortRuleInput[] = [
      { field: "age", direction: DrawSortDirection.Asc },
      { field: "pointBalance", direction: DrawSortDirection.Desc },
    ];

    await sortApplicants(applicants, sortRules);

    expect(applicants).toEqual([applicant4, applicant2, applicant1, applicant3]);
  });

  it("Should sort an alphanumeric string field on applicants properly", async () => {
    const applicant1 = buildApplicant({
      applicationNumber: 1,
      age: 30,
      pointBalance: 8,
    });

    const applicant2 = buildApplicant({
      applicationNumber: 2,
      age: 25,
      pointBalance: 9,
    });

    const applicant3 = buildApplicant({
      applicationNumber: 3,
      age: 35,
      pointBalance: 7,
    });

    const applicant4 = buildApplicant({
      applicationNumber: 4,
      age: 25,
      pointBalance: 10,
    });

    const applicants: Applicant[] = [applicant4, applicant1, applicant3, applicant2];

    const sortRules: DrawSortRuleInput[] = [{ field: "applicationNumber", direction: DrawSortDirection.Asc }];

    await sortApplicants(applicants, sortRules);

    expect(applicants).toEqual([applicant1, applicant2, applicant3, applicant4]);
  });
});

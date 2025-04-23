import assert from "node:assert";
import { DrawSortDirection, type DrawSortRuleInput } from "@aspira-nextgen/graphql/resolvers";
import type { Applicant } from "#s3/applicant.ts";

export const sortApplicants = async (applicants: Applicant[], sortRules: DrawSortRuleInput[]): Promise<void> => {
  if (applicants.length <= 1) {
    return;
  }

  const sortFns = sortRules.map((sortRule) => getSortFunction(sortRule, applicants));

  applicants.sort((a, b) => {
    for (const sortFn of sortFns) {
      const result = sortFn(a, b);

      if (result !== 0) {
        return result;
      }
    }

    return 0;
  });
};

const getSortFunction = (
  sortRule: DrawSortRuleInput,
  applicants: Applicant[],
): ((a: Applicant, b: Applicant) => number) => {
  const { direction, field } = sortRule;

  assert(applicants.length > 0, "No applicants provided for sorting.");

  const sampleApplicant = applicants[0];
  const castedField = field as keyof Applicant;
  const fieldType = typeof sampleApplicant[castedField];

  if (fieldType === "string") {
    return direction === DrawSortDirection.Asc ? sortStringAsc(castedField) : sortStringDesc(castedField);
  }

  return direction === DrawSortDirection.Asc ? sortNumberAsc(castedField) : sortNumberDesc(castedField);
};

const sortStringAsc = <T>(field: keyof T): ((a: T, b: T) => number) => {
  return (a, b) => {
    const aValue = String(a[field]);
    const bValue = String(b[field]);

    return aValue.localeCompare(bValue);
  };
};

const sortStringDesc = <T>(field: keyof T): ((a: T, b: T) => number) => {
  return (a, b) => {
    const aValue = String(a[field]);
    const bValue = String(b[field]);

    return bValue.localeCompare(aValue, undefined, { numeric: true });
  };
};

const sortNumberAsc = <T>(field: keyof T): ((a: T, b: T) => number) => {
  return (a, b) => {
    const aValue = a[field];
    const bValue = b[field];

    if (aValue < bValue) {
      return -1;
    }

    if (aValue > bValue) {
      return 1;
    }

    return 0;
  };
};

const sortNumberDesc = <T>(field: keyof T): ((a: T, b: T) => number) => {
  return (a, b) => {
    const aValue = a[field];
    const bValue = b[field];

    if (aValue < bValue) {
      return 1;
    }

    if (aValue > bValue) {
      return -1;
    }

    return 0;
  };
};

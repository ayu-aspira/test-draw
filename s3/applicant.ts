import { nullablePositiveNumber, nullableString, positiveNumberWithDefault } from "@/s3/common-zod-validators";
import { readCSVFromS3 } from "@/s3/s3";
import type { DrawApplicantSortOption } from "@aspira-nextgen/graphql/resolvers";
import { z } from "zod";

export enum ApplicantResidency {
  Resident = "RESIDENT",
  NonResident = "NON_RESIDENT",
}

export enum ApplicantDrawOutcome {
  Awarded = "AWARDED",
  NotAwarded = "NOT_AWARDED",
}

export type Applicant = {
  applicationNumber: number;
  age: number;
  residency: ApplicantResidency;
  pointBalance: number;
  choices: string[];

  // The following are result fields
  // after the draw has been run.
  drawOutcome: ApplicantDrawOutcome | null;
  choiceOrdinalAwarded: number | null;
  choiceAwarded: string | null;
};

export const ApplicantSortOptions: (Omit<DrawApplicantSortOption, "field"> & { field: keyof Applicant })[] = [
  { field: "applicationNumber", label: "Application Number" },
  { field: "age", label: "Age" },
  { field: "residency", label: "Residency" },
  { field: "pointBalance", label: "Pref. Points" },
];

const ApplicantCsvRowZodDefinition = z.object({
  application_number: z.preprocess(
    (x) => (x ? Number(x) : null),
    z.number({ message: "Application number is required." }),
  ),
  age: z.preprocess((x) => (x ? Number(x) : null), z.number({ message: "Age is either missing or invalid." })),
  residency: z.nativeEnum(ApplicantResidency, { message: "residency is either missing or invalid." }),
  point_balance: positiveNumberWithDefault("point_balance", 0),
  choice1: z.string({ message: "choice1 is required." }).min(1, { message: "choice1 is required." }),
  choice2: nullableString(),
  choice3: nullableString(),
  choice4: nullableString(),
  draw_outcome: z.preprocess((x) => (x ? x : null), z.nativeEnum(ApplicantDrawOutcome).nullable()),
  choice_ordinal_awarded: nullablePositiveNumber("choice_ordinal_awarded"),
  choice_awarded: nullableString(),
});

export type ApplicantCsvRow = z.infer<typeof ApplicantCsvRowZodDefinition>;

export const validateApplicantRows = (rows: Record<string, string>[]) => rows.forEach(validateApplicantRow);

const validateApplicantRow = (row: Record<string, string>): ApplicantCsvRow => {
  const result = ApplicantCsvRowZodDefinition.safeParse(row);

  if (!result.success) {
    const applicantNumber = row.application_number;

    const messagePrefix = applicantNumber
      ? `Record ${applicantNumber} has the following errors:`
      : "The following errors occurred:";

    const errors = Object.values(result.error.flatten().fieldErrors).flat();

    throw new Error(`${messagePrefix}\n${errors.join("\n")}`);
  }

  return result.data;
};

const mapChoices = (row: ApplicantCsvRow): string[] =>
  [row.choice1, row.choice2, row.choice3, row.choice4].filter(
    (choice): choice is string => choice !== undefined && choice !== null,
  );

export const readAllApplicants = async (s3Key: string): Promise<Applicant[]> =>
  mapApplicantCsvRowsToApplicants(await readCSVFromS3(s3Key));

/**
 * @private visible for testing
 */
export const mapApplicantCsvRowsToApplicants = (rows: Record<string, string>[]): Applicant[] =>
  rows.map((row) => {
    const applicantCsvRow = validateApplicantRow(row);

    return {
      applicationNumber: applicantCsvRow.application_number,
      age: applicantCsvRow.age,
      residency: applicantCsvRow.residency,
      pointBalance: applicantCsvRow.point_balance,
      choices: mapChoices(applicantCsvRow),
      drawOutcome: applicantCsvRow.draw_outcome,
      choiceOrdinalAwarded: applicantCsvRow.choice_ordinal_awarded,
      choiceAwarded: applicantCsvRow.choice_awarded,
    };
  });

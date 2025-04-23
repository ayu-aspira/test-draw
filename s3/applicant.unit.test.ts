import { ApplicantResidency, validateApplicantRows } from "@/s3/applicant";
import { describe, expect, it, vi } from "vitest";

vi.mock("@aspira-nextgen/core/dynamodb", async () => {
  const actual = await vi.importActual("@aspira-nextgen/core/dynamodb");
  return {
    ...actual,
    createDynamoDBClient: vi.fn(),
  };
});

vi.mock("sst/node/bucket", () => ({
  Bucket: {
    DrawDocumentBucket: {
      bucketName: "draw-document-bucket",
    },
  },
}));

describe("Applicant CSV Validation Tests", () => {
  it("Should validate a valid applicant CSV.", () => {
    const validRows = [
      {
        application_number: "1",
        residency: ApplicantResidency.Resident,
        choice1: "huntCode1",
        age: "30",
        point_balance: "10",
      },
      {
        application_number: "2",
        residency: ApplicantResidency.NonResident,
        choice1: "huntCode2",
        age: "25",
        point_balance: "5",
      },
    ];

    expect(() => validateApplicantRows(validRows)).not.toThrow();

    validRows[0].point_balance = "";

    expect(() => validateApplicantRows(validRows)).not.toThrow();
  });

  it("Should throw an error if a record is invalid.", () => {
    let invalidRows: Record<string, string>[] = [
      {
        residency: ApplicantResidency.Resident,
        choice1: "huntCode1",
        age: "30",
        point_balance: "10",
      },
    ];

    expect(() => validateApplicantRows(invalidRows)).toThrow(
      "The following errors occurred:\nApplication number is required.",
    );

    invalidRows = [
      {
        application_number: "1",
        choice1: "huntCode1",
        age: "30",
        point_balance: "10",
      },
    ];

    expect(() => validateApplicantRows(invalidRows)).toThrow(
      "Record 1 has the following errors:\nresidency is either missing or invalid.",
    );

    invalidRows = [
      {
        application_number: "1",
        residency: "InvalidResidency",
        choice1: "huntCode1",
        age: "30",
        point_balance: "10",
      },
    ];

    expect(() => validateApplicantRows(invalidRows)).toThrow(
      "Record 1 has the following errors:\nresidency is either missing or invalid.",
    );

    invalidRows = [
      {
        application_number: "1",
        residency: ApplicantResidency.Resident,
        age: "30",
        point_balance: "10",
      },
    ];

    expect(() => validateApplicantRows(invalidRows)).toThrow(
      "Record 1 has the following errors:\nchoice1 is required.",
    );

    invalidRows = [
      {
        application_number: "1",
        residency: ApplicantResidency.Resident,
        choice1: "",
        choice2: "",
        choice3: "",
        choice4: "",
        age: "30",
        point_balance: "10",
      },
    ];

    expect(() => validateApplicantRows(invalidRows)).toThrow(
      "Record 1 has the following errors:\nchoice1 is required.",
    );

    invalidRows = [
      {
        application_number: "1",
        residency: ApplicantResidency.Resident,
        choice1: "huntCode1",
        age: "",
        point_balance: "10",
      },
    ];

    expect(() => validateApplicantRows(invalidRows)).toThrow(
      "Record 1 has the following errors:\nAge is either missing or invalid.",
    );

    invalidRows = [
      {
        application_number: "1",
        residency: ApplicantResidency.Resident,
        choice1: "huntCode1",
        age: "30",
        point_balance: "invalidPointBalance",
      },
    ];

    expect(() => validateApplicantRows(invalidRows)).toThrow(
      "Record 1 has the following errors:\nField point_balance must be a number if provided.",
    );
  });
});

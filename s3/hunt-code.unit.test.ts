import { validateHuntCodeRows } from "@/s3/hunt-code";
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

describe("Hunt Code CSV Validation Tests", () => {
  it("Should validate valid hunt code rows.", () => {
    const validRows = [
      {
        hunt_code: "huntCode1",
        in_the_draw: "Y",
        is_valid: "Y",
        total_quota: "10",
        total_quota_awarded: "5",
        quota_balance: "5",
        quota_awarded_in_this_draw: "2",
        nr_cap_chk: "N",
        nrcap_prect: "",
        nrcap_amt: "",
        nrcap_bal: "",
        nr_quota_awarded_in_this_draw: "",
        nr_total_quota_awarded: "",
      },
      {
        hunt_code: "huntCode2",
        in_the_draw: "N",
        is_valid: "Y",
        total_quota: "5",
        total_quota_awarded: "0",
        quota_balance: "0",
        quota_awarded_in_this_draw: "0",
        nr_cap_chk: "N",
        nrcap_prect: "",
        nrcap_amt: "",
        nrcap_bal: "",
        nr_quota_awarded_in_this_draw: "",
        nr_total_quota_awarded: "",
      },
      {
        hunt_code: "huntCode3",
        in_the_draw: "Y",
        is_valid: "N",
        total_quota: "",
        total_quota_awarded: "",
        quota_balance: "",
        quota_awarded_in_this_draw: "",
        nr_cap_chk: "Y",
        nrcap_prect: "",
        nrcap_amt: "",
        nrcap_bal: "",
        nr_quota_awarded_in_this_draw: "",
        nr_total_quota_awarded: "",
      },
      {
        hunt_code: "huntCode4",
        in_the_draw: "Y",
        is_valid: "Y",
        total_quota: "10",
        total_quota_awarded: "5",
        quota_balance: "5",
        quota_awarded_in_this_draw: "2",
        nr_cap_chk: "Y",
        nrcap_prect: "10",
        nrcap_amt: "5",
        nrcap_bal: "0",
        nr_quota_awarded_in_this_draw: "1",
        nr_total_quota_awarded: "2",
      },
    ];

    expect(() => validateHuntCodeRows(validRows)).not.toThrow();
  });

  it("Should throw an error if the record is invalid.", () => {
    let invalidRows: Record<string, string>[] = [
      {
        total_quota: "10",
      },
    ];

    expect(() => validateHuntCodeRows(invalidRows)).toThrow(
      "The following errors occurred:\nhunt_code is required.\nis_valid must be either 'Y' or 'N'.\nin_the_draw must be either 'Y' or 'N'.",
    );

    invalidRows[0].hunt_code = "";

    expect(() => validateHuntCodeRows(invalidRows)).toThrow(
      "The following errors occurred:\nhunt_code is required.\nis_valid must be either 'Y' or 'N'.\nin_the_draw must be either 'Y' or 'N'.",
    );

    invalidRows = [
      {
        hunt_code: "huntCode1",
        total_quota: "invalidQuota",
      },
    ];

    expect(() => validateHuntCodeRows(invalidRows)).toThrow(
      "Record huntCode1 has the following errors:\nis_valid must be either 'Y' or 'N'.\nin_the_draw must be either 'Y' or 'N'.\nField total_quota must be a number if provided.",
    );

    invalidRows = [
      {
        hunt_code: "1",
        total_quota: "-1",
      },
    ];

    expect(() => validateHuntCodeRows(invalidRows)).toThrow(
      "Record 1 has the following errors:\nis_valid must be either 'Y' or 'N'.\nin_the_draw must be either 'Y' or 'N'.\nField total_quota cannot be negative.",
    );

    invalidRows = [
      {
        hunt_code: "1",
        is_valid: "invalid",
      },
    ];

    expect(() => validateHuntCodeRows(invalidRows)).toThrow(
      "Record 1 has the following errors:\nis_valid must be either 'Y' or 'N'.\nin_the_draw must be either 'Y' or 'N'.",
    );

    invalidRows = [
      {
        hunt_code: "1",
        in_the_draw: "Y",
        is_valid: "Y",
        total_quota: "10",
        nr_cap_chk: "Y",
        nrcap_prect: "-10",
      },
    ];

    expect(() => validateHuntCodeRows(invalidRows)).toThrow(
      "Record 1 has the following errors:\nField nrcap_prect cannot be negative.",
    );
  });
});

import { DynamoDBPrefix } from "@/dynamo/dynamo";
import { ulid } from "ulidx";
import { assert, describe, it, vi } from "vitest";

// importing dynamo/dynamo creates a client that needs sst
vi.mock("@aspira-nextgen/core/dynamodb", () => {
  return {
    createDynamoDBClient: () => {
      return {};
    },
  };
});

describe("DynamoDBPrefix", () => {
  it("should not contain prefix collisions", () => {
    //. get all the prefixes
    const prefixes = Object.values(DynamoDBPrefix);

    // make a list of ids prefix + ulid
    const ids = prefixes.map((prefix) => `${prefix}_${ulid()}`);

    // iterate over all prefixes and ensure they only match "startsWith" once
    prefixes.forEach((prefix) => {
      const matches = ids.filter((id) => id.startsWith(prefix));
      assert(matches.length === 1, `prefix ${prefix} matched ${matches.length} times`);
    });
  });
});

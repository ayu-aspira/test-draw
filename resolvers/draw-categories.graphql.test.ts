import { DynamoDBTypename } from "@/dynamo/dynamo";
import { createDrawCategory, createGraphQLClient, deleteDrawCategory } from "@/util/graphql-test-utils";
import type { DrawCategory } from "@aspira-nextgen/graphql/resolvers";
import { type GraphQLClient, gql } from "graphql-request";
import { ulid } from "ulidx";
import { afterAll, beforeAll, describe, expect, it as maybe, vi } from "vitest";

const { ENABLE_DRAW_GRAPHQL_TESTS } = process.env;
const it = ENABLE_DRAW_GRAPHQL_TESTS && ["1", "true", "yes"].includes(ENABLE_DRAW_GRAPHQL_TESTS) ? maybe : maybe.skip;

// importing dynamo/dynamo creates a client that needs sst
vi.mock("@aspira-nextgen/core/dynamodb", () => {
  return {
    createDynamoDBClient: () => {
      return {};
    },
  };
});

type Fixtures = {
  drawCategory: DrawCategory;
};
const createFixtures = async (client: GraphQLClient): Promise<Fixtures> => {
  return {
    drawCategory: await createDrawCategory({ name: `Draw Category ${ulid()}` }, client),
  };
};

describe("Query drawCategories", () => {
  let client: GraphQLClient;
  let fixtures: Fixtures;

  beforeAll(async () => {
    client = createGraphQLClient();
    fixtures = await createFixtures(client);
  }, 30_000);

  afterAll(async () => {
    if (fixtures.drawCategory) {
      await deleteDrawCategory({ id: fixtures.drawCategory.id }, client);
    }
  }, 30_000);

  it("drawCategories", async () => {
    expect(fixtures.drawCategory).toBeDefined();

    const query = gql`
      {
        drawCategories(input: { limit: 1000 }) {
          items {
            __typename
            id
            name
          }
        }
      }
  `;

    const response = await client.request(query);

    expect(response).toEqual({
      drawCategories: {
        items: expect.arrayContaining([
          expect.objectContaining({
            id: fixtures.drawCategory.id,
            name: fixtures.drawCategory.name,
            __typename: DynamoDBTypename.DRAW_CATEGORY,
          }),
        ]),
      },
    });
  });
}, 30_000);

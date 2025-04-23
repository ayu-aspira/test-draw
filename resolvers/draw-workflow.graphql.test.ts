import { DynamoDBTypename } from "@/dynamo/dynamo";
import {
  type WorkflowFixtures,
  cleanupWorkflowFixtures,
  createGraphQLClient,
  createWorkflowFixtures,
} from "@/util/graphql-test-utils";
import { type GraphQLClient, gql } from "graphql-request";
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

describe("Query drawWorkflow", () => {
  let client: GraphQLClient;
  let fixtures: WorkflowFixtures;

  beforeAll(async () => {
    client = createGraphQLClient();
    fixtures = await createWorkflowFixtures(client);
  }, 30_000);

  afterAll(async () => {
    if (fixtures) {
      await cleanupWorkflowFixtures(fixtures, client);
    }
  }, 30_000);

  it("drawWorkflow", async () => {
    const query = gql`
      query DrawWorkflow($id: ID!) {
        drawWorkflow(id: $id) {
          __typename
          id
          drawCategoryId

          workflow {
            id
            nodes(input: { limit: 1000 }) {
              items {
                __typename
                id

                ... on WorkflowNodeWithResults {
                  results {
                    workflowNodeResultId
                    domain
                    domainModel
                    type
                  }
                }

                ... on WorkflowDrawNode {
                  config {
                    id
                  }
                }

                ... on WorkflowDrawDataNode {
                  document {
                    id
                  }
                }

                ... on WorkflowDrawApplicantDataNode {
                  document {
                    id
                  }
                }
              }
            }
          }
        }
      }
    `;

    const variables = { id: fixtures.drawWorkflow.id };

    const response = await client.request(query, variables);

    expect(response).toEqual({
      drawWorkflow: {
        id: expect.stringMatching(/^draw_workflow_/),
        drawCategoryId: expect.stringMatching(/^draw_category_/),
        workflow: {
          id: fixtures.workflow.id,
          nodes: {
            items: expect.arrayContaining([
              {
                __typename: "WorkflowDrawDataNode",
                document: { id: fixtures.huntcodes.id },
                id: fixtures.data1.id,
              },
              {
                __typename: "WorkflowDrawApplicantDataNode",
                document: { id: fixtures.applicants.id },
                id: fixtures.data2.id,
              },
              {
                __typename: "WorkflowDrawNode",
                config: { id: fixtures.drawConfig.id },
                id: fixtures.node1.id,
                results: [],
              },
              {
                __typename: "WorkflowNoopNode",
                id: fixtures.node2.id,
                results: [],
              },
            ]),
          },
        },
        __typename: DynamoDBTypename.DRAW_WORKFLOW,
      },
    });
  });
}, 30_000);

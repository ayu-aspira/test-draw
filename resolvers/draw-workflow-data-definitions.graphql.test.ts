import { GraphQLTypename } from "@/service/graphql";
import { WorkflowMimeDataType } from "@/step-function/workflow-types";
import {
  type WorkflowFixtures,
  cleanupWorkflowFixtures,
  createGraphQLClient,
  createWorkflowFixtures,
} from "@/util/graphql-test-utils";
import { WorkflowDomain } from "@aspira-nextgen/graphql/resolvers";
import { type GraphQLClient, gql } from "graphql-request";
import { afterAll, beforeAll, describe, expect, it as maybe } from "vitest";

const { ENABLE_DRAW_GRAPHQL_TESTS } = process.env;
const it = ENABLE_DRAW_GRAPHQL_TESTS && ["1", "true", "yes"].includes(ENABLE_DRAW_GRAPHQL_TESTS) ? maybe : maybe.skip;

describe("GraphQL Queries", () => {
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

  it("drawWorkflowDataDefinitions", async () => {
    const query = gql`
      {
        drawWorkflowDataDefinitions {
          __typename

          nodes {
            __typename

            type
            inputs {
              domain
              domainModel
              mimetypes
            }
            outputs {
              domain
              domainModel
              mimetypes
            }
          }

          dataNodes {
            __typename

            type
            outputs {
              domain
              domainModel
              mimetypes
            }
          }
        }
      }
    `;

    const response = await client.request(query);

    expect(response).toEqual({
      drawWorkflowDataDefinitions: {
        __typename: GraphQLTypename.WORKFLOW_DATA_DEFINITION,
        nodes: expect.arrayContaining([
          expect.objectContaining({
            inputs: expect.arrayContaining([
              expect.objectContaining({
                domain: WorkflowDomain.Draw,
                domainModel: expect.any(String),
                mimetypes: expect.arrayContaining([
                  expect.toBeOneOf([
                    WorkflowMimeDataType.ANY,
                    WorkflowMimeDataType.CSV,
                    WorkflowMimeDataType.JSON,
                    WorkflowMimeDataType.ZIP,
                  ]),
                ]),
              }),
            ]),
            outputs: expect.arrayContaining([
              expect.objectContaining({
                domain: WorkflowDomain.Draw,
                domainModel: expect.any(String),
                mimetypes: expect.arrayContaining([
                  expect.toBeOneOf([
                    WorkflowMimeDataType.ANY,
                    WorkflowMimeDataType.CSV,
                    WorkflowMimeDataType.JSON,
                    WorkflowMimeDataType.ZIP,
                  ]),
                ]),
              }),
            ]),
            __typename: GraphQLTypename.WORKFLOW_FUNCTIONAL_NODE_DEFINITION,
          }),
        ]),
        dataNodes: expect.arrayContaining([
          expect.objectContaining({
            outputs: expect.arrayContaining([
              expect.objectContaining({
                domain: WorkflowDomain.Draw,
                domainModel: expect.any(String),
                mimetypes: expect.arrayContaining([
                  expect.toBeOneOf([
                    WorkflowMimeDataType.ANY,
                    WorkflowMimeDataType.CSV,
                    WorkflowMimeDataType.JSON,
                    WorkflowMimeDataType.ZIP,
                  ]),
                ]),
              }),
            ]),
            __typename: GraphQLTypename.WORKFLOW_DATA_NODE_DEFINITION,
          }),
        ]),
      },
    });
  });
}, 30_000);

import {
  type WorkflowFixtures,
  cleanupWorkflowFixtures,
  createGraphQLClient,
  createWorkflowFixtures,
} from "@/util/graphql-test-utils";
import { withConsistencyRetries } from "@aspira-nextgen/core/dynamodb-test-utils";
import { type DrawWorkflow, type WorkflowJob, WorkflowJobStatus } from "@aspira-nextgen/graphql/resolvers";
import { type GraphQLClient, gql } from "graphql-request";
import { afterAll, beforeAll, describe, expect, it as maybe } from "vitest";

const { ENABLE_DRAW_GRAPHQL_TESTS } = process.env;
const it = ENABLE_DRAW_GRAPHQL_TESTS && ["1", "true", "yes"].includes(ENABLE_DRAW_GRAPHQL_TESTS) ? maybe : maybe.skip;

const queueWorkflowJob = async (client: GraphQLClient, input: { workflowId: string }) => {
  const { workflowId } = input;

  const query = gql`
  mutation queueWorkflowJob($input: QueueWorkflowJobInput!) {
    queueWorkflowJob(input: $input) {
      id
      status
    }
  }
  `;

  const variables = { input: { workflowId } };

  const { queueWorkflowJob: job } = await client.request<{ queueWorkflowJob: WorkflowJob }>(query, variables);
  return job;
};

const queryWorkflowJob = async (client: GraphQLClient, input: { id: string }) => {
  const { id } = input;

  const query = gql`
    query WorkflowJob($id: ID!) {
      workflowJob(id: $id) {
        id
        status
      }
    }
  `;

  const variables = { id };

  const { workflowJob: job } = await client.request<{ workflowJob: WorkflowJob }>(query, variables);
  return job;
};

const queryWorkflow = async (client: GraphQLClient, input: { id: string }) => {
  const { id } = input;

  const query = gql`
    query DrawWorkflow($id: ID!) {
      drawWorkflow(id: $id) {
        id
        workflow {
          id
          nodes(input: { limit: 1000 }) {
            items {
              id

              ... on WorkflowNodeWithResults {
                results {
                  workflowNodeResultId
                  domain
                  domainModel
                  type
                }
              }
            }
          }
        }
      }
    }
  `;

  const variables = { id };

  const { drawWorkflow } = await client.request<{ drawWorkflow: DrawWorkflow }>(query, variables);
  return drawWorkflow;
};

describe("Mutation queueWorkflowJob", () => {
  let client: GraphQLClient;
  let fixtures: WorkflowFixtures;

  beforeAll(async () => {
    client = createGraphQLClient();
    fixtures = await createWorkflowFixtures(client);
  }, 30_000);

  afterAll(async () => {
    await cleanupWorkflowFixtures(fixtures, client);
  });

  it("queueWorkflowJob", async () => {
    const job = await queueWorkflowJob(client, { workflowId: fixtures.workflow.id });
    expect(job).toEqual(
      expect.objectContaining({
        id: expect.stringMatching(/^workflow_job_/),
        status: "WORKFLOW_BUILD_STARTED",
      }),
    );

    await withConsistencyRetries(
      async () => {
        const { status } = await queryWorkflowJob(client, { id: job.id });
        expect(status).toBe(WorkflowJobStatus.Succeeded);
      },
      30,
      1000,
    );

    const workflow = await queryWorkflow(client, { id: fixtures.drawWorkflow.id });
    expect(workflow).toEqual(
      expect.objectContaining({
        id: fixtures.drawWorkflow.id,
        workflow: {
          id: fixtures.workflow.id,
          nodes: {
            items: expect.arrayContaining([
              expect.objectContaining({
                id: fixtures.node1.id,
                results: expect.arrayContaining([
                  expect.objectContaining({
                    workflowNodeResultId: expect.stringMatching(/^workflow_node_result_/),
                    domain: "DRAW",
                    domainModel: "APPLICANTS",
                    type: "text/csv",
                  }),
                  expect.objectContaining({
                    workflowNodeResultId: expect.stringMatching(/^workflow_node_result_/),
                    domain: "DRAW",
                    domainModel: "HUNT_CODES",
                    type: "text/csv",
                  }),
                ]),
              }),
              expect.objectContaining({
                id: fixtures.node2.id,
                results: expect.arrayContaining([
                  expect.objectContaining({
                    workflowNodeResultId: expect.stringMatching(/^workflow_node_result_/),
                    domain: "DRAW",
                    domainModel: "APPLICANTS",
                    type: "text/csv",
                  }),
                  expect.objectContaining({
                    workflowNodeResultId: expect.stringMatching(/^workflow_node_result_/),
                    domain: "DRAW",
                    domainModel: "HUNT_CODES",
                    type: "text/csv",
                  }),
                ]),
              }),
            ]),
          },
        },
      }),
    );
  });
}, 60_000);

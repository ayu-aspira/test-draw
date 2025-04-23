import { DynamoDBTypename } from "@/dynamo/dynamo";
import { isWorkflowAcyclic } from "@/validation/workflow-is-acyclic";
import type { Workflow, WorkflowEdge, WorkflowNode, WorkflowNoopNode } from "@aspira-nextgen/graphql/resolvers";
import { ulid } from "ulidx";
import { beforeEach, describe, expect, it, vi } from "vitest";

vi.mock("@aspira-nextgen/core/dynamodb", () => {
  return {
    createDynamoDBClient: () => {
      return {};
    },
  };
});

const generateWorkflowInstance = (): Workflow => {
  return {
    id: `workflow-${ulid()}`,
    nodes: { items: [] },
    edges: { items: [] },
    __typename: DynamoDBTypename.WORKFLOW,
  };
};

const generateNodes = (workflow: Workflow, n: number): WorkflowNode[] => {
  const nodes: WorkflowNoopNode[] = [...Array(n).keys()].map((i) => ({
    id: `node-${i}`,
    workflowId: workflow.id,
    __typename: DynamoDBTypename.WORKFLOW_NOOP_NODE,
  }));

  workflow.nodes.items = nodes;

  return nodes;
};

const connectNodes = (workflow: Workflow, source: WorkflowNode, target: WorkflowNode): WorkflowEdge => {
  const edge: WorkflowEdge = {
    id: `edge-${ulid()}`,
    workflowId: workflow.id,
    sourceNodeId: source.id,
    targetNodeId: target.id,
    __typename: DynamoDBTypename.WORKFLOW_EDGE,
  };

  workflow.edges.items.push(edge);

  return edge;
};

// Introduce a cycle in the graph
//
// * start with a DAG
// * pick a random node A
// * pick a random edge with the node as the target
// * repeat a few times
// * we now have a node B that is "before" A
// * introduce a cycle by creating edge from A to B
//
const introduceCycle = (workflow: Workflow) => {
  const source = workflow.nodes.items[Math.floor(Math.random() * workflow.nodes.items.length)];

  let target: WorkflowNode | undefined = source;
  for (let i = 0; i < Math.floor(Math.random() * 5) + 1; i++) {
    const edges = workflow.edges.items.filter((edge) => edge.targetNodeId === target?.id);
    if (edges.length === 0) {
      break;
    }

    const edge = edges[Math.floor(Math.random() * edges.length)];
    target = workflow.nodes.items.find((node) => node.id === edge.sourceNodeId);
  }

  expect(target).toBeDefined();
  connectNodes(workflow, source, target as WorkflowNode);
};

describe("Workflow Validations", () => {
  let workflow: Workflow;
  let nodes: WorkflowNode[];

  describe("a single node graph", () => {
    beforeEach(() => {
      workflow = generateWorkflowInstance();
      nodes = generateNodes(workflow, 1);
    });

    it("should be a valid DAG", () => {
      expect(isWorkflowAcyclic(workflow)).toBe(true);
    });

    it("should be invalid DAG with an edge", () => {
      connectNodes(workflow, nodes[0], nodes[0]);
      expect(isWorkflowAcyclic(workflow)).toBe(false);
    });
  });

  describe("a simple graph", () => {
    beforeEach(() => {
      workflow = generateWorkflowInstance();
      nodes = generateNodes(workflow, 3);

      connectNodes(workflow, nodes[0], nodes[1]);
      connectNodes(workflow, nodes[1], nodes[2]);
    });

    it("should be a valid DAG", () => {
      expect(isWorkflowAcyclic(workflow)).toBe(true);
    });

    it("should be an invalid DAG if the last node connects to first node", () => {
      connectNodes(workflow, nodes[2], nodes[0]);
      expect(isWorkflowAcyclic(workflow)).toBe(false);
    });

    it("should be an invalid DAG if the last node connects to second node", () => {
      connectNodes(workflow, nodes[2], nodes[1]);
      expect(isWorkflowAcyclic(workflow)).toBe(false);
    });
  });

  describe("a complex graph with lots of nodes and branching and merging", () => {
    beforeEach(() => {
      workflow = generateWorkflowInstance();

      // Create nodes for different parts of the graph
      nodes = generateNodes(workflow, 15); // Generate 15 nodes

      // Branch 1
      connectNodes(workflow, nodes[0], nodes[1]); // node 0 -> node 1
      connectNodes(workflow, nodes[1], nodes[2]); // node 1 -> node 2
      connectNodes(workflow, nodes[1], nodes[3]); // node 1 -> node 3

      // Branch 2
      connectNodes(workflow, nodes[0], nodes[4]); // node 0 -> node 4
      connectNodes(workflow, nodes[4], nodes[5]); // node 4 -> node 5
      connectNodes(workflow, nodes[4], nodes[6]); // node 4 -> node 6

      // Merge point 1
      connectNodes(workflow, nodes[2], nodes[7]); // node 2 -> node 7
      connectNodes(workflow, nodes[3], nodes[7]); // node 3 -> node 7

      // Branch 3
      connectNodes(workflow, nodes[7], nodes[8]); // node 7 -> node 8
      connectNodes(workflow, nodes[8], nodes[9]); // node 8 -> node 9
      connectNodes(workflow, nodes[8], nodes[10]); // node 8 -> node 10

      // Merge point 2
      connectNodes(workflow, nodes[6], nodes[11]); // node 6 -> node 11
      connectNodes(workflow, nodes[5], nodes[11]); // node 5 -> node 11

      // Branch 4
      connectNodes(workflow, nodes[11], nodes[12]); // node 11 -> node 12
      connectNodes(workflow, nodes[12], nodes[13]); // node 12 -> node 13

      // Merge point 3
      connectNodes(workflow, nodes[13], nodes[14]); // node 13 -> node 14
      connectNodes(workflow, nodes[9], nodes[14]); // node 9 -> node 14
      connectNodes(workflow, nodes[10], nodes[14]); // node 10 -> node 14
    });

    it("should be a valid DAG", () => {
      expect(isWorkflowAcyclic(workflow)).toBe(true);
    });

    it("should be an invalid DAG if a cycle is introduced", () => {
      introduceCycle(workflow);
      expect(isWorkflowAcyclic(workflow)).toBe(false);
    });
  });
});

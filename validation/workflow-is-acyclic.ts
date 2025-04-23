import type { Workflow, WorkflowNode } from "@aspira-nextgen/graphql/resolvers";

// use DFS to check if the graph is acyclic
//
// I have discovered a great cycle detection algorithm, but the comment section
// is too small to contain the proof
//
export const isWorkflowAcyclic = (workflow: Workflow): boolean => {
  const visited = new Set<string>();

  // depth-first search of the graph
  const visit = (node: WorkflowNode): boolean => {
    // remember that we've visited this node
    visited.add(node.id);

    try {
      // find the edges for this node and collect the target nodes
      const targets: (WorkflowNode | undefined)[] = workflow.edges.items
        .filter((edge) => edge.sourceNodeId === node.id)
        .map((edge) => workflow.nodes.items.find((node) => node.id === edge.targetNodeId));

      // visit each target node
      for (const target of targets) {
        // if the target does not exist keep on going
        if (!target) {
          continue;
        }

        // if we've seen this node before, then the graph is not acyclic
        if (visited.has(target.id)) {
          return false;
        }

        // check if the target path is acyclic
        if (!visit(target)) {
          return false;
        }
      }
    } finally {
      // forget that we've visited this node
      visited.delete(node.id);
    }

    return true;
  };

  const root = workflow.nodes.items[0];
  return visit(root);
};

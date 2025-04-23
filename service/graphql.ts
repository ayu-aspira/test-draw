export enum GraphQLTypename {
  DRAW_RUN_RESULT = "DrawRunResult",
  DRAW_CATEGORY_CONNECTION = "DrawCategoryConnection",
  DRAW_SORT_RULE = "DrawSortRule",
  DRAW_APPLICANT_SORT_OPTION = "DrawApplicantSortOption",
  DRAW_CONNECTION = "DrawConnection",
  DRAW_DOCUMENT_COLLECTION_DOWNLOAD = "DrawDocumentCollectionDownload",
  DRAW_WORKFLOW_CONNECTION = "DrawWorkflowConnection",

  WORKFLOW_NODE_CONNECTION = "WorkflowNodeConnection",
  WORKFLOW_EDGE_CONNECTION = "WorkflowEdgeConnection",
  WORKFLOW_POSITION = "WorkflowPosition",
  WORKFLOW_DATA_DEFINITION = "WorkflowDataDefinition",
  WORKFLOW_DATA_NODE_DEFINITION = "WorkflowDataNodeDefinition",
  WORKFLOW_FUNCTIONAL_NODE_DEFINITION = "WorkflowFunctionalNodeDefinition",
}

export const batchResolved = <T>(): { items: T[] } => ({ items: [] });

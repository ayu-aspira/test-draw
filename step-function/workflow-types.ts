/*
 * This file contains a set of types that are used to define contracts for
 * functional nodes in a workflow.
 */

import type { WorkflowDomain, WorkflowJobLogParam, WorkflowMessageKey } from "@aspira-nextgen/graphql/resolvers";
import type { Handler } from "aws-lambda";

/**
 * A custom Error class that can be used to log errors in a workflow.
 * These errors will be persisted in the workflow job's logs.
 */
export class LoggableWorkflowError extends Error {
  constructor(
    message: string,
    public readonly messageKey: WorkflowMessageKey,
    public readonly messageParams: WorkflowJobLogParam[] = [],
  ) {
    super(message);
    this.name = "LoggableWorkflowError";
  }
}

/**
 * The MIME type of a result in a workflow.
 */
export enum WorkflowMimeDataType {
  ANY = "*/*",
  CSV = "text/csv",
  JSON = "application/json",
  ZIP = "application/zip",
}

/**
 * The data associated with a node in a workflow. This can be the output of a node that is passed
 * to the next node as input in the workflow or overridden by the workflow creator
 * depending on how the workflow was built by the user.
 */
export type WorkflowNodeData = {
  /**
   * URI describing how to access the data.
   *
   * E.g. s3://bucket/key
   */
  readonly uri: string;

  /**
   * A destructured representation of a MIME type
   * with additional domain information. When serialized
   * together, it would read like this:
   * text/csv; domain=Draw; domainType=ApplicantDocument
   */
  readonly mime: {
    /**
     * The MIME type of the data.
     *
     * E.g. text/csv
     */
    readonly type: WorkflowMimeDataType;

    /**
     * The domain of the data. This, in addition
     * with the domainModel, can be used to determine
     * how to interpret the data.
     */
    readonly domain: WorkflowDomain;

    /**
     * The domain model of the data. This, in addition
     * with the domain, can be used to determine how to
     * interpret the data.
     */
    readonly domainModel: string;
  };
};

/**
 * The context of a node in a workflow.
 */
export type WorkflowNodeContext = {
  /**
   * The unique identifier of the node within the workflow.
   */
  readonly nodeId: string;

  /**
   * The unique identifier of the previous node in the workflow.
   * Only available if the node is not the first node in the workflow.
   */
  readonly previousNodeId?: string;

  /**
   * The unique identifier of the workflow job associated with
   * this execution.
   */
  readonly workflowJobId: string;

  /**
   * The organization associated with the workflow.
   */
  readonly organizationId: string;
};

export type WorkflowNodeDataMap = {
  readonly [key: string]: WorkflowNodeData[];
};

/**
 * The payload that is passed to a node in a workflow.
 */
export type WorkflowNodePayload = {
  /**
   * The context of the node in the workflow.
   * Contains metadata about the node currently
   * executing and the workflow in general.
   */
  readonly context: WorkflowNodeContext;

  /**
   * User-configured data overrides. This is any data that
   * the workflow creator has configured to override the
   * data used by the node.
   */
  readonly nodeOverrideData: WorkflowNodeDataMap;

  /**
   * Results of a node's execution. This is the data that
   * a node has produced and is passed to the next node in
   * the workflow for use.
   */
  readonly nodeResultData: WorkflowNodeDataMap;
};

/**
 * Type that represents the input to a workflow execution. NodeId is omitted
 * as it is injected by the workflow-creator.
 */
export type WorkflowExecutionInput = {
  context: Omit<WorkflowNodeContext, "nodeId">;
};

/**
 * A handler type that each workflow node handler should
 * conform to.
 *
 * Note: we are redefining the Handler type from aws-lambda
 * because we want to avoid allowing a `void` return type.
 */
export type WorkflowNodeHandler = (
  event: Parameters<Handler<WorkflowNodePayload>>[0],
  context: Parameters<Handler<WorkflowNodePayload>>[1],
  callback: Parameters<Handler<WorkflowNodePayload>>[2],
) => Promise<WorkflowNodeData[]>;

import type { ErrorIntervals } from "@aspira-nextgen/core/dynamodb";

export const addErrorIntervalByEndExclusive = (input: {
  intervals: ErrorIntervals;
  message: string;
  endExclusive: number;
}) => {
  const { intervals, message, endExclusive } = input;
  return {
    ...intervals,
    [message]: {
      start: endExclusive - 1,
      end: endExclusive,
    },
  };
};

export const generateEntityExistsCondition = (input: {
  organizationId: string;
  entityId: string;
  table: string;
}) => ({
  ConditionCheck: {
    TableName: input.table,
    Key: {
      pk: input.organizationId,
      sk: input.entityId,
    },
    ConditionExpression: "attribute_exists(pk) AND attribute_exists(sk)",
  },
});

export const scrubUpdateFields = <T>(input: Partial<T>): Partial<T> =>
  Object.keys(input).reduce(
    (fields, key) => {
      if (input[key as keyof T] !== undefined) {
        fields[key as keyof T] = input[key as keyof T];
      }
      return fields;
    },
    {} as Partial<T>,
  );

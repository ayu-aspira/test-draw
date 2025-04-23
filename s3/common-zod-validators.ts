import { z } from "zod";

export const nullablePositiveNumber = (field: string) =>
  z.preprocess(
    (x) => (x ? Number(x) : null),
    z
      .number({ message: `Field ${field} must be a number if provided.` })
      .min(0, `Field ${field} cannot be negative.`)
      .nullable(),
  );

export const positiveNumberWithDefault = (field: string, defaultValue: number) =>
  // Need to use `undefined` as that is what will inform zod to use the default value.
  z.preprocess(
    (x) => (x ? Number(x) : undefined),
    z
      .number({ message: `Field ${field} must be a number if provided.` })
      .min(0, `Field ${field} cannot be negative.`)
      .default(defaultValue),
  );

// Will translate '' to null.
export const nullableString = () => z.preprocess((x) => (x ? x : null), z.string().min(1).nullable());

import { z } from "zod";

export const trimmedString = (input?: { min?: number; max?: number }) =>
  z
    .string()
    .transform((value) => value.replace(/\s+/g, " "))
    .pipe(
      z
        .string()
        .trim()
        .min(input?.min || 1)
        .max(input?.max || 64),
    );

export const idStringPrefix = (prefix: string) => z.string().startsWith(prefix, "Invalid ID");

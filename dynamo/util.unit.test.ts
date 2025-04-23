import { scrubUpdateFields } from "@/dynamo/util";
import { describe, expect, it } from "vitest";

describe("scrubUpdateFields", () => {
  it("should return an empty object when the input is empty", () => {
    const result = scrubUpdateFields({});
    expect(result).toEqual({});
  });

  it("should return an object with the same properties when no undefined values are present", () => {
    const input = { name: "John", age: 30, active: true };
    const result = scrubUpdateFields(input);
    expect(result).toEqual(input);
  });

  it("should exclude undefined values from the result", () => {
    const input = { name: "John", age: undefined, active: true };
    const result = scrubUpdateFields(input);
    expect(result).toEqual({ name: "John", active: true });
  });

  it("should retain null values but exclude undefined values", () => {
    const input = { name: "John", age: null, active: undefined };
    const result = scrubUpdateFields(input);
    expect(result).toEqual({ name: "John", age: null });
  });

  it("should handle only undefined values", () => {
    const input = { name: undefined, age: undefined };
    const result = scrubUpdateFields(input);
    expect(result).toEqual({});
  });

  it("should handle mixed types", () => {
    const input = { stringValue: "Hello", numberValue: 42, nullValue: null, undefinedValue: undefined };
    const result = scrubUpdateFields(input);
    expect(result).toEqual({ stringValue: "Hello", numberValue: 42, nullValue: null });
  });

  it("should handle deep objects (nested objects are not included)", () => {
    const input = { user: { name: "John", age: 30 }, active: true };
    const result = scrubUpdateFields(input);
    expect(result).toEqual({ user: { name: "John", age: 30 }, active: true });
  });

  it("should handle a property with an empty string value", () => {
    const input = { name: "", age: 30 };
    const result = scrubUpdateFields(input);
    expect(result).toEqual({ name: "", age: 30 });
  });

  it("should handle falsy values like false and 0", () => {
    const input = { zero: 0, isActive: false, undefinedValue: undefined, name: "John" };
    const result = scrubUpdateFields(input);
    expect(result).toEqual({ zero: 0, isActive: false, name: "John" });
  });
});

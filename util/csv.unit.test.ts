import { transformToCsv } from "@/util/csv";
import { describe, expect, it } from "vitest";

describe("JSON -> CSV Transform Tests", () => {
  it("Should transform a JSON object to a CSV string.", () => {
    const json = [
      { name: "John", age: 30, city: "New York" },
      { name: "Jane", age: 25, city: "Los Angeles" },
      { name: "Doe", age: 40, city: "Chicago" },
    ];

    const csv = transformToCsv(
      [
        { field: "name", header: "name" },
        { field: "age", header: "age" },
        { field: "city", header: "city" },
      ],
      json,
    );

    expect(csv).toEqual("name,age,city\nJohn,30,New York\nJane,25,Los Angeles\nDoe,40,Chicago");
  });
});

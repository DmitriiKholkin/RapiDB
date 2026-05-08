import { describe, expect, it } from "vitest";
import { inferColumnsFromRows } from "../../src/extension/dbDrivers/nosqlUtils";

describe("inferColumnsFromRows", () => {
  it("infers plain string values as text", () => {
    const columns = inferColumnsFromRows(
      [
        { _id: "69fc407aa89c5090b53d88b4", name: "Bob" },
        { _id: "69fc407aa89c5090b53d88b5", name: "Alice" },
      ],
      "_id",
    );

    const nameColumn = columns.find((column) => column.name === "name");
    expect(nameColumn?.category).toBe("text");
    expect(nameColumn?.nativeType).toBe("text");
  });
});

import { describe, expect, it, vi } from "vitest";
import type {
  IDBDriver,
  TableInfoList,
} from "../../src/extension/dbDrivers/types";
import { loadSchemaScope } from "../../src/extension/schema/schemaLoaders";

function createDriver(objects: TableInfoList): IDBDriver {
  return {
    listObjects: vi.fn(async () => objects),
    describeTable: vi.fn(async () => []),
  } as unknown as IDBDriver;
}

describe("loadSchemaScope — truncated object lists", () => {
  it("marks the schema entry when the driver reports a truncated object list", async () => {
    const objects: TableInfoList = [
      { schema: "", name: "users", type: "table" },
    ];
    objects.truncated = true;

    await expect(
      loadSchemaScope(createDriver(objects), "db0", "db0"),
    ).resolves.toEqual({
      name: "db0",
      objects: [{ name: "users", type: "table", columns: [] }],
      objectsTruncated: true,
    });
  });

  it("leaves complete object lists unmarked", async () => {
    const schema = await loadSchemaScope(
      createDriver([{ schema: "", name: "users", type: "table" }]),
      "db0",
      "db0",
    );

    expect(schema).not.toHaveProperty("objectsTruncated");
  });
});

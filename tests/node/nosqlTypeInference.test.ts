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

  it("marks non-key NoSQL fields nullable in schema-less mode without sampling nulls", () => {
    const columns = inferColumnsFromRows(
      [
        { _id: "69fc407aa89c5090b53d88b4", name: "Bob" },
        { _id: "69fc407aa89c5090b53d88b5", name: "Alice" },
      ],
      "_id",
      { nullableMode: "schemaLess" },
    );

    const idColumn = columns.find((column) => column.name === "_id");
    const nameColumn = columns.find((column) => column.name === "name");

    expect(idColumn?.nullable).toBe(false);
    expect(nameColumn?.nullable).toBe(true);
    expect(nameColumn?.filterOperators).toEqual([
      "like",
      "is_null",
      "is_not_null",
    ]);
  });

  it("supports composite primary keys for NoSQL inference", () => {
    const columns = inferColumnsFromRows(
      [
        {
          tenant_id: "tenant-1",
          user_id: "user-1",
          email: "person@example.com",
        },
      ],
      "tenant_id",
      {
        primaryKeyNames: ["tenant_id", "user_id"],
        nullableMode: "schemaLess",
      },
    );

    expect(columns).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          name: "tenant_id",
          isPrimaryKey: true,
          primaryKeyOrdinal: 1,
          nullable: false,
        }),
        expect.objectContaining({
          name: "user_id",
          isPrimaryKey: true,
          primaryKeyOrdinal: 2,
          nullable: false,
        }),
        expect.objectContaining({
          name: "email",
          isPrimaryKey: false,
          nullable: true,
        }),
      ]),
    );
  });

  it("keeps discovered column order while placing primary keys first", () => {
    const columns = inferColumnsFromRows(
      [
        {
          created_at: "2026-05-31T10:00:00Z",
          tenant_id: "tenant-1",
          user_id: "user-1",
          email: "first@example.com",
        },
        {
          user_id: "user-2",
          metadata: '{"role":"admin"}',
        },
      ],
      "tenant_id",
      {
        primaryKeyNames: ["tenant_id", "user_id"],
        nullableMode: "schemaLess",
      },
    );

    expect(columns.map((column) => column.name)).toEqual([
      "tenant_id",
      "user_id",
      "created_at",
      "email",
      "metadata",
    ]);
  });

  it("supports schema inference without any primary keys", () => {
    const columns = inferColumnsFromRows(
      [{ _id: "row-1", status: "active" }],
      "_id",
      {
        primaryKeyNames: [],
        nullableMode: "schemaLess",
      },
    );

    const idColumn = columns.find((column) => column.name === "_id");
    expect(idColumn?.isPrimaryKey).toBe(false);
    expect(idColumn?.primaryKeyOrdinal).toBeUndefined();
    expect(idColumn?.nullable).toBe(true);
  });
});

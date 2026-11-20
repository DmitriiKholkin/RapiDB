import { describe, expect, it } from "vitest";
import {
  applyFilters,
  inferColumnsFromRows,
} from "../../src/extension/dbDrivers/nosqlUtils";
import { inferValueCategory } from "../../src/shared/tableTypes";

describe("inferValueCategory", () => {
  it.each([
    ["  +9007199254740993  ", "integer"],
    ["-01", "integer"],
    ["1.5e3", "decimal"],
    ["+.5e-2", "decimal"],
    ["-1.E+3", "decimal"],
    ["1e3", "decimal"],
    ["3.", "decimal"],
    ["1.234567890123456789e999", "decimal"],
    ["1.5e", null],
    [".e3", null],
  ])("classifies exact numeric tokens: %s", (value, category) => {
    expect(inferValueCategory(value)).toBe(category);
  });

  it.each([
    ["2024-02-29", "date"],
    ["2000-02-29", "date"],
    ["0004-02-29", "date"],
    ["00:00", "time"],
    ["23:59:59.123456", "time"],
    ["24:00:00.000000", "time"],
    ["2024-02-29T00:00", "datetime"],
    ["2024-02-29 23:59:59.123456 -04:30", "datetime"],
    ["2024-02-29T12:30+05:45", "datetime"],
    ["2024-02-29t12:30z", "datetime"],
    ["2024-02-29T24:00Z", "datetime"],
    ["1900-02-29", null],
    ["2026-02-29", null],
    ["2026-04-31", null],
    ["2026-13-45", null],
    ["2026-01-00", null],
    ["25:99", null],
    ["12:60", null],
    ["12:00:60", null],
    ["24:01", null],
    ["24:00:00.000001", null],
    ["2026-02-30T12:30:10Z", null],
    ["2024-02-29T25:00:00Z", null],
    ["2024-02-29T12:30+24:00", null],
    ["2024-02-29T12:30-04:60", null],
  ])("classifies supported temporal strings: %s", (value, category) => {
    expect(inferValueCategory(value)).toBe(category);
  });
});

describe("inferColumnsFromRows", () => {
  it("compares inferred exponent decimals without rounding them", () => {
    const rows = [
      { amount: "9.007199254740992e15" },
      { amount: "9.007199254740993e15" },
    ];
    const columns = inferColumnsFromRows(rows);

    expect(columns[0]?.filterOperators).toContain("gt");
    expect(
      applyFilters(
        rows,
        [{ column: "amount", operator: "gt", value: "9007199254740992" }],
        columns,
      ),
    ).toEqual([rows[1]]);
  });

  it.each([
    false,
    true,
  ])("infers the same scalar categories with consistentCategories=%s", (consistentCategories) => {
    const columns = inferColumnsFromRows(
      [
        {
          integer: "-01",
          amount: "+.5e-2",
          dateLabel: "2026-13-45",
          timeLabel: "25:99",
          blank: " ",
        },
      ],
      "id",
      { consistentCategories },
    );

    expect(columns.map(({ category }) => category)).toEqual([
      "integer",
      "decimal",
      "text",
      "text",
      "other",
    ]);
  });

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

  it("infers a field from its first non-null value", () => {
    const columns = inferColumnsFromRows(
      [
        { _id: "row-1", score: null },
        { _id: "row-2", score: 5 },
      ],
      "_id",
    );

    expect(columns.find((column) => column.name === "score")?.category).toBe(
      "integer",
    );
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

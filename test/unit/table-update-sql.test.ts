import { describe, expect, it, vi } from "vitest";
import type { IDBDriver } from "../../src/extension/dbDrivers/types";
import {
  buildUpdateRowSql,
  writableEntries,
} from "../../src/extension/table/updateSql";
import { col } from "./helpers";

function makeDriver(): Pick<
  IDBDriver,
  | "buildInsertValueExpr"
  | "buildSetExpr"
  | "coerceInputValue"
  | "qualifiedTableName"
  | "quoteIdentifier"
> {
  return {
    buildInsertValueExpr: vi.fn((_column, paramIndex) => `$${paramIndex}`),
    buildSetExpr: vi.fn((column, paramIndex) => `${column.name} = $${paramIndex}`),
    coerceInputValue: vi.fn((value) =>
      typeof value === "string" ? value.trim() : value,
    ),
    qualifiedTableName: vi.fn(() => '"public"."users"'),
    quoteIdentifier: vi.fn((name) => `"${name}"`),
  };
}

describe("updateSql helpers", () => {
  it("filters writable values before generating insert/update entries", () => {
    const entries = writableEntries(
      {
        id: 1,
        name: "Alice",
        ignored: undefined,
      },
      new Map([
        [
          "id",
          col({
            name: "id",
            type: "integer",
            category: "integer",
            editable: false,
            isAutoIncrement: true,
          }),
        ],
        [
          "name",
          col({
            name: "name",
            type: "text",
            category: "text",
            editable: true,
          }),
        ],
      ]),
    );

    expect(entries).toEqual([["name", "Alice"]]);
  });

  it("builds update SQL with coerced params for writable changes and primary keys", () => {
    const driver = makeDriver();

    const result = buildUpdateRowSql(
      driver as unknown as IDBDriver,
      "main",
      "public",
      "users",
      { id: " 42 " },
      { name: " Alice ", audit_stamp: "ignored" },
      [
        col({
          name: "id",
          type: "integer",
          category: "integer",
          editable: false,
          isPrimaryKey: true,
        }),
        col({
          name: "name",
          type: "text",
          category: "text",
          editable: true,
        }),
        col({
          name: "audit_stamp",
          type: "timestamp",
          category: "datetime",
          editable: false,
        }),
      ],
    );

    expect(result).toEqual({
      sql: 'UPDATE "public"."users" SET name = $1 WHERE "id" = $2',
      params: ["Alice", "42"],
    });
  });
});
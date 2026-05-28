import { describe, expect, it } from "vitest";
import { PostgresDriver } from "../../src/extension/dbDrivers/postgres";
import type { ColumnTypeMeta } from "../../src/shared/tableTypes";

const driver = new PostgresDriver({
  id: "persisted-edit-diagnostics",
  name: "persisted-edit-diagnostics",
  type: "pg",
  host: "localhost",
  port: 5432,
  database: "postgres",
  username: "postgres",
  password: "postgres",
});

describe("persisted edit diagnostics", () => {
  it("quotes string values so whitespace differences are visible", () => {
    const column: ColumnTypeMeta = {
      name: "col_char",
      type: "TEXT",
      nativeType: "TEXT",
      nullable: true,
      isPrimaryKey: false,
      isForeignKey: false,
      category: "text",
      filterable: true,
      filterOperators: ["eq", "like"],
      valueSemantics: "plain",
    };

    const check = driver.checkPersistedEdit(column, "wad  23", {
      persistedValue: "wad 23",
    });

    expect(check?.ok).toBe(false);
    expect(check?.message).toContain('"wad 23"');
    expect(check?.message).toContain('"wad  23"');
  });

  it("treats equivalent timestamptz offset formats as persisted matches", () => {
    const column: ColumnTypeMeta = {
      name: "created_at",
      type: "timestamp with time zone",
      nativeType: "timestamp with time zone",
      nullable: true,
      isPrimaryKey: false,
      isForeignKey: false,
      category: "datetime",
      filterable: true,
      filterOperators: ["eq", "neq", "like", "is_null", "is_not_null"],
      valueSemantics: "plain",
    };

    const check = driver.checkPersistedEdit(
      column,
      "1970-01-01 00:00:00+00:00",
      {
        persistedValue: "1970-01-01 00:00:00+00",
      },
    );

    expect(check?.ok).toBe(true);
  });

  it("skips verification for likely auto-updated temporal columns", () => {
    const column: ColumnTypeMeta = {
      name: "updated_at",
      type: "timestamp with time zone",
      nativeType: "timestamp with time zone",
      nullable: true,
      isPrimaryKey: false,
      isForeignKey: false,
      category: "datetime",
      filterable: true,
      filterOperators: ["eq", "neq", "like", "is_null", "is_not_null"],
      valueSemantics: "plain",
    };

    const check = driver.checkPersistedEdit(
      column,
      "2026-05-28 13:08:31.75746+00:00",
      {
        persistedValue: "2026-05-28 19:16:35.014986+00",
      },
    );

    expect(check).toEqual({
      ok: true,
      shouldVerify: false,
    });
  });
});

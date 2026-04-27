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
      isAutoIncrement: false,
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
});

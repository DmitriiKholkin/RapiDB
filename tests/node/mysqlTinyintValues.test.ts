import { describe, expect, it } from "vitest";
import { MySQLDriver } from "../../src/extension/dbDrivers/mysql";
import type {
  ColumnTypeMeta,
  QueryResult,
} from "../../src/extension/dbDrivers/types";

const driver = new MySQLDriver({
  id: "tinyint",
  name: "Tinyint",
  type: "mysql",
});
const column: ColumnTypeMeta = {
  name: "id",
  type: "tinyint(1)",
  nativeType: "tinyint(1)",
  category: "boolean",
  valueSemantics: "boolean",
  nullable: false,
  isPrimaryKey: true,
  isForeignKey: false,
  filterable: true,
  filterOperators: ["eq"],
};

describe("MySQL tinyint display width must not change stored values", () => {
  it.each([
    "tinyint(1)",
    "tinyint(1) unsigned",
    "tinyint(4)",
  ])("uses integer input and filters for %s", (nativeType) => {
    class MetadataDriver extends MySQLDriver {
      metadata() {
        return this.enrichColumn({ ...column, type: nativeType });
      }
    }
    const metadata = new MetadataDriver({
      id: "meta",
      name: "meta",
      type: "mysql",
    }).metadata();
    expect(metadata.category).toBe("integer");
    expect(metadata.valueSemantics).toBe("plain");
    expect(driver.coerceInputValue("-2", metadata)).toBe("-2");
    expect(driver.buildFilterCondition(metadata, "eq", "-1", 1)).toEqual({
      sql: "`id` = ?",
      params: [-1],
    });
  });
  it.each([
    -128, -2, -1, 0, 1, 2, 127, 255,
  ])("preserves %s through query and table formatting", (value) => {
    const parsed = (
      driver as unknown as {
        _parseQueryResult(
          rows: unknown[][],
          fields: unknown[],
          time: number,
        ): QueryResult;
      }
    )._parseQueryResult([[value]], [{ name: "id", type: 1, length: 1 }], 0);
    expect(parsed.rows[0].__col_0).toBe(value);
    expect(driver.formatOutputValue(parsed.rows[0].__col_0, column)).toBe(
      value,
    );
    expect(driver.formatOutputValue(value, column)).toBe(value);
  });
});

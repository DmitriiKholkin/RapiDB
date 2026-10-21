import { describe, expect, it, vi } from "vitest";
import { MySQLDriver } from "../../src/extension/dbDrivers/mysql";
import { flattenRootRecord } from "../../src/extension/dbDrivers/nosqlUtils";
import type { ColumnTypeMeta } from "../../src/extension/dbDrivers/types";

describe("Prototype-named result fields", () => {
  it.each([
    "stored",
    null,
  ])("preserves MySQL __proto__ column value %j", async (value) => {
    const driver = new MySQLDriver({
      id: "prototype",
      name: "Prototype",
      type: "mysql",
    });
    const column: ColumnTypeMeta = {
      name: "__proto__",
      type: "text",
      nativeType: "text",
      category: "text",
      nullable: true,
      isPrimaryKey: false,
      isForeignKey: false,
      filterable: true,
      filterOperators: [],
      valueSemantics: "plain",
    };
    vi.spyOn(driver, "describeColumns").mockResolvedValue([column]);
    vi.spyOn(driver, "query").mockResolvedValue({
      columns: [column.name],
      rows: [{ __col_0: value }],
      rowCount: 1,
      executionTimeMs: 0,
    });
    const page = await driver.readTablePage({
      database: "db",
      schema: "",
      table: "items",
      page: 1,
      pageSize: 25,
      filters: [],
      sort: null,
      skipCount: true,
    });
    expect(Object.hasOwn(page.rows[0], column.name)).toBe(true);
    expect(page.rows[0][column.name]).toBe(value);
    expect(Object.getPrototypeOf(page.rows[0])).toBe(Object.prototype);
  });

  it.each([
    "stored",
    null,
    { nested: "data" },
  ])("preserves flattened NoSQL __proto__ field %j", (value) => {
    const row = flattenRootRecord({
      ["__proto__"]: value,
      constructor: "ctor",
    });
    expect(Object.hasOwn(row, "__proto__")).toBe(true);
    expect(row.__proto__).toBe(
      typeof value === "object" && value !== null
        ? JSON.stringify(value)
        : value,
    );
    expect(Object.getPrototypeOf(row)).toBe(Object.prototype);
    expect(JSON.parse(JSON.stringify(row))).toEqual({
      ["__proto__"]:
        typeof value === "object" && value !== null
          ? JSON.stringify(value)
          : value,
      constructor: "ctor",
    });
  });

  it("keeps MySQL spatial sorting on the displayed WKT projection", async () => {
    const driver = new MySQLDriver({
      id: "spatial-sort",
      name: "Spatial",
      type: "mysql",
    });
    vi.spyOn(driver, "describeColumns").mockResolvedValue([
      {
        name: "shape",
        type: "geometry",
        nativeType: "geometry",
        category: "spatial",
        nullable: true,
        isPrimaryKey: false,
        isForeignKey: false,
        filterable: false,
        filterOperators: [],
        valueSemantics: "plain",
      },
    ]);
    const query = vi.spyOn(driver, "query").mockResolvedValue({
      columns: ["__rapidb_col_0"],
      rows: [{ __col_0: "POINT(1 2)" }],
      rowCount: 1,
      executionTimeMs: 0,
    });
    await driver.readTablePage({
      database: "db",
      schema: "",
      table: "items",
      page: 1,
      pageSize: 25,
      filters: [],
      sort: { column: "shape", direction: "asc" },
      skipCount: true,
    });
    expect(query.mock.calls[0][0]).toContain(
      "ORDER BY ST_AsText(`db`.`items`.`shape`) ASC",
    );
  });
});

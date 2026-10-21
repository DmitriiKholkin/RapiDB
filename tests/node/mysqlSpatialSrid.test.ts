import { describe, expect, it } from "vitest";
import { MySQLDriver } from "../../src/extension/dbDrivers/mysql";
import type { ColumnTypeMeta } from "../../src/extension/dbDrivers/types";
import { buildInsertRowOperation } from "../../src/extension/table/insertSql";

const driver = new MySQLDriver({ id: "srid", name: "SRID", type: "mysql" });
const column: ColumnTypeMeta = {
  name: "shape`column",
  type: "geometry",
  nativeType: "geometry",
  category: "spatial",
  nullable: true,
  isPrimaryKey: false,
  isForeignKey: false,
  filterable: true,
  filterOperators: [],
  valueSemantics: "plain",
};

describe("MySQL spatial SRID bindings", () => {
  it.each([
    0, 4326, 0xffffffff,
  ])("binds explicit SRID %s separately from WKT", (srid) => {
    const operation = buildInsertRowOperation(
      driver,
      "db",
      "",
      "table",
      {
        [column.name]: `SRID=${srid};POINT(1 2)`,
      },
      [column],
    );
    expect(operation.sql).toBe(
      "INSERT INTO `db`.`table` (`shape``column`) VALUES (ST_GeomFromText(?, ?))",
    );
    expect(operation.params).toEqual(["POINT(1 2)", srid]);
    expect(
      driver.materializePreviewSql(operation.sql, operation.params),
    ).toContain(`, ${srid})`);
  });

  it.each([
    "4294967296",
    "9007199254740993",
    "9".repeat(400),
  ])("rejects overflowing SRID %s before execution", (srid) => {
    expect(() =>
      buildInsertRowOperation(
        driver,
        "db",
        "",
        "table",
        {
          [column.name]: `SRID=${srid};POINT(1 2)`,
        },
        [column],
      ),
    ).toThrow(/unsigned 32-bit/);
  });

  it("quotes the column when retaining the current SRID", () => {
    expect(
      driver.buildMutationValueExpr(column, "POINT(3 4)", "update"),
    ).toEqual({
      sql: "ST_GeomFromText(?, COALESCE(ST_SRID(`shape``column`), 0))",
      params: ["POINT(3 4)"],
    });
  });

  it.each([
    "POINT(1 2)",
    "SRID=0;POINT(1 2)",
    "SRID=4326;POINT(1 2)",
  ])("compares coordinates and SRID for %s", (original) => {
    const srid = original.startsWith("SRID=4326;") ? 4326 : 0;
    expect(
      driver.buildOriginalValueComparisonWithParams(column, original),
    ).toEqual({
      sql: "ST_SRID(`shape``column`) = ? AND ST_AsBinary(`shape``column`) = ST_AsBinary(ST_GeomFromText(?, ?))",
      params: [srid, "POINT(1 2)", srid],
    });
  });
});

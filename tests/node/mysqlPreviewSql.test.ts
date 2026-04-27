import { describe, expect, it } from "vitest";
import { MySQLDriver } from "../../src/extension/dbDrivers/mysql";
import type { ColumnTypeMeta } from "../../src/extension/dbDrivers/types";
import { buildInsertRowOperation } from "../../src/extension/table/insertSql";
import type { ConnectionConfig } from "../../src/shared/connectionConfig";

const driver = new MySQLDriver({
  id: "mysql-preview-sql",
  name: "MySQL Preview SQL",
  type: "mysql",
  host: "127.0.0.1",
  port: 3306,
  database: "test",
  username: "root",
  password: "root",
} as ConnectionConfig);

function column(
  name: string,
  nativeType: string,
  category: ColumnTypeMeta["category"],
  valueSemantics: ColumnTypeMeta["valueSemantics"] = "plain",
): ColumnTypeMeta {
  return {
    name,
    type: nativeType,
    nativeType,
    category,
    nullable: true,
    defaultValue: undefined,
    isPrimaryKey: false,
    primaryKeyOrdinal: undefined,
    isForeignKey: false,
    isAutoIncrement: false,
    filterable: true,
    filterOperators: ["is_null", "is_not_null"],
    valueSemantics,
  };
}

describe("mysql preview SQL literals", () => {
  it("preserves full exact numeric literals in insert previews", () => {
    const amountColumn = column("amount", "decimal(28,10)", "decimal");
    const operation = buildInsertRowOperation(
      driver,
      "rtest",
      "",
      "all_types",
      {
        amount: "9999999999.1234567890",
      },
      [amountColumn],
    );

    expect(operation.params).toEqual(["9999999999.1234567890"]);
    expect(driver.materializePreviewSql(operation.sql, operation.params)).toBe(
      "INSERT INTO `rtest`.`all_types` (`amount`) VALUES ('9999999999.1234567890')",
    );
  });

  it("casts BIT(64) insert placeholders to unsigned numeric values", () => {
    const bitColumn = column("col_bit64", "bit(64)", "integer", "bit");
    const operation = buildInsertRowOperation(
      driver,
      "rtest",
      "",
      "all_types",
      {
        col_bit64: "17361641481138401520",
      },
      [bitColumn],
    );

    expect(operation.sql).toBe(
      "INSERT INTO `rtest`.`all_types` (`col_bit64`) VALUES (CAST(? AS UNSIGNED))",
    );
    expect(operation.params).toEqual(["17361641481138401520"]);
    expect(driver.materializePreviewSql(operation.sql, operation.params)).toBe(
      "INSERT INTO `rtest`.`all_types` (`col_bit64`) VALUES (CAST('17361641481138401520' AS UNSIGNED))",
    );
  });

  it("converts copied MySQL point JSON into WKT insert literals", () => {
    const pointColumn = column("col_point", "point", "spatial");
    const operation = buildInsertRowOperation(
      driver,
      "rtest",
      "",
      "all_types",
      {
        col_point: '{"x":10.5,"y":20.3}',
      },
      [pointColumn],
    );

    expect(operation.sql).toBe(
      "INSERT INTO `rtest`.`all_types` (`col_point`) VALUES (ST_GeomFromText(?))",
    );
    expect(operation.params).toEqual(["POINT(10.5 20.3)"]);
    expect(driver.materializePreviewSql(operation.sql, operation.params)).toBe(
      "INSERT INTO `rtest`.`all_types` (`col_point`) VALUES (ST_GeomFromText('POINT(10.5 20.3)'))",
    );
  });

  it("converts copied MySQL polygon JSON into WKT insert literals", () => {
    const polygonColumn = column("col_polygon", "polygon", "spatial");
    const operation = buildInsertRowOperation(
      driver,
      "rtest",
      "",
      "all_types",
      {
        col_polygon:
          '[[{"x":0,"y":0},{"x":4,"y":0},{"x":4,"y":4},{"x":0,"y":4},{"x":0,"y":0}]]',
      },
      [polygonColumn],
    );

    expect(operation.sql).toBe(
      "INSERT INTO `rtest`.`all_types` (`col_polygon`) VALUES (ST_GeomFromText(?))",
    );
    expect(operation.params).toEqual(["POLYGON((0 0, 4 0, 4 4, 0 4, 0 0))"]);
    expect(driver.materializePreviewSql(operation.sql, operation.params)).toBe(
      "INSERT INTO `rtest`.`all_types` (`col_polygon`) VALUES (ST_GeomFromText('POLYGON((0 0, 4 0, 4 4, 0 4, 0 0))'))",
    );
  });
});

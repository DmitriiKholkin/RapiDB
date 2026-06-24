import { describe, expect, it } from "vitest";
import { MSSQLDriver } from "../../src/extension/dbDrivers/mssql";
import { MySQLDriver } from "../../src/extension/dbDrivers/mysql";
import { OracleDriver } from "../../src/extension/dbDrivers/oracle";
import type { ColumnTypeMeta } from "../../src/extension/dbDrivers/types";
import type { ConnectionConfig } from "../../src/shared/connectionConfig";

const baseConfig = {
  id: "float-precision",
  name: "Float precision",
  host: "127.0.0.1",
  username: "user",
  password: "secret",
} as ConnectionConfig;

const floatColumn: ColumnTypeMeta = {
  name: "v",
  type: "FLOAT",
  nativeType: "FLOAT",
  category: "float",
  nullable: true,
  isPrimaryKey: false,
  isForeignKey: false,
  filterable: true,
  filterOperators: ["eq", "neq"],
  valueSemantics: "plain",
};

const realColumn: ColumnTypeMeta = {
  name: "v",
  type: "Real",
  nativeType: "Real",
  category: "float",
  nullable: true,
  isPrimaryKey: false,
  isForeignKey: false,
  filterable: true,
  filterOperators: ["eq", "neq"],
  valueSemantics: "plain",
};

const binaryDoubleColumn: ColumnTypeMeta = {
  name: "v",
  type: "BINARY_DOUBLE",
  nativeType: "BINARY_DOUBLE",
  category: "float",
  nullable: true,
  isPrimaryKey: false,
  isForeignKey: false,
  filterable: true,
  filterOperators: ["eq", "neq"],
  valueSemantics: "plain",
};

const binaryFloatColumn: ColumnTypeMeta = {
  name: "v",
  type: "BINARY_FLOAT",
  nativeType: "BINARY_FLOAT",
  category: "float",
  nullable: true,
  isPrimaryKey: false,
  isForeignKey: false,
  filterable: true,
  filterOperators: ["eq", "neq"],
  valueSemantics: "plain",
};

describe("Float column precision preservation", () => {
  it("MySQL preserves the full precision of a FLOAT value", () => {
    const driver = new MySQLDriver({
      ...baseConfig,
      type: "mysql",
      port: 3306,
      database: "test",
    } as ConnectionConfig);
    // For whole numbers, `13000.0` and `13000` are the same JS Number;
    // the test asserts the value is passed through without truncation.
    expect(driver.formatOutputValue(13000.0, floatColumn)).toBe(13000);
    expect(driver.formatOutputValue(0.123456789, floatColumn)).toBe(
      0.123456789,
    );
  });

  it("MSSQL preserves the full precision of a Real value", () => {
    const driver = new MSSQLDriver({
      ...baseConfig,
      type: "mssql",
      port: 1433,
      database: "master",
    } as ConnectionConfig);
    expect(driver.formatOutputValue(13000.0, realColumn)).toBe(13000);
    expect(driver.formatOutputValue(0.123456789, realColumn)).toBe(0.123456789);
  });

  it("Oracle BINARY_DOUBLE preserves the full precision", () => {
    const driver = new OracleDriver({
      ...baseConfig,
      type: "oracle",
      port: 1521,
      serviceName: "FREEPDB1",
    } as ConnectionConfig);
    expect(
      driver.formatOutputValue(
        Number.parseFloat("3.141592653589793"),
        binaryDoubleColumn,
      ),
    ).toBe("3.141592653589793");
  });

  it("Oracle BINARY_FLOAT preserves the value as a string", () => {
    const driver = new OracleDriver({
      ...baseConfig,
      type: "oracle",
      port: 1521,
      serviceName: "FREEPDB1",
    } as ConnectionConfig);
    expect(driver.formatOutputValue(13000.0, binaryFloatColumn)).toBe("13000");
  });
});

import { describe, expect, it } from "vitest";
import { MSSQLDriver } from "../../src/extension/dbDrivers/mssql";
import type { ColumnTypeMeta } from "../../src/extension/dbDrivers/types";
import { buildUpdateRowSql } from "../../src/extension/table/updateSql";
import type { ConnectionConfig } from "../../src/shared/connectionConfig";

function makeDriver(): MSSQLDriver {
  return new MSSQLDriver({
    id: "mssql-language-neutral-datetime",
    name: "MSSQL Language Neutral Datetime",
    type: "mssql",
    host: "127.0.0.1",
    port: 1433,
    database: "master",
    username: "sa",
    password: "secret",
  } as ConnectionConfig);
}

function column(
  name: string,
  nativeType: string,
  category: ColumnTypeMeta["category"],
  isPrimaryKey = false,
): ColumnTypeMeta {
  return {
    name,
    type: nativeType,
    nativeType,
    category,
    nullable: true,
    isPrimaryKey,
    isForeignKey: false,
    filterable: true,
    filterOperators: [
      "eq",
      "neq",
      "gt",
      "gte",
      "lt",
      "lte",
      "in",
      "between",
      "like",
    ],
    valueSemantics: "plain",
  };
}

const datetimeColumn = column("created_at", "datetime", "datetime");
const smalldatetimeColumn = column("seen_at", "smalldatetime", "datetime");
const datetime2Column = column("updated_at", "datetime2(7)", "datetime");

describe("MSSQL language-neutral datetime literals", () => {
  it("binds datetime and smalldatetime input in ISO 8601 form", () => {
    const driver = makeDriver();

    expect(
      driver.coerceInputValue("2024-01-02 10:00:00.123", datetimeColumn),
    ).toBe("2024-01-02T10:00:00.123");
    expect(driver.coerceInputValue("2024-01-02", datetimeColumn)).toBe(
      "2024-01-02T00:00:00",
    );
    expect(
      driver.coerceInputValue("2024-01-13 10:00:00", smalldatetimeColumn),
    ).toBe("2024-01-13T10:00:00");
    expect(driver.coerceInputValue("2024-01-02T10:00:00", datetimeColumn)).toBe(
      "2024-01-02T10:00:00",
    );
  });

  it("keeps datetime2 input unchanged because its format is already language-neutral", () => {
    const driver = makeDriver();

    expect(
      driver.coerceInputValue("2024-01-02 10:00:00.1234567", datetime2Column),
    ).toBe("2024-01-02 10:00:00.1234567");
  });

  it("uses ISO 8601 literals in datetime filters", () => {
    const driver = makeDriver();

    expect(
      driver.buildFilterCondition(
        datetimeColumn,
        "eq",
        "2024-01-02 10:00:00",
        1,
      ),
    ).toEqual({
      sql: "[created_at] = CAST(? AS datetime)",
      params: ["2024-01-02T10:00:00"],
    });
    expect(
      driver.buildFilterCondition(
        smalldatetimeColumn,
        "gt",
        "2024-01-02 10:00:00",
        1,
      ),
    ).toEqual({
      sql: "[seen_at] > ?",
      params: ["2024-01-02T10:00:00"],
    });
    expect(
      driver.buildFilterCondition(
        datetimeColumn,
        "between",
        ["2024-01-02", "2024-01-13 23:59:59.997"],
        1,
      ),
    ).toEqual({
      sql: "[created_at] BETWEEN ? AND ?",
      params: ["2024-01-02T00:00:00", "2024-01-13T23:59:59.997"],
    });
    expect(
      driver.buildFilterCondition(
        datetimeColumn,
        "in",
        "2024-01-02 10:00:00, 2024-01-13 10:00:00",
        1,
      ),
    ).toEqual({
      sql: "[created_at] IN (?, ?)",
      params: ["2024-01-02T10:00:00", "2024-01-13T10:00:00"],
    });
  });

  it("uses ISO 8601 literals in the datetime export cursor", () => {
    const driver = makeDriver();

    expect(
      driver.buildCursorComparison(
        datetimeColumn,
        "gt",
        "2024-01-02 10:00:00.000",
        1,
      ),
    ).toEqual({
      sql: "[created_at] > CAST(? AS datetime)",
      params: ["2024-01-02T10:00:00.000"],
    });
  });

  it("uses ISO 8601 literals for datetime update values and original-value checks", () => {
    const driver = makeDriver();
    const id = column("id", "int", "integer", true);

    const update = buildUpdateRowSql(
      driver,
      "master",
      "dbo",
      "events",
      { id: "1" },
      { created_at: "2024-01-13 10:00:00" },
      [id, datetimeColumn],
      { created_at: "2024-01-02 10:00:00.123" },
    );

    expect(update?.params).toEqual([
      "2024-01-13T10:00:00",
      1,
      "2024-01-02T10:00:00.123",
    ]);
  });

  it("uses ISO 8601 literals for sql_variant datetime values only", () => {
    const driver = makeDriver();
    const variant = column("v", "sql_variant", "other");
    const snapshot = (baseType: string, maxLength: number | null) => ({
      __rapidbSqlVariant: 1,
      displayValue: "2024-01-02 10:00:00.000",
      baseType,
      precision: null,
      scale: null,
      maxLength,
      collation: baseType === "nvarchar" ? "Latin1_General_CI_AS" : null,
      binaryHex: "00",
    });

    expect(
      driver.buildMutationValueExpr(
        variant,
        "2024-01-13 10:00:00",
        "update",
        snapshot("datetime", null),
      ),
    ).toEqual({
      sql: "CAST(? AS datetime)",
      params: ["2024-01-13T10:00:00"],
    });
    expect(
      driver.buildMutationValueExpr(
        variant,
        "2024-01-13 10:00:00",
        "update",
        snapshot("nvarchar", 200),
      )?.params,
    ).toEqual(["2024-01-13 10:00:00"]);
    expect(
      driver.buildMutationValueExpr(
        variant,
        " 2024-01-13 9:30 ",
        "update",
        snapshot("smalldatetime", null),
      ),
    ).toEqual({
      sql: "CAST(? AS smalldatetime)",
      params: ["2024-01-13T09:30:00"],
    });
  });

  it("normalizes short clock forms before building legacy datetime literals", () => {
    const driver = makeDriver();

    expect(driver.coerceInputValue("2024-01-13 10:00", datetimeColumn)).toBe(
      "2024-01-13T10:00:00",
    );
    expect(
      driver.coerceInputValue("2024-01-13 9:00:00", smalldatetimeColumn),
    ).toBe("2024-01-13T09:00:00");
    expect(
      driver.buildFilterCondition(datetimeColumn, "eq", "2024-01-02 10:00", 1),
    ).toEqual({
      sql: "[created_at] = CAST(? AS datetime)",
      params: ["2024-01-02T10:00:00"],
    });
    expect(
      driver.buildFilterCondition(
        smalldatetimeColumn,
        "gt",
        "2024-01-02 9:00",
        1,
      ),
    ).toEqual({
      sql: "[seen_at] > ?",
      params: ["2024-01-02T09:00:00"],
    });
    expect(
      driver.buildFilterCondition(
        datetimeColumn,
        "between",
        ["2024-01-02 9:00", "2024-01-13 23:59"],
        1,
      ),
    ).toEqual({
      sql: "[created_at] BETWEEN ? AND ?",
      params: ["2024-01-02T09:00:00", "2024-01-13T23:59:00"],
    });
  });

  it("keeps date-only filters on datetime2 and datetimeoffset unchanged", () => {
    const driver = makeDriver();
    const datetimeoffsetColumn = column(
      "logged_at",
      "datetimeoffset(7)",
      "datetime",
    );

    expect(
      driver.buildFilterCondition(datetime2Column, "gt", "2024-01-02", 1),
    ).toEqual({
      sql: "[updated_at] > ?",
      params: ["2024-01-02"],
    });
    expect(
      driver.buildFilterCondition(
        datetimeoffsetColumn,
        "between",
        ["2024-01-02", "2024-01-13"],
        1,
      ),
    ).toEqual({
      sql: "[logged_at] BETWEEN ? AND ?",
      params: ["2024-01-02", "2024-01-13"],
    });
  });
});

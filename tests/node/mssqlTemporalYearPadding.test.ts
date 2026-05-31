import { describe, expect, it } from "vitest";
import { MSSQLDriver } from "../../src/extension/dbDrivers/mssql";
import type { ColumnTypeMeta } from "../../src/extension/dbDrivers/types";
import type { ConnectionConfig } from "../../src/shared/connectionConfig";

function makeDriver(): MSSQLDriver {
  return new MSSQLDriver({
    id: "mssql-temporal-year-padding",
    name: "MSSQL Temporal Year Padding",
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
): ColumnTypeMeta {
  return {
    name,
    type: nativeType,
    nativeType,
    category,
    nullable: true,
    isPrimaryKey: false,
    isForeignKey: false,
    filterable: true,
    filterOperators: ["eq", "is_null", "is_not_null"],
    valueSemantics: "plain",
  };
}

describe("MSSQL temporal year padding", () => {
  it("normalizes date input year to 4 digits", () => {
    const driver = makeDriver();
    const dateColumn = column("created_on", "date", "date");

    expect(driver.coerceInputValue("1-01-01", dateColumn)).toBe("0001-01-01");
  });

  it("normalizes datetime2 input year to 4 digits", () => {
    const driver = makeDriver();
    const datetime2Column = column("created_at", "datetime2(7)", "datetime");

    expect(
      driver.coerceInputValue("1-01-01 00:00:00.0000000", datetime2Column),
    ).toBe("0001-01-01 00:00:00.0000000");
  });

  it("binds time strings without Date timezone conversion", () => {
    const driver = makeDriver() as unknown as {
      bindPositionalParameters: (
        request: {
          input: (name: string, type: unknown, value: unknown) => void;
        },
        sql: string,
        params: readonly unknown[],
      ) => string;
    };
    const recorded: Array<{ typeName: string; value: unknown }> = [];
    const fakeRequest = {
      input(_name: string, type: unknown, value: unknown): void {
        const typeName =
          typeof type === "function"
            ? type.name
            : typeof (type as { type?: unknown }).type === "function"
              ? ((type as { type: { name: string } }).type.name ?? "")
              : "";
        recorded.push({ typeName, value });
      },
    };

    const sql = driver.bindPositionalParameters(
      fakeRequest,
      "SELECT 1 WHERE CAST(? AS time) IS NOT NULL",
      ["00:00:00.0000000"],
    );

    expect(sql).toBe("SELECT 1 WHERE CAST(@p1 AS time) IS NOT NULL");
    expect(recorded).toHaveLength(1);
    expect(recorded[0].typeName).toBe("NVarChar");
    expect(recorded[0].value).toBe("00:00:00.0000000");
  });

  it("binds datetimeoffset strings as text to preserve wall-clock values", () => {
    const driver = makeDriver() as unknown as {
      bindPositionalParameters: (
        request: {
          input: (name: string, type: unknown, value: unknown) => void;
        },
        sql: string,
        params: readonly unknown[],
      ) => string;
    };
    const recorded: Array<{ typeName: string; value: unknown }> = [];
    const fakeRequest = {
      input(_name: string, type: unknown, value: unknown): void {
        const typeName =
          typeof type === "function"
            ? type.name
            : typeof (type as { type?: unknown }).type === "function"
              ? ((type as { type: { name: string } }).type.name ?? "")
              : "";
        recorded.push({ typeName, value });
      },
    };

    const sql = driver.bindPositionalParameters(
      fakeRequest,
      "SELECT 1 WHERE CAST(? AS datetimeoffset(7)) IS NOT NULL",
      ["2024-07-04 06:34:56.1230000+02:00"],
    );

    expect(sql).toBe(
      "SELECT 1 WHERE CAST(@p1 AS datetimeoffset(7)) IS NOT NULL",
    );
    expect(recorded).toHaveLength(1);
    expect(recorded[0].typeName).toBe("NVarChar");
    expect(recorded[0].value).toBe("2024-07-04 06:34:56.1230000+02:00");
  });

  it("confirms persisted datetime values when read back as Date", () => {
    const driver = makeDriver();
    const datetimeColumn = column("col_datetime", "datetime", "datetime");
    const persistedDate = new Date(0);
    persistedDate.setFullYear(1753, 0, 1);
    persistedDate.setHours(0, 0, 0, 0);

    const check = driver.checkPersistedEdit(
      datetimeColumn,
      "1753-01-01 00:00:00",
      {
        persistedValue: persistedDate,
      },
    );

    expect(check).toEqual({ ok: true, shouldVerify: true });
  });

  it("confirms persisted smalldatetime values when read back as Date", () => {
    const driver = makeDriver();
    const smalldatetimeColumn = column(
      "col_smalldatetime",
      "smalldatetime",
      "datetime",
    );
    const persistedDate = new Date(0);
    persistedDate.setFullYear(2024, 0, 1);
    persistedDate.setHours(13, 0, 0, 0);

    const check = driver.checkPersistedEdit(
      smalldatetimeColumn,
      "2024-01-01 13:00:00",
      {
        persistedValue: persistedDate,
      },
    );

    expect(check).toEqual({ ok: true, shouldVerify: true });
  });

  it("normalizes datetimeoffset spacing during persisted verification", () => {
    const driver = makeDriver();
    const datetimeOffsetColumn = column(
      "col_datetimeoffset",
      "datetimeoffset(7)",
      "datetime",
    );

    const check = driver.checkPersistedEdit(
      datetimeOffsetColumn,
      "2024-07-04 09:34:56.1230000 +02:00",
      {
        persistedValue: "2024-07-04 09:34:56.1230000+02:00",
      },
    );

    expect(check).toEqual({ ok: true, shouldVerify: true });
  });
});

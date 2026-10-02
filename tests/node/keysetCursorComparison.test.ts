import { describe, expect, it, vi } from "vitest";
import { MSSQLDriver } from "../../src/extension/dbDrivers/mssql";
import { OracleDriver } from "../../src/extension/dbDrivers/oracle";
import { PostgresDriver } from "../../src/extension/dbDrivers/postgres";
import type {
  ColumnTypeMeta,
  QueryResult,
} from "../../src/extension/dbDrivers/types";
import { TableReadService } from "../../src/extension/table/tableReadService";
import { TEST_CONNECTION_SEEDS } from "../contracts/testingContracts";

function column(
  nativeType: string,
  category: ColumnTypeMeta["category"] = "datetime",
  name = "ts",
): ColumnTypeMeta {
  return {
    name,
    type: nativeType,
    nativeType,
    category,
    nullable: false,
    isPrimaryKey: false,
    isForeignKey: false,
    filterable: true,
    filterOperators: ["eq", "gt", "lt"],
    valueSemantics: "plain",
  };
}

const operators = [
  ["eq", "="],
  ["gt", ">"],
  ["lt", "<"],
] as const;

describe("exact driver cursor predicates", () => {
  it.each(
    operators,
  )("MSSQL datetime %s casts the Date bind back to native ticks", (operator, sqlOperator) => {
    const driver = new MSSQLDriver(TEST_CONNECTION_SEEDS.mssql.connection);
    const raw = new Date(2024, 0, 15, 12, 0, 0, 3);
    const predicate = driver.buildCursorComparison(
      column("datetime", "datetime", "t]s"),
      operator,
      raw,
      7,
    );
    expect(predicate.sql).toBe(`[t]]s] ${sqlOperator} CAST(? AS datetime)`);
    expect(predicate.params).toEqual([raw]);
    expect(predicate.params[0]).toBe(raw);
  });

  it.each([
    "datetime2(7)",
    "datetimeoffset(7)",
    "time(7)",
  ])("MSSQL %s retains all raw fractional digits", (nativeType) => {
    const driver = new MSSQLDriver(TEST_CONNECTION_SEEDS.mssql.connection);
    const raw =
      nativeType === "time(7)"
        ? "12:00:00.1234567"
        : `2024-07-15T12:00:00.1234567${nativeType === "datetimeoffset(7)" ? "+02:00" : ""}`;
    expect(
      driver.buildCursorComparison(
        column(nativeType, nativeType === "time(7)" ? "time" : "datetime"),
        "eq",
        raw,
        7,
      ),
    ).toEqual({
      sql: "[ts] = ?",
      params: [raw],
    });
  });

  for (const nativeType of [
    "TIMESTAMP(9) WITH TIME ZONE",
    "TIMESTAMP(9) WITH LOCAL TIME ZONE",
  ]) {
    it.each(
      operators,
    )(`Oracle ${nativeType} %s binds precise UTC text without Date coercion`, (operator, sqlOperator) => {
      const driver = new OracleDriver(TEST_CONNECTION_SEEDS.oracle.connection);
      for (const raw of [
        "2024-01-15 11:00:00.123456789",
        "2024-07-15 10:00:00.123456789",
      ]) {
        const predicate = driver.buildCursorComparison(
          column(nativeType, "datetime", 't"s'),
          operator,
          raw,
          7,
        );
        expect(predicate).toEqual({
          sql: `SYS_EXTRACT_UTC(CAST("t""s" AS TIMESTAMP(9) WITH TIME ZONE)) ${sqlOperator} TO_TIMESTAMP(:7, 'YYYY-MM-DD HH24:MI:SS.FF')`,
          params: [raw],
        });
      }
    });
  }

  it.each(
    operators,
  )("Oracle plain timestamp %s retains wall-clock nanoseconds", (operator, sqlOperator) => {
    const driver = new OracleDriver(TEST_CONNECTION_SEEDS.oracle.connection);
    const raw = "2024-07-15 12:00:00.123456789";
    expect(
      driver.buildCursorComparison(column("TIMESTAMP(9)"), operator, raw, 4),
    ).toEqual({
      sql: `"ts" ${sqlOperator} TO_TIMESTAMP(:4, 'YYYY-MM-DD HH24:MI:SS.FF')`,
      params: [raw],
    });
  });

  it("Oracle DATE accepts raw date-only output without session NLS coercion", () => {
    const driver = new OracleDriver(TEST_CONNECTION_SEEDS.oracle.connection);
    expect(
      driver.buildCursorComparison(
        column("DATE", "date"),
        "gt",
        "2024-01-15",
        2,
      ),
    ).toEqual({
      sql: `"ts" > TO_DATE(:2, 'YYYY-MM-DD HH24:MI:SS')`,
      params: ["2024-01-15 00:00:00"],
    });
  });

  it.each(
    operators,
  )("float cursor %s is exact, despite approximate user filter equality", (operator, sqlOperator) => {
    for (const driver of [
      new PostgresDriver(TEST_CONNECTION_SEEDS.postgres.connection),
      new MSSQLDriver(TEST_CONNECTION_SEEDS.mssql.connection),
      new OracleDriver(TEST_CONNECTION_SEEDS.oracle.connection),
    ]) {
      const meta = column(
        driver instanceof OracleDriver
          ? "BINARY_DOUBLE"
          : driver instanceof MSSQLDriver
            ? "float"
            : "float8",
        "float",
        "f",
      );
      const raw = 1 + Number.EPSILON;
      const placeholder = driver.buildInsertValueExpr(meta, 3);
      expect(driver.buildCursorComparison(meta, operator, raw, 3)).toEqual({
        sql: `${driver.quoteIdentifier("f")} ${sqlOperator} ${placeholder}`,
        params: [raw],
      });
      expect(
        driver.buildFilterCondition(meta, "eq", String(raw), 3)?.sql,
      ).toMatch(/ABS/i);
    }
  });
});

function result(names: string[], values: unknown[][]): QueryResult {
  return {
    columns: names,
    rows: values.map((row) =>
      Object.fromEntries(row.map((value, i) => [`__col_${i}`, value])),
    ),
    rowCount: values.length,
    executionTimeMs: 0,
  };
}

describe("TableReadService cursor integration", () => {
  it.each([
    "mssql",
    "oracle",
  ] as const)("%s uses the native cursor hook with raw values and user filter binds", async (engine) => {
    const config = TEST_CONNECTION_SEEDS[engine].connection;
    const driver =
      engine === "mssql" ? new MSSQLDriver(config) : new OracleDriver(config);
    const raw =
      engine === "mssql"
        ? new Date(2024, 0, 15, 12, 0, 0, 3)
        : "2024-07-15 10:00:00.123456789";
    const meta = [
      column(engine === "mssql" ? "datetime" : "TIMESTAMP(9) WITH TIME ZONE"),
      {
        ...column(engine === "mssql" ? "int" : "NUMBER(10)", "integer", "id"),
        isPrimaryKey: true,
      },
      column(engine === "mssql" ? "int" : "NUMBER(10)", "integer", "bucket"),
    ];
    vi.spyOn(driver, "describeColumns").mockResolvedValue(meta);
    vi.spyOn(driver, "formatOutputValue").mockImplementation((value, col) =>
      col.name === "ts" ? "lossy display" : value,
    );
    const query = vi
      .spyOn(driver, "query")
      .mockResolvedValueOnce(result(["ts", "id", "bucket"], [[raw, 2, 1]]))
      .mockResolvedValueOnce(result(["ts", "id", "bucket"], []));
    const hook = vi.spyOn(driver, "buildCursorComparison");
    const filter = vi.spyOn(driver, "buildFilterCondition");
    const service = new TableReadService({
      getDriver: () => driver,
      getConnection: () => config,
    } as never);
    const rows: Record<string, unknown>[] = [];
    for await (const chunk of service.exportAll(
      config.id,
      "db",
      "schema",
      "items",
      1,
      { column: "ts", direction: "desc" },
      [{ column: "bucket", operator: "eq", value: "1" }],
    ))
      rows.push(...chunk.rows);
    expect(rows).toEqual([{ ts: "lossy display", id: 2, bucket: 1 }]);
    expect(hook.mock.calls).toEqual([
      [meta[0], "lt", raw, 2],
      [meta[0], "eq", raw, 3],
      [meta[1], "gt", 2, 4],
    ]);
    expect(filter.mock.calls.every(([col]) => col.name === "bucket")).toBe(
      true,
    );
    const params = query.mock.calls[1][1] ?? [];
    expect(params.slice(1, 4)).toEqual([raw, raw, 2]);
    expect(params[0]).toEqual(query.mock.calls[0][1]?.[0]);
    expect(query.mock.calls[1][0]).toContain(
      engine === "mssql" ? "CAST(? AS datetime)" : "SYS_EXTRACT_UTC",
    );
  });

  it("keeps decimal/bigint composite PK precision and indexed filter/pagination binds", async () => {
    const config = TEST_CONNECTION_SEEDS.postgres.connection;
    const driver = new PostgresDriver(config);
    const meta = [
      {
        ...column("bigint", "integer", "id"),
        isPrimaryKey: true,
        primaryKeyOrdinal: 2,
      },
      {
        ...column("numeric(30,10)", "decimal", "amount"),
        isPrimaryKey: true,
        primaryKeyOrdinal: 1,
      },
      column("integer", "integer", "bucket"),
    ];
    const id = "9007199254740993";
    const amount = "12345678901234567890.1234567891";
    vi.spyOn(driver, "describeColumns").mockResolvedValue(meta);
    const query = vi
      .spyOn(driver, "query")
      .mockResolvedValueOnce(
        result(["id", "amount", "bucket"], [[id, amount, 1]]),
      )
      .mockResolvedValueOnce(result(["id", "amount", "bucket"], []));
    const hook = vi.spyOn(driver, "buildCursorComparison");
    const service = new TableReadService({
      getDriver: () => driver,
      getConnection: () => config,
    } as never);
    for await (const _chunk of service.exportAll(
      config.id,
      "db",
      "schema",
      "items",
      1,
      null,
      [{ column: "bucket", operator: "eq", value: "1" }],
    )) {
      /* exhaust */
    }
    expect(hook.mock.calls).toEqual([
      [meta[1], "gt", amount, 2],
      [meta[1], "eq", amount, 3],
      [meta[0], "gt", id, 4],
    ]);
    expect(query.mock.calls[1][0]).toContain('ORDER BY "amount" ASC, "id" ASC');
    expect(query.mock.calls[1][0]).toContain('"amount" = $3');
    expect(query.mock.calls[1][0]).toContain('"id" > $4');
    expect(query.mock.calls[1][0]).toContain("LIMIT $5 OFFSET $6");
    expect(query.mock.calls[1][1]?.slice(1)).toEqual([
      amount,
      amount,
      id,
      1,
      0,
    ]);
  });

  it.each([
    null,
    undefined,
  ])("fails closed on a missing non-null raw cursor %s", async (raw) => {
    const config = TEST_CONNECTION_SEEDS.postgres.connection;
    const driver = new PostgresDriver(config);
    vi.spyOn(driver, "describeColumns").mockResolvedValue([
      { ...column("int", "integer", "id"), isPrimaryKey: true },
    ]);
    const query = vi
      .spyOn(driver, "query")
      .mockResolvedValue(result(["id"], [[raw]]));
    const service = new TableReadService({
      getDriver: () => driver,
      getConnection: () => config,
    } as never);
    const iterator = service.exportAll(config.id, "db", "schema", "items", 1);
    await iterator.next();
    await expect(iterator.next()).rejects.toThrow(
      "Export cursor is missing a non-null value for id",
    );
    expect(query).toHaveBeenCalledTimes(1);
  });

  it("fails closed before yielding a repeated raw cursor", async () => {
    const config = TEST_CONNECTION_SEEDS.postgres.connection;
    const driver = new PostgresDriver(config);
    vi.spyOn(driver, "describeColumns").mockResolvedValue([
      { ...column("int", "integer", "id"), isPrimaryKey: true },
    ]);
    const query = vi
      .spyOn(driver, "query")
      .mockResolvedValue(result(["id"], [[1]]));
    const service = new TableReadService({
      getDriver: () => driver,
      getConnection: () => config,
    } as never);
    const iterator = service.exportAll(config.id, "db", "schema", "items", 1);
    await expect(iterator.next()).resolves.toMatchObject({
      value: { rows: [{ id: 1 }] },
      done: false,
    });
    await expect(iterator.next()).rejects.toThrow(
      "Export cursor did not advance",
    );
    expect(query).toHaveBeenCalledTimes(2);
  });
});

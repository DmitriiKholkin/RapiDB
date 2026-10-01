import { afterEach, describe, expect, it } from "vitest";
import { SQLiteDriver } from "../../../src/extension/dbDrivers/sqlite";
import { SQLiteCoreDriver } from "../../../src/extension/dbDrivers/sqliteCore";
import type { FilterOperator } from "../../../src/extension/dbDrivers/types";
import { TableReadService } from "../../../src/extension/table/tableReadService";
import type { FilterExpression } from "../../../src/shared/tableTypes";

const drivers: SQLiteCoreDriver[] = [];
afterEach(async () => {
  await Promise.all(drivers.splice(0).map((driver) => driver.disconnect()));
});

async function setup(mode: "native" | "worker", tableSql: string) {
  const config = {
    id: "filter-regression",
    name: "Filter Regression",
    type: "sqlite" as const,
    filePath: ":memory:",
  };
  const driver =
    mode === "native" ? new SQLiteCoreDriver(config) : new SQLiteDriver(config);
  drivers.push(driver);
  await driver.connect();
  await driver.query(tableSql);
  const read = new TableReadService({
    getConnection: () => config,
    getDriver: () => driver,
  } as never);
  const page = (
    column: string,
    operator: FilterOperator,
    value?: string | [string, string],
  ) =>
    read.getPage(
      config.id,
      "",
      "main",
      "items",
      1,
      100,
      [{ column, operator, value } as FilterExpression],
      { column: "id", direction: "asc" },
    );
  const ids = async (
    column: string,
    operator: FilterOperator,
    value?: string | [string, string],
  ) => {
    const result = await page(column, operator, value);
    expect(result.totalCount).toBe(result.rows.length);
    return result.rows.map((row) => row.id);
  };
  return { driver, read, page, ids };
}

describe.each([
  "native",
  "worker",
] as const)("real %s SQLite filter regressions", (mode) => {
  it("distinguishes exact int64 values stored under declared DECIMAL in scalar, IN and range filters", async () => {
    const { driver, read, ids } = await setup(
      mode,
      `
      CREATE TABLE items (id INTEGER PRIMARY KEY, amount DECIMAL(20,0));
      INSERT INTO items VALUES
        (1, 9007199254740993), (2, 9007199254740992), (3, 9007199254740994),
        (4, -9007199254740993), (5, -9007199254740992), (6, -9007199254740994),
        (7, 9223372036854775807), (8, -9223372036854775808), (9, NULL);
    `,
    );
    expect(
      (await read.getColumns("filter-regression", "", "main", "items")).find(
        (column) => column.name === "amount",
      )?.category,
    ).toBe("decimal");
    const storage = await driver.query(
      "SELECT id, typeof(amount), CAST(amount AS TEXT) FROM items ORDER BY id",
    );
    expect(storage.rows.slice(0, 3)).toEqual([
      { __col_0: 1, __col_1: "integer", __col_2: "9007199254740993" },
      { __col_0: 2, __col_1: "integer", __col_2: "9007199254740992" },
      { __col_0: 3, __col_1: "integer", __col_2: "9007199254740994" },
    ]);
    for (const [value, id] of [
      ["9007199254740993", 1],
      ["9007199254740992", 2],
      ["9007199254740994", 3],
      ["-9007199254740993", 4],
      ["-9007199254740992", 5],
      ["-9007199254740994", 6],
      ["9223372036854775807", 7],
      ["-9223372036854775808", 8],
    ] as const) {
      expect(await ids("amount", "eq", value)).toEqual([id]);
      expect(await ids("amount", "neq", value)).toEqual(
        [1, 2, 3, 4, 5, 6, 7, 8].filter((other) => other !== id),
      );
    }
    expect(
      await ids("amount", "in", "9007199254740993, -9007199254740993"),
    ).toEqual([1, 4]);
    expect(await ids("amount", "gt", "9007199254740993")).toEqual([3, 7]);
    expect(await ids("amount", "gte", "9007199254740993")).toEqual([1, 3, 7]);
    expect(await ids("amount", "lt", "-9007199254740993")).toEqual([6, 8]);
    expect(await ids("amount", "lte", "-9007199254740993")).toEqual([4, 6, 8]);
    expect(
      await ids("amount", "between", ["9007199254740993", "9007199254740994"]),
    ).toEqual([1, 3]);
    expect(
      await ids("amount", "between", [
        "-9007199254740994",
        "-9007199254740993",
      ]),
    ).toEqual([4, 6]);
  });

  it("uses SQLite numeric affinity for ordinary, fractional and scientific literals, including storage rounding", async () => {
    const { driver, ids } = await setup(
      mode,
      `
      CREATE TABLE items (id INTEGER PRIMARY KEY, amount NUMERIC);
      INSERT INTO items VALUES (1, 42), (2, 0.125), (3, 125), (4, -0.5),
        (5, 9007199254740993.0), (6, 9.007199254740993e15),
        (7, 0.12345678901234567890), (8, 0.12345678901234567891);
    `,
    );
    for (const [value, expected] of [
      ["42", [1]],
      ["+42", [1]],
      ["42.0", [1]],
      ["4.2e1", [1]],
      [".125", [2]],
      ["1.25e-1", [2]],
      ["1.25E+2", [3]],
      ["-.5", [4]],
      ["9007199254740993", []],
      ["9007199254740992", [5, 6]],
      ["9007199254740993.0", [5, 6]],
      ["9.007199254740993e15", [5, 6]],
      ["0.12345678901234567890", [7, 8]],
    ] as const) {
      expect(await ids("amount", "eq", value)).toEqual(expected);
    }
    // DECIMAL/NUMERIC affinity is not arbitrary-precision decimal storage.
    expect(
      (
        await driver.query(
          "SELECT typeof(amount), CAST(amount AS TEXT) FROM items WHERE id IN (5, 6) ORDER BY id",
        )
      ).rows,
    ).toEqual([
      { __col_0: "integer", __col_1: "9007199254740992" },
      { __col_0: "integer", __col_1: "9007199254740992" },
    ]);
    expect(await ids("amount", "in", ".125, 1.25e2")).toEqual([2, 3]);
    expect(await ids("amount", "between", [".125", "42"])).toEqual([1, 2]);
  });

  it("rejects invalid decimal input before executing a filter", async () => {
    const { driver, page } = await setup(
      mode,
      "CREATE TABLE items (id INTEGER PRIMARY KEY, amount DECIMAL); INSERT INTO items VALUES (1, 1)",
    );
    for (const value of [
      "NaN",
      "Infinity",
      "-Infinity",
      "1e999",
      "0x10",
      "1 OR 1=1",
      "1); DROP TABLE items; --",
    ]) {
      for (const [operator, input] of [
        ["eq", value],
        ["neq", value],
        ["gt", value],
        ["in", `1, ${value}`],
        ["between", ["1", value]],
      ] as [FilterOperator, string | [string, string]][]) {
        await expect(page("amount", operator, input)).rejects.toThrow(
          /RapiDB Filter/,
        );
      }
    }
    expect(
      (await driver.query("SELECT count(*) FROM items")).rows[0].__col_0,
    ).toBe(1);
  });

  it.each([
    ["TIME", "12", "12:30:00", "12:30", "12:30:00"],
    [
      "DATETIME",
      "2026",
      "2026-10-01 12:30:00",
      "2026-10-01T12:30:00",
      "2026-10-01 14:30:00+02:00",
    ],
  ] as const)("compares raw %s values exactly and preserves valid normalization and null semantics", async (nativeType, raw, validStored, validInput, equivalentStored) => {
    const { driver, ids, page } = await setup(
      mode,
      `CREATE TABLE items (id INTEGER PRIMARY KEY, event ${nativeType})`,
    );
    const values = [
      raw,
      validStored,
      `prefix${raw}suffix`,
      "raw%_value",
      "rawXYvalue",
      "%",
      "_",
      "x' OR 1=1 --",
      null,
      equivalentStored,
      nativeType === "TIME" ? "13:00:00" : "2026-10-02 12:30:00",
    ];
    for (const [index, value] of values.entries()) {
      await driver.query("INSERT INTO items VALUES (?, ?)", [index + 1, value]);
    }
    for (const [value, id] of [
      [raw, 1],
      ["raw%_value", 4],
      ["%", 6],
      ["_", 7],
      ["x' OR 1=1 --", 8],
    ] as const) {
      expect(await ids("event", "eq", value)).toEqual([id]);
      expect(await ids("event", "neq", value)).toEqual(
        [1, 2, 3, 4, 5, 6, 7, 8, 10, 11].filter((other) => other !== id),
      );
    }
    expect(await ids("event", "eq", validInput)).toEqual([2, 10]);
    // Nonnumeric raw text yields NULL in temporal functions, but numeric raw
    // values (row 1) are stored as integers and interpreted as Julian days.
    expect(await ids("event", "neq", validInput)).toEqual([1, 11]);
    expect(await ids("event", "is_null")).toEqual([9]);
    expect(await ids("event", "is_not_null")).toEqual([
      1, 2, 3, 4, 5, 6, 7, 8, 10, 11,
    ]);
    for (const operator of ["gt", "gte", "lt", "lte"] as const) {
      await expect(page("event", operator, raw)).rejects.toThrow(
        /expects a valid/,
      );
    }
    await expect(page("event", "between", [validInput, raw])).rejects.toThrow(
      /expects a valid/,
    );
    await expect(page("event", "between", [raw, validInput])).rejects.toThrow(
      /expects a valid/,
    );
  });

  it("uses exact raw comparisons for impossible datetime dates and offsets and rejects ranges/lists", async () => {
    const { driver, ids, page } = await setup(
      mode,
      "CREATE TABLE items (id INTEGER PRIMARY KEY, event DATETIME)",
    );
    const invalidValues = [
      "2026-13-01 12:34:56",
      "2026-13-01T12:34:56",
      "2026-00-01 12:34:56",
      "2026-10-00 12:34:56",
      "2026-10-32 12:34:56",
      "2026-04-31 12:34:56",
      "2026-02-29 12:34:56",
      "2024-02-30 12:34:56",
      "1900-02-29 12:34:56",
      "2100-02-29 12:34:56",
      "2026-10-01 12:34:56+99:99",
      "2026-10-01T12:34:56 +99:99",
      "2026-10-01 12:34:56-99:99",
      "2026-10-01 12:34:56+15:00",
      "2026-10-01 12:34:56-15:00",
      "2026-10-01 12:34:56+00:60",
      "2026-10-01 12:34:56-14:60",
    ];
    const valid = "2026-10-01 12:34:56";
    const values = [
      ...invalidValues,
      valid,
      // SQLite rolls impossible calendar dates into these valid dates.
      // Raw equality must not match their normalized counterparts.
      "2026-05-01 12:34:56",
      "2026-03-01 12:34:56",
      "2024-03-01 12:34:56",
      "1900-03-01 12:34:56",
      "2100-03-01 12:34:56",
      null,
    ];
    for (const [index, value] of values.entries()) {
      await driver.query("INSERT INTO items VALUES (?, ?)", [index + 1, value]);
    }
    const nonNullIds = values.slice(0, -1).map((_, index) => index + 1);
    for (const [index, value] of invalidValues.entries()) {
      expect(await ids("event", "eq", value)).toEqual([index + 1]);
      expect(await ids("event", "neq", value)).toEqual(
        nonNullIds.filter((id) => id !== index + 1),
      );
      for (const [operator, input] of [
        ["gt", value],
        ["gte", value],
        ["lt", value],
        ["lte", value],
        ["between", [valid, value]],
        ["between", [value, valid]],
        ["in", `${valid}, ${value}`],
      ] as [FilterOperator, string | [string, string]][]) {
        await expect(page("event", operator, input)).rejects.toThrow(
          /expects a valid datetime/,
        );
      }
    }
  });

  it("normalizes supported datetime date boundaries, leap years and SQLite timezone offsets", async () => {
    const { driver, ids } = await setup(
      mode,
      "CREATE TABLE items (id INTEGER PRIMARY KEY, event DATETIME)",
    );
    const cases = [
      ["0000-02-29 00:00", "0000-02-29 00:00:00"],
      ["0001-01-01 00:00:00", "0001-01-01 00:00:00"],
      ["1900-02-28 23:59:59", "1900-02-28 23:59:59"],
      ["2000-02-29 12:34:56", "2000-02-29 12:34:56"],
      ["2024-02-29T12:34:56.123Z", "2024-02-29 12:34:56"],
      ["2026-04-30 12:34:56", "2026-04-30 12:34:56"],
      ["9999-12-31 23:59:59", "9999-12-31 23:59:59"],
      ["2026-10-01T12:34z", "2026-10-01 12:34:00"],
      ["2026-10-01 12:34:56 Z", "2026-10-01 12:34:56"],
      ["2026-10-01T12:34:56 +02:00", "2026-10-01 10:34:56"],
      ["2026-10-01 12:34:56-00:00", "2026-10-01 12:34:56"],
      ["2026-10-01 12:34:56+14:59", "2026-09-30 21:35:56"],
      ["2026-10-01 12:34:56-14:59", "2026-10-02 03:33:56"],
    ];
    for (const [index, [value, canonical]] of cases.entries()) {
      await driver.query("DELETE FROM items");
      // DATETIME intentionally compares at SQLite's whole-second precision.
      await driver.query("INSERT INTO items VALUES (?, ?)", [
        index + 1,
        canonical,
      ]);
      expect(
        (await driver.query("SELECT DATETIME(?)", [value])).rows[0].__col_0,
      ).toBe(canonical);
      expect(await ids("event", "eq", value)).toEqual([index + 1]);
      expect(await ids("event", "neq", value)).toEqual([]);
      expect(await ids("event", "between", [value, value])).toEqual([
        index + 1,
      ]);
      expect(await ids("event", "in", value)).toEqual([index + 1]);
    }
  });

  it.each([
    ["TIME", "10:00", "12:30", "14:00", "15:00", "13:00"],
    [
      "DATETIME",
      "2026-10-01T10:00Z",
      "2026-10-01 14:30:00 +02:00",
      "2026-10-01T09:00-05:00",
      "2026-10-01 15:00",
      "2026-10-01T13:00Z",
    ],
  ] as const)("executes valid %s ranges, BETWEEN, IN and Contains", async (nativeType, first, middle, last, later, upper) => {
    const { driver, ids } = await setup(
      mode,
      `CREATE TABLE items (id INTEGER PRIMARY KEY, event ${nativeType})`,
    );
    const values = [
      first,
      middle,
      last,
      later,
      "raw%_value",
      "rawXYvalue",
      null,
    ];
    for (const [index, value] of values.entries()) {
      await driver.query("INSERT INTO items VALUES (?, ?)", [index + 1, value]);
    }
    expect(await ids("event", "gt", middle)).toEqual([3, 4]);
    expect(await ids("event", "gte", middle)).toEqual([2, 3, 4]);
    expect(await ids("event", "lt", middle)).toEqual([1]);
    expect(await ids("event", "lte", middle)).toEqual([1, 2]);
    expect(await ids("event", "between", [first, upper])).toEqual([1, 2]);
    expect(await ids("event", "in", `${first}, ${last}`)).toEqual([1, 3]);
    const eventColumn = (
      await driver.describeColumns("", "main", "items")
    ).find((column) => column.name === "event");
    if (!eventColumn) throw new Error("Expected event column");
    // Contains is not exposed in temporal metadata; exercise its existing
    // builder/execution path directly without changing the UI operator list.
    const contains = driver.buildFilterCondition(
      eventColumn,
      "like",
      "raw%_value",
      1,
    );
    if (!contains) throw new Error("Expected Contains condition");
    expect(
      (
        await driver.query(
          `SELECT id FROM items WHERE ${contains.sql} ORDER BY id`,
          contains.params,
        )
      ).rows.map((row) => row.__col_0),
    ).toEqual([5, 6]);
    expect(await ids("event", "eq", "raw%_value")).toEqual([5]);
  });
});

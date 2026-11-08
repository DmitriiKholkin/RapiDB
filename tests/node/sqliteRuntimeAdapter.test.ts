import { access, mkdir, readFile, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { SQLiteDriver } from "../../src/extension/dbDrivers/sqlite";
import {
  formatSQLiteRuntimeLoadErrorMessage,
  resolveBetterSqlite3LoadTargets,
  resolveSQLiteRuntimeTarget,
} from "../../src/extension/dbDrivers/sqliteRuntime";
import {
  configureSQLiteInstaller,
  ensureSQLiteRuntimeInstalled,
  probeInstalledBetterSqlite3Runtime,
  resetSQLiteInstallerForTests,
} from "../../src/extension/utils/sqliteInstaller";
import type { ConnectionConfig } from "../../src/shared/connectionConfig";
import type { ColumnTypeMeta } from "../../src/shared/tableTypes";
import { createProjectTempDir } from "../runtime/tempDirectories.ts";

const openDrivers = new Set<SQLiteDriver>();

afterEach(async () => {
  for (const driver of openDrivers) {
    await driver.disconnect();
  }
  openDrivers.clear();
  resetSQLiteInstallerForTests();
});

async function createManagedRuntimeFixture(prefix: string): Promise<string> {
  const rootDir = await createProjectTempDir(prefix);
  const betterSqlite3Root = join(rootDir, "node_modules", "better-sqlite3");
  const bindingsRoot = join(rootDir, "node_modules", "bindings");
  const fileUriToPathRoot = join(rootDir, "node_modules", "file-uri-to-path");
  await mkdir(betterSqlite3Root, { recursive: true });
  await mkdir(bindingsRoot, { recursive: true });
  await mkdir(fileUriToPathRoot, { recursive: true });
  await writeFile(
    join(rootDir, "package.json"),
    JSON.stringify({ name: prefix }),
    "utf8",
  );
  await writeFile(
    join(betterSqlite3Root, "package.json"),
    JSON.stringify({ name: "better-sqlite3", version: "12.10.0" }),
    "utf8",
  );
  await writeFile(
    join(bindingsRoot, "package.json"),
    JSON.stringify({ name: "bindings" }),
    "utf8",
  );
  await writeFile(
    join(fileUriToPathRoot, "package.json"),
    JSON.stringify({ name: "file-uri-to-path" }),
    "utf8",
  );
  configureSQLiteInstaller({ storageRoot: join(rootDir, ".global-storage") });
  return rootDir;
}

async function createDriver(
  overrides: Partial<ConnectionConfig> = {},
): Promise<{ driver: SQLiteDriver; filePath: string }> {
  const directory = await createProjectTempDir("sqlite-runtime");
  const filePath = join(
    directory,
    overrides.readOnly ? "readonly.sqlite" : "db.sqlite",
  );
  const driver = new SQLiteDriver({
    id: overrides.id ?? `sqlite-runtime-${Math.random().toString(36).slice(2)}`,
    name: overrides.name ?? "SQLite Runtime",
    type: "sqlite",
    filePath,
    ...overrides,
  } as ConnectionConfig);
  openDrivers.add(driver);
  return { driver, filePath: overrides.filePath ?? filePath };
}

async function readSingleValue(
  driver: SQLiteDriver,
  sql: string,
): Promise<unknown> {
  const result = await driver.query(sql);
  return result.rows[0]?.__col_0;
}

describe("SQLite better-sqlite3 runtime adapter", () => {
  it("creates a missing file for writable connections but requires it for read-only connections", async () => {
    const readOnly = await createDriver({ readOnly: true });
    await expect(readOnly.driver.connect()).rejects.toThrow();
    await expect(access(readOnly.filePath)).rejects.toThrow();
    const writable = await createDriver();
    await writable.driver.connect();
    await expect(access(writable.filePath)).resolves.toBeUndefined();
  });
  it("rejects an existing corrupt database during the read-only connection probe", async () => {
    const { driver, filePath } = await createDriver({ readOnly: true });
    await writeFile(filePath, "not a sqlite database");
    await expect(driver.connect()).rejects.toThrow();
    expect(await readFile(filePath, "utf8")).toBe("not a sqlite database");
  });
  it("filters SQLite's full Julian range including BC dates with ordinary AD operands", async () => {
    const { driver } = await createDriver();
    await driver.connect();
    await driver.query(
      "CREATE TABLE early_dates (id INTEGER PRIMARY KEY, dt DATETIME)",
    );
    await driver.query(`INSERT INTO early_dates VALUES
      (1, 0),
      (2, '-4713-11-24 12:00:00'),
      (3, julianday('-0001-12-31 23:59:59')),
      (4, '-0001-12-31 23:59:59.123456'),
      (5, '0000-01-01 00:00:00'),
      (6, '1969-12-31 23:59:59'),
      (7, '1970-01-01 00:00:00'),
      (8, '1971-01-01 00:00:00'),
      (9, julianday('9999-12-31 23:59:59.999')),
      (10, NULL), (11, -0.0000000001), (12, 5373484.5),
      (13, '-4713-11-24 11:59:59.999'),
      (14, '-0001-13-01 00:00:00'), (15, 'not a date')`);
    const native = await driver.query(
      "SELECT id, DATETIME(dt) FROM early_dates WHERE id <= 9 ORDER BY id",
    );
    expect(native.rows.every((row) => row.__col_1 !== null)).toBe(true);
    expect(native.rows[0].__col_1).toBe("-4713-11-24 12:00:00");
    const invalid = await driver.query(
      "SELECT DATETIME(dt) FROM early_dates WHERE id >= 10",
    );
    expect(invalid.rows.every((row) => row.__col_0 === null)).toBe(true);
    const dt = (
      await driver.describeColumns("main", "main", "early_dates")
    ).find((column) => column.name === "dt");
    if (!dt) throw new Error("Expected DATETIME metadata");
    const threshold = "1970-01-01 00:00:00";
    const cases = [
      ["eq", threshold, [7]],
      ["neq", threshold, [1, 2, 3, 4, 5, 6, 8, 9]],
      ["lt", threshold, [1, 2, 3, 4, 5, 6]],
      ["lte", threshold, [1, 2, 3, 4, 5, 6, 7]],
      ["gt", threshold, [8, 9]],
      ["gte", threshold, [7, 8, 9]],
      ["between", [threshold, "9999-12-31 23:59:59.999"], [7, 8, 9]],
      ["in", `${threshold},1971-01-01 00:00:00`, [7, 8]],
    ] as const;
    for (const [operator, input, expected] of cases) {
      const value =
        typeof input === "string" ? input : ([...input] as [string, string]);
      const condition = driver.buildFilterCondition(
        dt,
        operator,
        driver.normalizeFilterValue(dt, operator, value),
        1,
      );
      if (!condition) throw new Error("Expected datetime filter");
      const result = await driver.query(
        `SELECT id FROM early_dates WHERE ${condition.sql} ORDER BY id`,
        condition.params,
      );
      expect(
        result.rows.map((row) => row.__col_0),
        operator,
      ).toEqual(expected);
    }
  });

  it("preserves stored fractions across SQLite date/time and timezone separators", async () => {
    const { driver } = await createDriver();
    await driver.connect();
    const canonical = "2024-02-29 12:34:56.123456";
    const later = "2024-02-29 12:34:56.123457";
    const column: ColumnTypeMeta = {
      name: "event_at",
      type: "DATETIME",
      nativeType: "DATETIME",
      category: "datetime",
      nullable: true,
      isPrimaryKey: false,
      isForeignKey: false,
      filterable: true,
      filterOperators: ["eq", "neq", "gt", "gte", "lt", "lte", "between", "in"],
      valueSemantics: "plain",
    };
    for (const stored of [
      "2024-02-29  12:34:56.123456",
      "2024-02-29T T 12:34:56.123456",
      "2024-02-29\t12:34:56.123456",
      "2024-02-29 \r\n\t12:34:56.123456",
      "2024-02-29 15:34:56.123456   +03:00",
      "2024-02-29 15:34:56.123456\t+03:00",
    ]) {
      expect(
        (await driver.query("SELECT DATETIME(?)", [stored])).rows[0]?.__col_0,
      ).toBe("2024-02-29 12:34:56");
      const cases = [
        ["eq", canonical, [1]],
        ["neq", canonical, [2]],
        ["gt", canonical, [2]],
        ["gte", canonical, [1, 2]],
        ["lt", later, [1]],
        ["lte", canonical, [1]],
        ["between", [canonical, canonical], [1]],
        ["in", `${canonical}, ${later}`, [1, 2]],
      ] as const;
      for (const [operator, rawValue, expected] of cases) {
        const value =
          typeof rawValue === "string"
            ? rawValue
            : ([...rawValue] as [string, string]);
        const normalized = driver.normalizeFilterValue(column, operator, value);
        const condition = driver.buildFilterCondition(
          column,
          operator,
          normalized,
          1,
        );
        if (!condition) throw new Error("Expected datetime filter");
        const result = await driver.query(
          `WITH events(id,event_at) AS (VALUES (1,?),(2,?)) SELECT id FROM events WHERE ${condition.sql} ORDER BY id`,
          [stored, later, ...condition.params],
        );
        expect(
          result.rows.map((row) => row.__col_0),
          `${operator}: ${JSON.stringify(stored)}`,
        ).toEqual(expected);
      }
    }
    expect(
      (
        await driver.query("SELECT RAPIDB_DATETIME_KEY(?)", [
          "2024-02-29\u00a012:34:56.123456",
        ])
      ).rows[0]?.__col_0,
    ).toBeNull();
  });

  it("filters time-only SQLite datetimes without truncating fractional seconds", async () => {
    const { driver } = await createDriver();
    await driver.connect();
    await driver.query("CREATE TABLE time_values(event_at DATETIME)");
    const column = (
      await driver.describeColumns("main", "main", "time_values")
    ).find((meta) => meta.name === "event_at");
    if (!column) throw new Error("Expected DATETIME column metadata");
    const canonical = "2000-01-01 12:34:56.123456";
    const later = "2000-01-01 12:34:56.123457";
    for (const stored of [
      "12:34:56.123456",
      "12:34:56.123456Z",
      "15:34:56.123456+03:00",
      "07:34:56.123456-05:00",
    ]) {
      const keys = await driver.query(
        "SELECT RAPIDB_DATETIME_KEY(?), RAPIDB_DATETIME_KEY(?)",
        [stored, canonical],
      );
      expect(keys.rows[0].__col_0, stored).toBe(keys.rows[0].__col_1);
      expect(keys.rows[0].__col_0).not.toBeNull();
      for (const [operator, raw, expected] of [
        ["eq", canonical, [1]],
        ["neq", canonical, [2]],
        ["gt", canonical, [2]],
        ["gte", canonical, [1, 2]],
        ["lt", later, [1]],
        ["lte", canonical, [1]],
        ["between", [canonical, canonical], [1]],
        ["in", `${canonical}, ${later}`, [1, 2]],
      ] as const) {
        const input =
          typeof raw === "string" ? raw : ([...raw] as [string, string]);
        const condition = driver.buildFilterCondition(
          column,
          operator,
          driver.normalizeFilterValue(column, operator, input),
          1,
        );
        if (!condition) throw new Error("Expected datetime condition");
        const result = await driver.query(
          `WITH events(id,event_at) AS (VALUES(1,?),(2,?)) SELECT id FROM events WHERE ${condition.sql} ORDER BY id`,
          [stored, later, ...condition.params],
        );
        expect(
          result.rows.map((row) => row.__col_0),
          `${stored} ${operator}`,
        ).toEqual(expected);
      }
    }
  });

  it("matches SQLite Julian REAL dates to their millisecond text values without artificial fractions", async () => {
    const { driver } = await createDriver();
    await driver.connect();
    for (const value of [
      "2024-02-29 12:34:56",
      "2024-02-29 12:34:56.123",
      "2024-02-29 12:34:56.999",
      "2024-02-29 15:34:56.123+03:00",
      "1970-01-01 00:00:00",
      "1969-12-31 23:59:59.999",
      "0000-01-01 00:00:00.001",
      "9999-12-31 23:59:59.999",
    ]) {
      const result = await driver.query(
        "SELECT RAPIDB_DATETIME_KEY(julianday(?)) AS numeric_key, RAPIDB_DATETIME_KEY(?) AS text_key",
        [value, value],
      );
      expect(result.rows[0].__col_0, value).toBe(result.rows[0].__col_1);
      expect(result.rows[0].__col_0, value).not.toBeNull();
    }
  });

  it("filters stored hour-24 datetimes by their canonical instant without losing fractions", async () => {
    const { driver } = await createDriver();
    await driver.connect();
    const column: ColumnTypeMeta = {
      name: "dt",
      type: "DATETIME",
      nativeType: "DATETIME",
      category: "datetime",
      nullable: true,
      isPrimaryKey: false,
      isForeignKey: false,
      filterable: true,
      filterOperators: ["eq", "neq", "gt", "gte", "lt", "lte", "between", "in"],
      valueSemantics: "plain",
    };
    const later = "2025-01-01 00:00:00";
    for (const [stored, canonical] of [
      ["2024-02-29 24:00:00", "2024-03-01 00:00:00"],
      ["2024-02-29 24:30:00", "2024-03-01 00:30:00"],
      ["2024-02-29 24:59:59.1234567", "2024-03-01 00:59:59.1234567"],
      ["2024-02-29 24:00:00+03:00", "2024-02-29 21:00:00"],
      ["2024-02-29 24:00:00-05:00", "2024-03-01 05:00:00"],
      ["2024-12-31 24:00:00Z", "2025-01-01 00:00:00"],
    ]) {
      const keys = await driver.query(
        "SELECT JULIANDAY(?) = JULIANDAY(?), RAPIDB_DATETIME_KEY(?), RAPIDB_DATETIME_KEY(?)",
        [stored, canonical, stored, canonical],
      );
      expect(keys.rows[0].__col_0, stored).toBe(1);
      expect(keys.rows[0].__col_1, stored).not.toBeNull();
      expect(keys.rows[0].__col_1, stored).toBe(keys.rows[0].__col_2);
      const second = canonical === later ? "2025-01-02 00:00:00" : later;
      const cases = [
        ["eq", canonical, [1]],
        ["neq", canonical, [2]],
        ["gt", canonical, [2]],
        ["gte", canonical, [1, 2]],
        ["lt", canonical, []],
        ["lte", canonical, [1]],
        ["between", [canonical, canonical], [1]],
        ["in", canonical, [1]],
      ] as const;
      for (const [operator, raw, expected] of cases) {
        const value =
          typeof raw === "string" ? raw : ([...raw] as [string, string]);
        const condition = driver.buildFilterCondition(
          column,
          operator,
          driver.normalizeFilterValue(column, operator, value),
          1,
        );
        if (!condition) throw new Error("Expected datetime predicate");
        const result = await driver.query(
          `WITH events(id,dt) AS (VALUES (1,?),(2,?)) SELECT id FROM events WHERE ${condition.sql} ORDER BY id`,
          [stored, second, ...condition.params],
        );
        expect(
          result.rows.map((row) => row.__col_0),
          `${stored}: ${operator}`,
        ).toEqual(expected);
      }
    }
  });

  it("carries hour 24 across BC/year-zero boundaries and rejects invalid clock components", async () => {
    const { driver } = await createDriver();
    await driver.connect();
    for (const [stored, canonical] of [
      ["-0001-12-31 24:00:00", "0000-01-01 00:00:00"],
      ["0000-02-29 24:30:00.123456", "0000-03-01 00:30:00.123456"],
    ]) {
      const result = await driver.query(
        "SELECT RAPIDB_DATETIME_KEY(?), RAPIDB_DATETIME_KEY(?), JULIANDAY(?) = JULIANDAY(?)",
        [stored, canonical, stored, canonical],
      );
      expect(result.rows[0].__col_0).not.toBeNull();
      expect(result.rows[0].__col_0).toBe(result.rows[0].__col_1);
      expect(result.rows[0].__col_2).toBe(1);
    }
    for (const value of [
      "2024-02-29 25:00:00",
      "2024-02-29 24:60:00",
      "2024-02-29 24:00:60",
    ]) {
      const result = await driver.query(
        "SELECT RAPIDB_DATETIME_KEY(?), DATETIME(?)",
        [value, value],
      );
      expect(result.rows[0]).toMatchObject({ __col_0: null, __col_1: null });
    }
  });

  it("filters mixed Julian REAL and precise TEXT datetimes with all comparison operators", async () => {
    const { driver } = await createDriver();
    await driver.connect();
    await driver.query(
      "CREATE TABLE julian_dates (id INTEGER PRIMARY KEY, dt DATETIME)",
    );
    await driver.query(`INSERT INTO julian_dates VALUES
      (1, julianday('2024-02-29 12:34:56')),
      (2, julianday('2024-02-29 12:34:56.123')),
      (3, julianday('2024-02-29 12:34:56.124')),
      (4, '2024-02-29 12:34:56.123456'),
      (5, julianday('2024-02-29 15:34:56.123+03:00'))`);
    const dt = (
      await driver.describeColumns("main", "main", "julian_dates")
    ).find((column) => column.name === "dt");
    if (!dt) throw new Error("Expected DATETIME metadata");
    const threshold = "2024-02-29 12:34:56.123";
    const cases = [
      ["eq", "2024-02-29 12:34:56", [1]],
      ["eq", threshold, [2, 5]],
      ["neq", threshold, [1, 3, 4]],
      ["gt", threshold, [3, 4]],
      ["gte", threshold, [2, 3, 4, 5]],
      ["lt", threshold, [1]],
      ["lte", threshold, [1, 2, 5]],
      ["between", [threshold, "2024-02-29 12:34:56.124"], [2, 3, 4, 5]],
      ["between", [threshold, threshold], [2, 5]],
      ["in", `2024-02-29 12:34:56,${threshold}`, [1, 2, 5]],
    ] as const;
    for (const [operator, input, expected] of cases) {
      const value =
        typeof input === "string" ? input : ([...input] as [string, string]);
      const normalized = driver.normalizeFilterValue(dt, operator, value);
      const condition = driver.buildFilterCondition(
        dt,
        operator,
        normalized,
        1,
      );
      const result = await driver.query(
        `SELECT id FROM julian_dates WHERE ${condition?.sql} ORDER BY id`,
        condition?.params,
      );
      expect(
        result.rows.map((row) => row.__col_0),
        operator,
      ).toEqual(expected);
    }
  });

  it("reads CHECK constraints with nested parentheses", async () => {
    const { driver } = await createDriver();
    await driver.connect();
    await driver.query(
      `CREATE TABLE checked_values (
        value INTEGER,
        status TEXT,
        CONSTRAINT "valid status" CHECK ((value > 0) AND status IN ('new', 'done'))
      )`,
    );

    const constraints = await driver.getConstraints(
      "main",
      "main",
      "checked_values",
    );

    expect(constraints).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          name: "valid status",
          kind: "check",
          checkExpression: "(value > 0) AND status IN ('new', 'done')",
        }),
      ]),
    );
  });

  it("enables foreign keys without changing the journal mode on writable connections", async () => {
    const { driver } = await createDriver();

    await driver.connect();

    await driver.query("CREATE TABLE parents (id INTEGER PRIMARY KEY)");
    await driver.query(
      "CREATE TABLE children (id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES parents(id))",
    );

    expect(await readSingleValue(driver, "PRAGMA foreign_keys")).toBe(1);
    expect(await readSingleValue(driver, "PRAGMA journal_mode")).toBe("delete");
    await expect(
      driver.query("INSERT INTO children (id, parent_id) VALUES (1, 999)"),
    ).rejects.toThrow(/foreign key/i);
  });

  it("preserves each rowset from a multi-statement query", async () => {
    const { driver } = await createDriver();
    await driver.connect();

    const result = await driver.query(
      "SELECT 1 AS first_value; SELECT 2 AS second_value;",
    );

    expect(result.resultSets).toEqual([
      {
        id: "result-1",
        columns: ["first_value"],
        columnMeta: [{ category: "integer" }],
        rows: [{ __col_0: 1 }],
        rowCount: 1,
        truncated: false,
      },
      {
        id: "result-2",
        columns: ["second_value"],
        columnMeta: [{ category: "integer" }],
        rows: [{ __col_0: 2 }],
        rowCount: 1,
        truncated: false,
      },
    ]);
    expect(result.columns).toEqual(["first_value"]);
    expect(result.rows).toEqual([{ __col_0: 1 }]);
    expect(result.rowCount).toBe(2);
  });

  it("provides safe numeric query metadata and leaves numeric-looking text untyped", async () => {
    const { driver } = await createDriver();
    await driver.connect();

    const numeric = await driver.query(
      "SELECT 10 AS value UNION ALL SELECT 2 AS value",
    );
    const text = await driver.query(
      "SELECT '10' AS value UNION ALL SELECT '2' AS value",
    );

    expect(numeric.columnMeta).toEqual([{ category: "integer" }]);
    expect(text.columnMeta).toEqual([{ category: null }]);
  });

  it("retains exact numeric metadata for INTEGER and REAL values in one query column", async () => {
    const { driver } = await createDriver();
    await driver.connect();

    const result = await driver.query(
      "SELECT 9007199254740993 AS value UNION ALL SELECT 9007199254740992.0 AS value",
    );

    expect(result.columnMeta).toEqual([{ category: "decimal" }]);
    expect(result.rows.map((row) => row.__col_0)).toEqual([
      "9007199254740993",
      9007199254740992,
    ]);
  });

  it("retains exact numeric metadata on SQLite RETURNING results", async () => {
    const { driver } = await createDriver();
    await driver.connect();
    await driver.query(
      "CREATE TABLE returning_values (id INTEGER PRIMARY KEY)",
    );

    const result = await driver.query(
      "INSERT INTO returning_values DEFAULT VALUES RETURNING id",
    );

    expect(result.columnMeta).toEqual([{ category: "integer" }]);
    expect(result.rows).toEqual([{ __col_0: 1 }]);
  });

  it("applies one row hard cap across all retained script rowsets", async () => {
    const { driver } = await createDriver();
    await driver.connect();

    const result = await driver.query(
      "SELECT 1 AS first_value UNION ALL SELECT 2; SELECT 3 AS second_value UNION ALL SELECT 4;",
      undefined,
      { hardCap: 2 },
    );

    expect(result.resultSets).toMatchObject([
      {
        columns: ["first_value"],
        rows: [{ __col_0: 1 }, { __col_0: 2 }],
        rowCount: 2,
      },
      { columns: ["second_value"], rows: [], rowCount: 2, truncated: true },
    ]);
    expect(
      result.resultSets?.reduce((total, set) => total + set.rows.length, 0),
    ).toBe(2);
    expect(result.truncated).toBe(true);
  });

  it("filters weakly typed INTEGER values using raw SQLite equality semantics", async () => {
    const { driver } = await createDriver();
    const typedIntegerColumn: ColumnTypeMeta = {
      name: "typed_integer",
      type: "INTEGER",
      nativeType: "INTEGER",
      category: "integer",
      nullable: true,
      isPrimaryKey: false,
      isForeignKey: false,
      filterable: true,
      filterOperators: [
        "eq",
        "neq",
        "gt",
        "gte",
        "lt",
        "lte",
        "between",
        "in",
        "is_null",
        "is_not_null",
      ],
      valueSemantics: "plain",
    };

    await driver.connect();
    await driver.query(
      "CREATE TABLE weak_values (id INTEGER PRIMARY KEY, typed_integer INTEGER)",
    );
    await driver.query(
      "INSERT INTO weak_values (id, typed_integer) VALUES (1, 'not_a_number'), (2, '-9223372036854776000')",
    );

    const textFilter = driver.buildFilterCondition(
      typedIntegerColumn,
      "eq",
      driver.normalizeFilterValue(
        typedIntegerColumn,
        "eq",
        "not_a_number",
      ) as string,
      1,
    );
    const largeIntegerFilter = driver.buildFilterCondition(
      typedIntegerColumn,
      "eq",
      driver.normalizeFilterValue(
        typedIntegerColumn,
        "eq",
        "-9223372036854776000",
      ) as string,
      1,
    );

    expect(textFilter).toBeTruthy();
    expect(largeIntegerFilter).toBeTruthy();

    const textResult = await driver.query(
      `SELECT typed_integer FROM weak_values WHERE ${textFilter?.sql}`,
      textFilter?.params,
    );
    const largeIntegerResult = await driver.query(
      `SELECT typed_integer FROM weak_values WHERE ${largeIntegerFilter?.sql}`,
      largeIntegerFilter?.params,
    );

    expect(textResult.rows).toEqual([{ __col_0: "not_a_number" }]);
    expect(largeIntegerResult.rowCount).toBe(1);
    expect(String(largeIntegerResult.rows[0]?.__col_0)).toBe(
      "-9223372036854776000",
    );
  });

  it("preserves precision across SQLite DATETIME representations and offsets", async () => {
    const { driver } = await createDriver();
    const datetimeColumn: ColumnTypeMeta = {
      name: "datetime_value",
      type: "DATETIME",
      nativeType: "DATETIME",
      category: "datetime",
      nullable: true,
      isPrimaryKey: false,
      isForeignKey: false,
      filterable: true,
      filterOperators: [
        "eq",
        "neq",
        "gt",
        "gte",
        "lt",
        "lte",
        "between",
        "in",
        "is_null",
        "is_not_null",
      ],
      valueSemantics: "plain",
    };

    await driver.connect();
    await driver.query(
      "CREATE TABLE datetime_values (id INTEGER PRIMARY KEY, datetime_value DATETIME)",
    );
    await driver.query(
      `INSERT INTO datetime_values (id, datetime_value) VALUES
        (1, '2024-02-29 12:34:56.123456'),
        (2, '2024-02-29T12:34:56.1234'),
        (3, '2024-02-29 15:34:56.123456+03:00'),
        (4, '2024-02-29 12:34:56.1'),
        (5, '2024-02-29 12:34:56.100'),
        (6, 10000000),
        (7, '2024-02-29 15:34:56.123456+14:59')`,
    );
    expect(
      await readSingleValue(
        driver,
        "SELECT typeof(datetime_value) FROM datetime_values WHERE id = 1",
      ),
    ).toBe("text");
    expect(
      await readSingleValue(
        driver,
        "SELECT DATETIME(datetime_value) FROM datetime_values WHERE id = 6",
      ),
    ).toBe(null);

    const runFilter = async (
      operator: "eq" | "gt" | "lt" | "between" | "in",
      value: string | [string, string],
    ) => {
      const normalized = driver.normalizeFilterValue(
        datetimeColumn,
        operator,
        value,
      );
      const condition = driver.buildFilterCondition(
        datetimeColumn,
        operator,
        normalized,
        1,
      );
      const result = await driver.query(
        `SELECT id FROM datetime_values WHERE ${condition?.sql}`,
        condition?.params,
      );
      return result.rows.map((row) => row.__col_0);
    };

    expect(await runFilter("gt", "2024-02-29 12:34:56.123455")).toEqual([1, 3]);
    expect(await runFilter("lt", "2024-02-29 12:34:56.123457")).toEqual([
      1, 2, 3, 4, 5, 7,
    ]);
    expect(await runFilter("eq", "2024-02-29 12:34:56.123456")).toEqual([1, 3]);
    expect(await runFilter("eq", "2024-02-29 12:34:56.123455")).toEqual([]);
    expect(await runFilter("eq", "2024-02-29 12:34:56.123400")).toEqual([2]);
    expect(await runFilter("eq", "2024-02-29 12:34:56.100")).toEqual([4, 5]);
    expect(await runFilter("lt", "2024-02-29 12:34:56.100")).toEqual([7]);
    expect(await runFilter("eq", "2024-02-29 00:35:56.123456Z")).toEqual([7]);
    expect(
      await runFilter("between", [
        "2024-02-29 00:35:56.123455Z",
        "2024-02-29 00:35:56.123457Z",
      ]),
    ).toEqual([7]);
    expect(await runFilter("eq", "9999-12-31 23:59:59")).toEqual([]);
    expect(
      await runFilter("between", [
        "2024-02-29 12:34:56.123455",
        "2024-02-29 12:34:56.123457",
      ]),
    ).toEqual([1, 3]);
    expect(
      await runFilter("between", [
        "2024-02-29 12:34:56.123457",
        "2024-02-29 12:34:56.123458",
      ]),
    ).toEqual([]);
    expect(
      await runFilter(
        "in",
        "2024-02-29 12:34:56.123455, 2024-02-29 12:34:56.123456, 2024-02-29T12:34:56.123400",
      ),
    ).toEqual([1, 2, 3]);
    expect(await runFilter("eq", "2024-02-29 15:34:56.123456+03:00")).toEqual([
      1, 3,
    ]);
    expect(await runFilter("eq", "2024-02-29 12:34:56.123456Z")).toEqual([
      1, 3,
    ]);
  });

  it("round-trips signed 64-bit integers without JavaScript precision loss", async () => {
    const { driver } = await createDriver();
    await driver.connect();
    await driver.query("CREATE TABLE exact_ints (value INTEGER)");
    await driver.query("INSERT INTO exact_ints (value) VALUES (?)", [
      "9223372036854775807",
    ]);

    expect(await readSingleValue(driver, "SELECT value FROM exact_ints")).toBe(
      "9223372036854775807",
    );
  });

  it.each([
    "delete",
    "wal",
  ])("leaves an existing %s journal mode unchanged on writable connect", async (mode) => {
    const { driver: setupDriver, filePath } = await createDriver();
    await setupDriver.connect();
    await setupDriver.query("CREATE TABLE shared (id INTEGER PRIMARY KEY)");
    await setupDriver.query(`PRAGMA journal_mode = ${mode}`);
    expect(await readSingleValue(setupDriver, "PRAGMA journal_mode")).toBe(
      mode,
    );
    await setupDriver.disconnect();
    openDrivers.delete(setupDriver);

    const { driver } = await createDriver({ filePath });
    await driver.connect();

    expect(await readSingleValue(driver, "PRAGMA journal_mode")).toBe(mode);
    expect(await readSingleValue(driver, "SELECT COUNT(*) FROM shared")).toBe(
      0,
    );
  });

  it("opens readonly databases without mutating journal mode", async () => {
    const { driver: writableDriver, filePath } = await createDriver();
    await writableDriver.connect();
    await writableDriver.query("CREATE TABLE sample (id INTEGER PRIMARY KEY)");
    await writableDriver.query("INSERT INTO sample (id) VALUES (7)");
    expect(await readSingleValue(writableDriver, "PRAGMA journal_mode")).toBe(
      "delete",
    );
    await writableDriver.disconnect();
    openDrivers.delete(writableDriver);

    const readonlyDriver = new SQLiteDriver({
      id: "sqlite-runtime-readonly",
      name: "SQLite Runtime Readonly",
      type: "sqlite",
      filePath,
      readOnly: true,
    } as ConnectionConfig);
    openDrivers.add(readonlyDriver);

    await readonlyDriver.connect();

    const fileBeforeReadonlyQueries = await readFile(filePath);

    expect(await readSingleValue(readonlyDriver, "PRAGMA foreign_keys")).toBe(
      1,
    );
    expect(await readSingleValue(readonlyDriver, "PRAGMA journal_mode")).toBe(
      "delete",
    );
    await expect(
      readonlyDriver.query("INSERT INTO sample (id) VALUES (8)"),
    ).rejects.toThrow();

    const readOnlyGuard = readonlyDriver.getCapabilities().readOnlyQueryGuard;
    const tempTableScript =
      "SELECT 1; CREATE TEMP TABLE rapidb_temp (id INTEGER); INSERT INTO rapidb_temp (id) VALUES (1)";
    expect(readOnlyGuard?.(tempTableScript)).toEqual(
      expect.objectContaining({ allowed: false }),
    );
    await readonlyDriver.query(tempTableScript);
    expect(
      await readSingleValue(readonlyDriver, "SELECT id FROM rapidb_temp"),
    ).toBe(1);
    expect(await readFile(filePath)).toEqual(fileBeforeReadonlyQueries);
  });

  it("rejects PRAGMA optimize as read-only SQL although SQLite can persist its stats", async () => {
    const { driver } = await createDriver();
    await driver.connect();
    await driver.query("CREATE TABLE optimize_sample (value TEXT)");
    await driver.query(
      "CREATE INDEX optimize_sample_value ON optimize_sample(value)",
    );
    await driver.query(
      "INSERT INTO optimize_sample VALUES ('alpha'), ('beta'), ('gamma')",
    );

    await driver.query("PRAGMA optimize=0x10002");

    expect(
      await readSingleValue(
        driver,
        "SELECT COUNT(*) FROM sqlite_schema WHERE name = 'sqlite_stat1'",
      ),
    ).toBe(1);
    expect(
      driver.getCapabilities().readOnlyQueryGuard?.("PRAGMA optimize"),
    ).toEqual(expect.objectContaining({ allowed: false }));
  });

  it("keeps WAL journal mode intact for readonly opens", async () => {
    const { driver: writableDriver, filePath } = await createDriver();
    await writableDriver.connect();
    await writableDriver.query("PRAGMA journal_mode = WAL");
    await writableDriver.query(
      "CREATE TABLE sample (id INTEGER PRIMARY KEY, name TEXT)",
    );
    await writableDriver.query(
      "INSERT INTO sample (id, name) VALUES (1, 'alpha')",
    );
    expect(await readSingleValue(writableDriver, "PRAGMA journal_mode")).toBe(
      "wal",
    );
    await writableDriver.disconnect();
    openDrivers.delete(writableDriver);

    const readonlyDriver = new SQLiteDriver({
      id: "sqlite-runtime-readonly-wal",
      name: "SQLite Runtime Readonly WAL",
      type: "sqlite",
      filePath,
      readOnly: true,
    } as ConnectionConfig);
    openDrivers.add(readonlyDriver);

    await readonlyDriver.connect();

    expect(await readSingleValue(readonlyDriver, "PRAGMA foreign_keys")).toBe(
      1,
    );
    expect(await readSingleValue(readonlyDriver, "PRAGMA journal_mode")).toBe(
      "wal",
    );
    expect(
      await readSingleValue(readonlyDriver, "SELECT COUNT(*) FROM sample"),
    ).toBe(1);
  });

  it("rejects readonly opens for missing files and leaves them absent", async () => {
    const directory = await createProjectTempDir("sqlite-runtime-missing");
    const filePath = join(directory, "missing.sqlite");
    const driver = new SQLiteDriver({
      id: "sqlite-runtime-missing",
      name: "SQLite Runtime Missing",
      type: "sqlite",
      filePath,
      readOnly: true,
    } as ConnectionConfig);
    openDrivers.add(driver);

    const error = await driver.connect().catch((failure: unknown) => failure);

    expect(error).toBeInstanceOf(Error);
    expect((error as Error).message).toContain(filePath);
    expect((error as Error).message).toContain("read-only mode");
    await expect(access(filePath)).rejects.toThrow();
  });

  it("formats non-finite REAL values as display strings", () => {
    const driver = new SQLiteDriver({
      id: "sqlite-runtime-special-float",
      name: "SQLite Runtime Special Float",
      type: "sqlite",
      filePath: "/tmp/sqlite-runtime-special-float.sqlite",
    } as ConnectionConfig);
    const column: ColumnTypeMeta = {
      name: "col_real",
      type: "real",
      nativeType: "REAL",
      category: "float",
      nullable: true,
      defaultValue: undefined,
      isPrimaryKey: false,
      primaryKeyOrdinal: undefined,
      isForeignKey: false,
      filterable: true,
      filterOperators: [],
      valueSemantics: "plain",
    };

    expect(driver.formatOutputValue(Number.POSITIVE_INFINITY, column)).toBe(
      "Infinity",
    );
  });

  it("lists attached databases and their objects after ATTACH DATABASE", async () => {
    const { driver } = await createDriver({ name: "SQLite Main" });
    const { driver: billingDriver, filePath: billingPath } = await createDriver(
      {
        name: "SQLite Billing",
      },
    );

    await billingDriver.connect();
    await billingDriver.query(
      "CREATE TABLE invoices (id INTEGER PRIMARY KEY, amount_cents INTEGER NOT NULL)",
    );
    await billingDriver.disconnect();

    await driver.connect();
    await driver.query("CREATE TABLE customers (id INTEGER PRIMARY KEY)");
    await driver.query(
      `ATTACH DATABASE '${billingPath.replace(/'/g, "''")}' AS billing`,
    );

    const databases = await driver.listDatabases();

    expect(databases.map((database) => database.name)).toEqual([
      "main",
      "billing",
    ]);
    await expect(driver.listSchemas("billing")).resolves.toEqual([
      { name: "billing" },
    ]);
    await expect(driver.listObjects("billing", "billing")).resolves.toEqual([
      {
        schema: "billing",
        name: "invoices",
        type: "table",
      },
    ]);

    const columns = await driver.describeTable(
      "billing",
      "billing",
      "invoices",
    );

    expect(columns.map((column) => column.name)).toEqual([
      "id",
      "amount_cents",
    ]);
  });

  it.each([
    '"a(b"',
    '"a)b("',
    '"a""(b"',
    "`a(b`",
    "[a(b]",
  ])("preserves identity and generated metadata for a table named %s", async (quotedName) => {
    const { driver } = await createDriver();
    await driver.connect();
    await driver.query(
      `CREATE TABLE ${quotedName} (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, slug TEXT GENERATED ALWAYS AS (lower(name)) STORED)`,
    );
    const objects = await driver.listObjects("main", "main");
    const table = objects[0]?.name;
    if (!table) throw new Error("Missing created table");
    const columns = await driver.describeColumns("main", "main", table);
    expect(columns.find((column) => column.name === "id")).toMatchObject({
      identityGeneration: "auto_increment",
    });
    expect(columns.find((column) => column.name === "slug")).toMatchObject({
      computedExpression: "lower(name)",
      generatedKind: "stored",
      isPersisted: true,
    });
  });

  it("lists user tables whose names only resemble reserved sqlite_ names", async () => {
    const { driver } = await createDriver();
    await driver.connect();
    await driver.query(
      "CREATE TABLE counters (id INTEGER PRIMARY KEY AUTOINCREMENT)",
    );
    await driver.query("INSERT INTO counters DEFAULT VALUES");
    for (const name of ["sqlite3_cache", "SQLiteLogs", "sqlitex"]) {
      await driver.query(`CREATE TABLE ${name} (id INTEGER)`);
    }

    const names = (await driver.listObjects("main", "main")).map(
      (object) => object.name,
    );

    expect(names).toEqual(
      expect.arrayContaining([
        "counters",
        "sqlite3_cache",
        "SQLiteLogs",
        "sqlitex",
      ]),
    );
    expect(names).not.toContain("sqlite_sequence");
  });

  it("groups SQLite foreign keys by constraint and resolves implicit primary key references", async () => {
    const { driver } = await createDriver();
    const { driver: archiveDriver, filePath: archivePath } =
      await createDriver();
    await archiveDriver.connect();
    await archiveDriver.query(
      "CREATE TABLE parents (a INTEGER, b INTEGER, PRIMARY KEY (b, a))",
    );
    await archiveDriver.query("CREATE TABLE owners (id INTEGER PRIMARY KEY)");
    await archiveDriver.query(
      "CREATE TABLE children (x INTEGER, y INTEGER, z INTEGER, FOREIGN KEY (x, y) REFERENCES parents (b, a), FOREIGN KEY (z) REFERENCES owners, FOREIGN KEY (z) REFERENCES parents (a), FOREIGN KEY (x, y) REFERENCES parents)",
    );
    await archiveDriver.disconnect();
    openDrivers.delete(archiveDriver);

    await driver.connect();
    await driver.query(
      `ATTACH DATABASE '${archivePath.replace(/'/g, "''")}' AS archive`,
    );

    const foreignKeys = await driver.getForeignKeys(
      "archive",
      "archive",
      "children",
    );
    const byConstraint = new Map<string, string[]>();
    for (const foreignKey of foreignKeys) {
      expect(foreignKey.referencedSchema).toBe("archive");
      byConstraint.set(foreignKey.constraintName, [
        ...(byConstraint.get(foreignKey.constraintName) ?? []),
        `${foreignKey.column}->${foreignKey.referencedTable}.${foreignKey.referencedColumn}`,
      ]);
    }

    expect(
      Array.from(byConstraint.values())
        .map((columns) => columns.join(","))
        .sort(),
    ).toEqual([
      "x->parents.b,y->parents.a",
      "x->parents.b,y->parents.a",
      "z->owners.id",
      "z->parents.a",
    ]);

    const constraints = (
      await driver.getConstraints("archive", "archive", "children")
    ).filter((constraint) => constraint.kind === "foreign_key");
    expect(constraints).toHaveLength(4);
    expect(
      constraints.every(
        (constraint) =>
          constraint.columns.length === constraint.referencedColumns?.length,
      ),
    ).toBe(true);
  });

  it("ships the better-sqlite3 scaffold but excludes native binaries from the VSIX", async () => {
    const betterSqlite3Package = JSON.parse(
      await readFile(
        new URL(
          "../../node_modules/better-sqlite3/package.json",
          import.meta.url,
        ),
        "utf8",
      ),
    ) as {
      dependencies?: Record<string, string>;
    };
    const bindingsPackage = JSON.parse(
      await readFile(
        new URL("../../node_modules/bindings/package.json", import.meta.url),
        "utf8",
      ),
    ) as {
      dependencies?: Record<string, string>;
    };
    const vscodeIgnore = await readFile(
      new URL("../../.vscodeignore", import.meta.url),
      "utf8",
    );

    expect(betterSqlite3Package.dependencies?.bindings).toBeTruthy();
    expect(bindingsPackage.dependencies?.["file-uri-to-path"]).toBeTruthy();
    expect(vscodeIgnore).toContain(
      "!.rapidb-runtime/node_modules/better-sqlite3/**",
    );
    expect(vscodeIgnore).toContain("!.rapidb-runtime/node_modules/bindings/**");
    expect(vscodeIgnore).toContain(
      "!.rapidb-runtime/node_modules/file-uri-to-path/**",
    );
    expect(vscodeIgnore).not.toContain("!.rapidb-vscode/better-sqlite3/**");
  });

  it("prefers the installed SQLite runtime cache before the default package", async () => {
    const rootDir = await createManagedRuntimeFixture("sqlite-runtime-cache");
    const probe = probeInstalledBetterSqlite3Runtime(join(rootDir, "dist"));
    if (!probe.installedPackagePath || !probe.installedBinaryPath) {
      throw new Error("Managed SQLite runtime paths could not be resolved.");
    }
    await mkdir(join(probe.installedPackagePath, "build", "Release"), {
      recursive: true,
    });
    await writeFile(
      join(probe.installedPackagePath, "package.json"),
      JSON.stringify({ name: "better-sqlite3", version: "12.10.0" }),
      "utf8",
    );
    await writeFile(probe.installedBinaryPath, "stub", "utf8");

    expect(
      resolveBetterSqlite3LoadTargets(join(rootDir, "dist"), true),
    ).toEqual([probe.installedPackagePath, "better-sqlite3"]);
    expect(
      resolveBetterSqlite3LoadTargets(join(rootDir, "dist"), false),
    ).toEqual(["better-sqlite3"]);
  });

  it("refuses to install a missing runtime without authorization", async () => {
    const rootDir = await createManagedRuntimeFixture("sqlite-runtime-denied");
    configureSQLiteInstaller({
      storageRoot: join(rootDir, ".global-storage"),
      allowInstall: () => false,
    });

    await expect(
      ensureSQLiteRuntimeInstalled(join(rootDir, "dist")),
    ).rejects.toThrow("installation was not authorized");
  });

  it("resolves staged runtime targets across the supported platform matrix", () => {
    expect(resolveSQLiteRuntimeTarget("win32", "x64")).toBe("win32-x64");
    expect(resolveSQLiteRuntimeTarget("win32", "arm64")).toBe("win32-arm64");
    expect(resolveSQLiteRuntimeTarget("linux", "x64")).toBe("linux-x64");
    expect(resolveSQLiteRuntimeTarget("linux", "arm64")).toBe("linux-arm64");
    expect(resolveSQLiteRuntimeTarget("darwin", "x64")).toBe("darwin-x64");
    expect(resolveSQLiteRuntimeTarget("darwin", "arm64")).toBe("darwin-arm64");
  });

  it("reports installed runtime probe details including cached binary presence", async () => {
    const rootDir = await createManagedRuntimeFixture("sqlite-runtime-probe");
    const probeBeforeBinary = probeInstalledBetterSqlite3Runtime(
      join(rootDir, "dist"),
    );
    expect(probeBeforeBinary.bundledPackageExists).toBe(true);
    expect(probeBeforeBinary.installedPackageExists).toBe(false);
    expect(probeBeforeBinary.installedBinaryExists).toBe(false);

    if (
      !probeBeforeBinary.installedPackagePath ||
      !probeBeforeBinary.installedBinaryPath
    ) {
      throw new Error("Managed SQLite runtime paths could not be resolved.");
    }

    await mkdir(
      join(probeBeforeBinary.installedPackagePath, "build", "Release"),
      {
        recursive: true,
      },
    );
    await writeFile(
      join(probeBeforeBinary.installedPackagePath, "package.json"),
      JSON.stringify({ name: "better-sqlite3", version: "12.10.0" }),
      "utf8",
    );
    await writeFile(probeBeforeBinary.installedBinaryPath, "stub", "utf8");

    const probeAfterBinary = probeInstalledBetterSqlite3Runtime(
      join(rootDir, "dist"),
    );
    expect(probeAfterBinary.installedPackageExists).toBe(true);
    expect(probeAfterBinary.installedBinaryExists).toBe(true);
  });

  it("formats sqlite runtime load failures with attempted targets and installed runtime probe details", () => {
    const message = formatSQLiteRuntimeLoadErrorMessage(
      new Error("Cannot find module better-sqlite3"),
      [
        "/tmp/global-storage/sqlite-runtime/better-sqlite3/12.10.0/electron-abi-127-linux-x64/node_modules/better-sqlite3",
        "better-sqlite3",
      ],
      {
        target: "linux-x64",
        runtime: "node",
        packageRoot: "/tmp/workspace",
        bundledPackagePath: "/tmp/workspace/node_modules/better-sqlite3",
        bundledPackageExists: true,
        installedPackagePath:
          "/tmp/global-storage/sqlite-runtime/better-sqlite3/12.10.0/node-abi-115-linux-x64/node_modules/better-sqlite3",
        installedPackageExists: true,
        installedBinaryPath:
          "/tmp/global-storage/sqlite-runtime/better-sqlite3/12.10.0/node-abi-115-linux-x64/node_modules/better-sqlite3/build/Release/better_sqlite3.node",
        installedBinaryExists: false,
      },
    );

    expect(message).toContain("Attempted load targets");
    expect(message).toContain("Installed runtime probe:");
    expect(message).toContain("installedBinaryExists=false");
    expect(message).toContain("Cannot find module better-sqlite3");
  });
});

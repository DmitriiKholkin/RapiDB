import { describe, expect, it, vi } from "vitest";
import { MutationNotExecutedError } from "../../src/extension/dbDrivers/mutationExecutionState";
import { MySQLDriver } from "../../src/extension/dbDrivers/mysql";
import type {
  ColumnTypeMeta,
  TransactionOperation,
} from "../../src/extension/dbDrivers/types";
import { buildInsertRowOperation } from "../../src/extension/table/insertSql";
import { prepareInsertVerification } from "../../src/extension/table/insertVerification";
import { buildUpdateRowSql } from "../../src/extension/table/updateSql";

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
    nullable: !isPrimaryKey,
    isPrimaryKey,
    isForeignKey: false,
    filterable: true,
    filterOperators: [],
    valueSemantics: "plain",
  };
}

const seenAt = column("seen_at", "timestamp(3)", "datetime");
const createdAt = column("created_at", "datetime", "datetime");
const note = column("note", "varchar(64)", "text");
const id = column("id", "int", "integer", true);

function newDriver() {
  return new MySQLDriver({
    id: "dst",
    name: "DST",
    type: "mysql",
    database: "target",
  } as never);
}

function harness(epochs: (params: unknown[]) => unknown[]) {
  const driver = newDriver();
  const statements: Array<{ sql: string; values: unknown[] }> = [];
  const query = vi.fn(async (input: { sql: string; values?: unknown[] }) => {
    const values = input.values ?? [];
    statements.push({ sql: input.sql, values });
    if (input.sql === "ROLLBACK") return [{ warningStatus: 0 }];
    if (input.sql.startsWith("SELECT @@session.time_zone")) {
      const row: Record<string, unknown> = {
        __rapidb_time_zone: "Europe/Berlin",
      };
      for (const [index, epoch] of epochs(values).entries())
        row[`__rapidb_epoch_${index}`] = epoch;
      return [[row]];
    }
    if (input.sql.startsWith("SELECT")) return [[]];
    return [{ affectedRows: 1, insertId: 0 }];
  });
  const connection = {
    query,
    beginTransaction: vi.fn(async () => {
      statements.push({ sql: "BEGIN", values: [] });
    }),
    commit: vi.fn(async () => {
      statements.push({ sql: "COMMIT", values: [] });
    }),
    release: vi.fn(),
    destroy: vi.fn(),
  };
  (driver as unknown as { pool: unknown }).pool = {
    getConnection: vi.fn(async () => connection),
    config: { connectionConfig: {} },
  };
  return { driver, statements };
}

function insertOperation(
  driver: MySQLDriver,
  values: Record<string, unknown>,
): TransactionOperation {
  return buildInsertRowOperation(driver, "target", "", "events", values, [
    id,
    seenAt,
    createdAt,
    note,
  ]);
}

describe("MySQL TIMESTAMP filters with an explicit offset", () => {
  const driver = newDriver();

  it("compares the stored instant through UNIX_TIMESTAMP with an exact decimal epoch", () => {
    expect(
      driver.buildFilterCondition(seenAt, "eq", "2024-10-27T00:30:00Z", 1),
    ).toEqual({
      sql: "UNIX_TIMESTAMP(`seen_at`) = CAST(? AS DECIMAL(20,6))",
      params: ["1729989000"],
    });
    expect(
      driver.buildFilterCondition(seenAt, "eq", "2024-10-27T01:30:00Z", 1),
    ).toEqual({
      sql: "UNIX_TIMESTAMP(`seen_at`) = CAST(? AS DECIMAL(20,6))",
      params: ["1729992600"],
    });
    expect(
      driver.buildFilterCondition(
        seenAt,
        "neq",
        "2024-10-27T02:30:00.123456+02:00",
        1,
      ),
    ).toEqual({
      sql: "UNIX_TIMESTAMP(`seen_at`) != CAST(? AS DECIMAL(20,6))",
      params: ["1729989000.123456"],
    });
    expect(
      driver.buildFilterCondition(
        seenAt,
        "gte",
        "2024-10-27 01:30:00.500-05:30",
        1,
      ),
    ).toEqual({
      sql: "UNIX_TIMESTAMP(`seen_at`) >= CAST(? AS DECIMAL(20,6))",
      params: ["1730012400.5"],
    });
  });

  it("keeps BETWEEN and IN exact when operands mix offset and local values", () => {
    expect(
      driver.buildFilterCondition(
        seenAt,
        "between",
        ["2024-10-27T00:30:00Z", "2024-10-27T01:30:00Z"],
        1,
      ),
    ).toEqual({
      sql: "UNIX_TIMESTAMP(`seen_at`) BETWEEN CAST(? AS DECIMAL(20,6)) AND CAST(? AS DECIMAL(20,6))",
      params: ["1729989000", "1729992600"],
    });
    expect(
      driver.buildFilterCondition(
        seenAt,
        "between",
        ["2024-10-27 00:00:00", "2024-10-27T01:30:00Z"],
        1,
      ),
    ).toEqual({
      sql: "(`seen_at` >= ? AND UNIX_TIMESTAMP(`seen_at`) <= CAST(? AS DECIMAL(20,6)))",
      params: ["2024-10-27 00:00:00", "1729992600"],
    });
    expect(
      driver.buildFilterCondition(
        seenAt,
        "in",
        "2024-10-27T00:30:00Z, 2024-10-27T01:30:00Z",
        1,
      ),
    ).toEqual({
      sql: "UNIX_TIMESTAMP(`seen_at`) IN (CAST(? AS DECIMAL(20,6)), CAST(? AS DECIMAL(20,6)))",
      params: ["1729989000", "1729992600"],
    });
    expect(
      driver.buildFilterCondition(
        seenAt,
        "in",
        "2024-10-27T00:30:00Z, 2024-10-27 05:00:00",
        1,
      ),
    ).toEqual({
      sql: "(`seen_at` IN (?) OR UNIX_TIMESTAMP(`seen_at`) IN (CAST(? AS DECIMAL(20,6))))",
      params: ["2024-10-27 05:00:00", "1729989000"],
    });
  });

  it("leaves local TIMESTAMP and all DATETIME filters on the column itself", () => {
    expect(
      driver.buildFilterCondition(seenAt, "eq", "2024-10-27 02:30:00", 1),
    ).toEqual({ sql: "`seen_at` = ?", params: ["2024-10-27 02:30:00"] });
    expect(
      driver.buildFilterCondition(seenAt, "in", "2024-10-27 02:30:00", 1),
    ).toEqual({ sql: "`seen_at` IN (?)", params: ["2024-10-27 02:30:00"] });
    expect(
      driver.buildFilterCondition(createdAt, "eq", "2024-10-27T00:30:00Z", 1),
    ).toEqual({ sql: "`created_at` = ?", params: ["2024-10-27 00:30:00"] });
  });

  it.each([
    "1970-01-01T00:00:00Z",
    "1970-01-01T00:00:00.999999Z",
    "1970-01-01T01:00:00+02:00",
    "1969-12-31T23:59:59Z",
  ])("clamps %s below the first TIMESTAMP second and keeps zero dates out", (value) => {
    const fromFirstSecond = {
      sql: "UNIX_TIMESTAMP(`seen_at`) >= CAST(? AS DECIMAL(20,6))",
      params: ["1"],
    };
    const none = { sql: "1 = 0", params: [] };
    expect(driver.buildFilterCondition(seenAt, "gt", value, 1)).toEqual(
      fromFirstSecond,
    );
    expect(driver.buildFilterCondition(seenAt, "gte", value, 1)).toEqual(
      fromFirstSecond,
    );
    for (const operator of ["eq", "lt", "lte"] as const) {
      expect(driver.buildFilterCondition(seenAt, operator, value, 1)).toEqual(
        none,
      );
    }
    expect(driver.buildFilterCondition(seenAt, "neq", value, 1)).toEqual({
      sql: "`seen_at` IS NOT NULL",
      params: [],
    });
    expect(
      driver.buildFilterCondition(
        seenAt,
        "between",
        [value, "2024-01-01T00:00:00Z"],
        1,
      ),
    ).toEqual({
      sql: "(UNIX_TIMESTAMP(`seen_at`) >= CAST(? AS DECIMAL(20,6)) AND UNIX_TIMESTAMP(`seen_at`) <= CAST(? AS DECIMAL(20,6)))",
      params: ["1", "1704067200"],
    });
    expect(
      driver.buildFilterCondition(
        seenAt,
        "between",
        ["2024-01-01 00:00:00", value],
        1,
      ),
    ).toEqual({
      sql: "(`seen_at` >= ? AND 1 = 0)",
      params: ["2024-01-01 00:00:00"],
    });
    expect(driver.buildFilterCondition(seenAt, "in", value, 1)).toEqual(none);
    expect(
      driver.buildFilterCondition(
        seenAt,
        "in",
        `${value}, 2024-10-27T00:30:00Z`,
        1,
      ),
    ).toEqual({
      sql: "UNIX_TIMESTAMP(`seen_at`) IN (CAST(? AS DECIMAL(20,6)))",
      params: ["1729989000"],
    });
    expect(
      driver.buildFilterCondition(
        seenAt,
        "in",
        `${value}, 2024-10-27 05:00:00`,
        1,
      ),
    ).toEqual({ sql: "`seen_at` IN (?)", params: ["2024-10-27 05:00:00"] });
  });

  it("clamps a filter that lies entirely before the first TIMESTAMP second", () => {
    expect(
      driver.buildFilterCondition(
        seenAt,
        "between",
        ["1960-01-01T00:00:00Z", "1965-06-01T00:00:00Z"],
        1,
      ),
    ).toEqual({
      sql: "(UNIX_TIMESTAMP(`seen_at`) >= CAST(? AS DECIMAL(20,6)) AND 1 = 0)",
      params: ["1"],
    });
    expect(
      driver.buildFilterCondition(seenAt, "lt", "1965-06-01T00:00:00Z", 1),
    ).toEqual({ sql: "1 = 0", params: [] });
  });

  it("accepts the first representable TIMESTAMP second", () => {
    expect(
      driver.buildFilterCondition(seenAt, "gt", "1970-01-01T00:00:01Z", 1),
    ).toEqual({
      sql: "UNIX_TIMESTAMP(`seen_at`) > CAST(? AS DECIMAL(20,6))",
      params: ["1"],
    });
  });
});

describe("MySQL TIMESTAMP key comparisons with an explicit offset", () => {
  const driver = newDriver();

  it("uses an exact epoch comparison behind a +/-1 day index range", () => {
    expect(
      driver.buildOriginalValueComparisonWithParams(
        seenAt,
        "2024-10-27 00:30:00+00:00",
      ),
    ).toEqual({
      sql: "`seen_at` BETWEEN FROM_UNIXTIME(?) AND FROM_UNIXTIME(?) AND UNIX_TIMESTAMP(`seen_at`) = CAST(? AS DECIMAL(20,6))",
      params: [1729989000 - 86400, 1729989000 + 86401, "1729989000"],
    });
  });

  it.each([
    ["1970-01-01T12:00:00Z", "43200"],
    ["2038-01-18T12:00:00.5Z", "2147428800.5"],
  ])("drops the range prefilter where FROM_UNIXTIME bounds would be out of range (%s)", (value, epoch) => {
    expect(
      driver.buildOriginalValueComparisonWithParams(seenAt, value),
    ).toEqual({
      sql: "UNIX_TIMESTAMP(`seen_at`) = CAST(? AS DECIMAL(20,6))",
      params: [epoch],
    });
  });

  it("does not take over local TIMESTAMP or DATETIME values", () => {
    expect(
      driver.buildOriginalValueComparisonWithParams(
        seenAt,
        "2024-10-27 02:30:00",
      ),
    ).toBeNull();
    expect(
      driver.buildOriginalValueComparisonWithParams(
        createdAt,
        "2024-10-27T00:30:00Z",
      ),
    ).toBeNull();
  });

  it("rejects key values that would match zero dates", () => {
    expect(() =>
      driver.buildOriginalValueComparisonWithParams(
        seenAt,
        "1970-01-01 00:00:00+00:00",
      ),
    ).toThrow(
      "[RapiDB] Column seen_at expects a TIMESTAMP value after 1970-01-01 00:00:00 UTC.",
    );
  });

  it("guards and reads back an INSERT by the exact instant, not the local wall clock", () => {
    const key = { ...seenAt, isPrimaryKey: true, nullable: false };
    const amount = column("amount", "decimal(10,2)", "decimal");
    const operation: TransactionOperation = { sql: "INSERT", params: [] };
    const verification = prepareInsertVerification(
      driver,
      "mysql",
      "target",
      "",
      "events",
      { seen_at: "2024-10-27T02:30:00+02:00", amount: "1.25" },
      [key, amount],
      operation,
    );
    const exact =
      "`seen_at` BETWEEN FROM_UNIXTIME(?) AND FROM_UNIXTIME(?) AND UNIX_TIMESTAMP(`seen_at`) = CAST(? AS DECIMAL(20,6))";
    const params = [1729989000 - 86400, 1729989000 + 86401, "1729989000"];
    expect(operation.captureIdentity?.mysqlKeyGuard).toEqual({
      sql: `SELECT 1 FROM \`target\`.\`events\` WHERE ${exact} LIMIT 1 FOR UPDATE`,
      params,
    });
    expect(verification?.sql).toBe(
      `SELECT \`amount\` AS \`__col_0\` FROM \`target\`.\`events\` WHERE ${exact}`,
    );
    expect(verification?.params).toEqual(params);
  });
});

describe("MySQL TIMESTAMP writes with an explicit offset", () => {
  it("checks every offset value in one round trip on the transaction connection before writing", async () => {
    const { driver, statements } = harness((params) =>
      params.map((value) =>
        value === "2024-10-27 00:30:00"
          ? "1729989000.000000"
          : "1729992600.500",
      ),
    );
    const first = insertOperation(driver, {
      id: "1",
      seen_at: "2024-10-27T02:30:00+02:00",
      created_at: "2024-10-27 00:30:00",
      note: "2024-10-27 01:30:00.5",
    });
    const second = insertOperation(driver, {
      id: "2",
      seen_at: "2024-10-27T01:30:00.5Z",
    });
    const third = insertOperation(driver, {
      id: "3",
      seen_at: "2024-10-27T00:30:00Z",
    });
    await driver.runTransaction([first, second, third]);

    const sqls = statements.map((statement) => statement.sql);
    const check = statements.filter((statement) =>
      statement.sql.startsWith("SELECT @@session.time_zone"),
    );
    expect(check).toEqual([
      {
        sql: "SELECT @@session.time_zone AS __rapidb_time_zone, UNIX_TIMESTAMP(CONVERT_TZ(?, '+00:00', @@session.time_zone)) AS __rapidb_epoch_0, UNIX_TIMESTAMP(CONVERT_TZ(?, '+00:00', @@session.time_zone)) AS __rapidb_epoch_1",
        values: ["2024-10-27 00:30:00", "2024-10-27 01:30:00.5"],
      },
    ]);
    expect(sqls.indexOf(check[0].sql)).toBe(sqls.indexOf("BEGIN") + 1);
    expect(sqls.filter((sql) => sql.startsWith("INSERT"))).toHaveLength(3);
    expect(sqls.at(-1)).toBe("COMMIT");
  });

  it("refuses an instant in the repeated DST hour and writes nothing", async () => {
    const { driver, statements } = harness(() => [1729992600]);
    const operation = insertOperation(driver, {
      id: "1",
      seen_at: "2024-10-27T00:30:00Z",
    });
    const error = await driver
      .runTransaction([operation])
      .catch((caught: unknown) => caught);
    expect(error).toBeInstanceOf(MutationNotExecutedError);
    expect((error as Error).message).toBe(
      "[RapiDB] TIMESTAMP value 2024-10-27 00:30:00 UTC falls into a repeated local hour of the session time zone Europe/Berlin (for example when daylight saving time ends), so MySQL cannot store that exact instant. Nothing was written.",
    );
    const sqls = statements.map((statement) => statement.sql);
    expect(sqls.some((sql) => sql.startsWith("INSERT"))).toBe(false);
    expect(sqls).not.toContain("COMMIT");
    expect(sqls.at(-1)).toBe("ROLLBACK");
  });

  it("refuses an UPDATE SET in the repeated hour before running it", async () => {
    const { driver, statements } = harness(() => ["1729989000"]);
    const update = buildUpdateRowSql(
      driver,
      "target",
      "",
      "events",
      { id: 1 },
      { seen_at: "2024-10-27T01:30:00Z" },
      [id, seenAt],
    );
    if (!update) throw new Error("Expected UPDATE");
    await expect(
      driver.runTransaction([{ ...update, expectedAffectedRows: 1 }]),
    ).rejects.toThrow(
      "TIMESTAMP value 2024-10-27 01:30:00 UTC falls into a repeated local hour",
    );
    expect(statements.some((s) => s.sql.startsWith("UPDATE"))).toBe(false);
  });

  it.each([
    [0, "outside the range"],
    [null, "outside the range"],
    ["0.000000", "outside the range"],
  ])("refuses when the server cannot convert the instant (%s)", async (epoch, message) => {
    const { driver, statements } = harness(() => [epoch]);
    const operation = insertOperation(driver, {
      id: "1",
      seen_at: "2040-01-01T00:00:00Z",
    });
    await expect(driver.runTransaction([operation])).rejects.toThrow(message);
    expect(statements.some((s) => s.sql.startsWith("INSERT"))).toBe(false);
  });

  it("skips the check when no offset-bearing TIMESTAMP is written", async () => {
    const { driver, statements } = harness(() => []);
    const local = insertOperation(driver, {
      id: "1",
      seen_at: "2024-10-27 02:30:00",
      created_at: "2024-10-27T00:30:00Z",
      note: "2024-10-27 00:30:00",
    });
    const whereOnly = buildUpdateRowSql(
      driver,
      "target",
      "",
      "events",
      { id: 1 },
      { note: "x" },
      [id, seenAt, note],
      { seen_at: "2024-10-27T00:30:00Z" },
    );
    if (!whereOnly) throw new Error("Expected UPDATE");
    expect(whereOnly.sql).toContain("UNIX_TIMESTAMP(`seen_at`)");
    await driver.runTransaction([local, whereOnly]);
    expect(
      statements.some((s) => s.sql.startsWith("SELECT @@session.time_zone")),
    ).toBe(false);
    expect(statements.at(-1)?.sql).toBe("COMMIT");
  });
});

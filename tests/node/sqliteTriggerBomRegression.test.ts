import { afterEach, describe, expect, it, vi } from "vitest";
import { SQLiteDriver } from "../../src/extension/dbDrivers/sqlite";
import {
  SQLiteCoreDriver,
  splitSQLiteScript,
} from "../../src/extension/dbDrivers/sqliteCore";
import { openSQLiteDatabase } from "../../src/extension/dbDrivers/sqliteRuntime";
import * as sqliteInstaller from "../../src/extension/utils/sqliteInstaller";

const drivers: SQLiteCoreDriver[] = [];
const config = {
  id: "sqlite-trigger-bom-regression",
  name: "SQLite trigger BOM regression",
  type: "sqlite" as const,
  filePath: ":memory:",
};

afterEach(async () => {
  await Promise.all(drivers.splice(0).map((driver) => driver.disconnect()));
});

async function open(worker = false) {
  // Use only the installed runtime, never download or install one in this test.
  vi.spyOn(sqliteInstaller, "ensureSQLiteRuntimeInstalled").mockResolvedValue(
    null,
  );
  const driver = worker
    ? new SQLiteDriver(config)
    : new SQLiteCoreDriver(config, undefined, ["better-sqlite3"]);
  drivers.push(driver);
  await driver.connect();
  await driver.query("CREATE TABLE a(x)");
  return driver;
}

const trigger = "CREATE TRIGGER t AFTER INSERT ON a BEGIN SELECT 1; END";
const insert = "INSERT INTO a VALUES(1)";
const select = "SELECT x FROM a";
const validTriggers = [
  ["at script start", `\uFEFF${trigger}`],
  ["after a separator", `SELECT 0;\uFEFF${trigger}`],
  ["after comments", `/* before */\uFEFF${trigger}`],
  [
    "between header tokens",
    "CREATE \uFEFFTEMP /* between */\uFEFFTRIGGER t AFTER INSERT ON a BEGIN SELECT 1; END",
  ],
  [
    "before END and its separator",
    "CREATE TRIGGER t AFTER INSERT ON a BEGIN SELECT 1;\uFEFFEND \uFEFF;",
  ],
] as const;

describe("SQLite trigger BOM token boundaries", () => {
  it.each(
    validTriggers,
  )("keeps the trigger intact %s", (_label, definition) => {
    const statements = splitSQLiteScript(`${definition};${insert};${select}`);
    expect(statements).toHaveLength(definition.startsWith("SELECT") ? 4 : 3);
    expect(statements.slice(-2)).toEqual([insert, select]);
    expect(statements.at(-3)).toContain("SELECT 1;");
  });

  it("recognizes BOM before EXPLAIN without changing the complete state machine", () => {
    expect(splitSQLiteScript(`\uFEFFEXPLAIN ${trigger};${select}`)).toEqual([
      `EXPLAIN ${trigger}`,
      select,
    ]);
  });

  it("preserves internal BOM trivia and named parameter token text", () => {
    const definition =
      "CREATE \uFEFFTRIGGER t AFTER INSERT ON a BEGIN SELECT :\uFEFFx, @\uFEFFy, $\uFEFFz;\uFEFFEND";
    expect(splitSQLiteScript(`\uFEFF${definition};${select}`)).toEqual([
      definition,
      select,
    ]);
  });

  it.each([
    "\t",
    "\n",
    "\f",
    "\r",
    " ",
  ])("accepts SQLite ASCII whitespace %j", (space) => {
    expect(splitSQLiteScript(`${space}${trigger};${select}`)).toEqual([
      trigger,
      select,
    ]);
  });

  it.each([
    "\v",
    "\u00a0",
    "\u1680",
    "\u2003",
    "\u2028",
    "\u2029",
  ])("does not erase non-SQLite JS whitespace %j", async (space) => {
    const sql = `${space}SELECT 1`;
    expect(splitSQLiteScript(sql)).toEqual([sql]);
    const native = await openSQLiteDatabase(config, ["better-sqlite3"]);
    try {
      expect(() => native.allRaw(sql)).toThrow();
    } finally {
      native.close();
    }
    const driver = await open();
    await expect(driver.query(sql)).rejects.toThrow();
  });

  it.each([
    "CRE\uFEFFATE TRIGGER t AFTER INSERT ON a BEGIN SELECT 1; END",
    "CREATE\uFEFF TRIGGER t AFTER INSERT ON a BEGIN SELECT 1; END",
    "CREATE TRIGGER\uFEFF t AFTER INSERT ON a BEGIN SELECT 1; END",
    `${trigger}\uFEFF`,
  ])("does not repair BOM embedded in a token: %j", async (definition) => {
    const native = await openSQLiteDatabase(config, ["better-sqlite3"]);
    try {
      native.exec("CREATE TABLE a(x)");
      expect(() => native.allRaw(`${definition};`)).toThrow();
    } finally {
      native.close();
    }
    const driver = await open();
    await expect(driver.query(`${definition};${insert}`)).rejects.toThrow();
    expect((await driver.query("SELECT count(*) FROM a")).rows).toEqual([
      { __col_0: 0 },
    ]);
  });

  it.each([
    "x\uFEFF",
    "x\uFEFFy",
    "é\uFEFF",
    "x\u00a0",
    "x\u2003",
    "😀\uFEFF",
  ])("preserves Unicode identifier %j, including at statement end", async (name) => {
    const sql = `SELECT 1 AS ${name}`;
    expect(splitSQLiteScript(`${sql};`)).toEqual([sql]);
    const driver = await open();
    expect((await driver.query(sql)).columns).toEqual([name]);
  });

  it("does not treat END followed by embedded BOM as a terminator", () => {
    const sql = `${trigger}\uFEFF; SELECT 2; END`;
    expect(splitSQLiteScript(`${sql};${select}`)).toEqual([sql, select]);
  });
});

describe.each([
  false,
  true,
])("BOM scripts using real SQLite (worker=%s)", (worker) => {
  it.each(
    validTriggers,
  )("matches native exec %s and retains SELECTs", async (_label, definition) => {
    const sql = `${definition};${insert};${select};\uFEFFSELECT count(*) FROM a`;
    const native = await openSQLiteDatabase(config, ["better-sqlite3"]);
    try {
      native.exec("CREATE TABLE a(x)");
      native.exec(sql);
      expect(native.allRaw(select).rows).toEqual([[1]]);
    } finally {
      native.close();
    }
    const driver = await open(worker);
    const result = await driver.query(sql);
    expect(result.resultSets?.slice(-2).map((set) => set.rows)).toEqual([
      [{ __col_0: 1 }],
      [{ __col_0: 1 }],
    ]);
  });

  it("preserves BOM in literals, all identifier quote styles, and trigger comments", async () => {
    const driver = await open(worker);
    await driver.query(
      'CREATE TABLE log("\uFEFFdouble", `\uFEFFbacktick`, [\uFEFFbracket])',
    );
    const definition = `CREATE TRIGGER t AFTER INSERT ON a BEGIN
      -- \uFEFF END; CREATE TRIGGER
      /* \uFEFF END; */
      INSERT INTO log VALUES ('\uFEFF; END;', 'it''s\uFEFF', 'tail\uFEFF');
      SELECT CASE WHEN new.x THEN CASE new.x WHEN 1 THEN 1 END END;
    END`;
    expect(splitSQLiteScript(`\uFEFF${definition};${insert}`)).toEqual([
      definition,
      insert,
    ]);
    const result = await driver.query(
      `\uFEFF${definition};${insert};SELECT * FROM log;SELECT x FROM a`,
      [],
      { hardCap: 2 },
    );
    expect(result.columns).toEqual([
      "\uFEFFdouble",
      "\uFEFFbacktick",
      "\uFEFFbracket",
    ]);
    expect(result.resultSets?.map((set) => set.rows)).toEqual([
      [
        {
          __col_0: "\uFEFF; END;",
          __col_1: "it's\uFEFF",
          __col_2: "tail\uFEFF",
        },
      ],
      [{ __col_0: 1 }],
    ]);
    const stored = await driver.query(
      "SELECT sql FROM sqlite_master WHERE name='t'",
    );
    expect(stored.rows[0]?.__col_0).toBe(definition);
  });

  it("rolls back a BOM-prefixed trigger and earlier writes after RAISE", async () => {
    const driver = await open(worker);
    await expect(
      driver.query(`\uFEFF${insert};\uFEFFCREATE TRIGGER t BEFORE INSERT ON a
        BEGIN SELECT RAISE(ABORT, 'BOM rollback'); END;${insert}`),
    ).rejects.toMatchObject({
      executionState: "not-executed",
      message: expect.stringContaining("BOM rollback"),
    });
    expect((await driver.query("SELECT count(*) FROM a")).rows).toEqual([
      { __col_0: 0 },
    ]);
    expect(
      (await driver.query("SELECT count(*) FROM sqlite_master WHERE name='t'"))
        .rows,
    ).toEqual([{ __col_0: 0 }]);
  });

  it("does not execute BOM scripts after an expired deadline", async () => {
    const driver = await open(worker);
    await expect(
      driver.query(`\uFEFF${trigger};${insert}`, [], {
        deadline: Date.now() - 1,
      }),
    ).rejects.toMatchObject({ executionState: "not-executed" });
    expect((await driver.query("SELECT count(*) FROM a")).rows).toEqual([
      { __col_0: 0 },
    ]);
  });
});

it("rolls back BOM-prefixed scripts when the core completion deadline expires", async () => {
  const driver = await open();
  vi.spyOn(Date, "now").mockReturnValueOnce(0).mockReturnValue(200);
  await expect(
    driver.query(`\uFEFF${trigger};${insert}`, [], { deadline: 100 }),
  ).rejects.toMatchObject({ executionState: "not-executed" });
  vi.restoreAllMocks();
  expect((await driver.query("SELECT count(*) FROM a")).rows).toEqual([
    { __col_0: 0 },
  ]);
  expect(
    (await driver.query("SELECT count(*) FROM sqlite_master WHERE name='t'"))
      .rows,
  ).toEqual([{ __col_0: 0 }]);
});

it("does not dispatch an already cancelled BOM trigger script to the real worker", async () => {
  const driver = await open(true);
  const controller = new AbortController();
  controller.abort();
  await expect(
    driver.query(`\uFEFF${trigger};${insert}`, [], {
      signal: controller.signal,
    }),
  ).rejects.toMatchObject({ code: "NOT_EXECUTED", reason: "cancelled" });
  expect((await driver.query("SELECT count(*) FROM a")).rows).toEqual([
    { __col_0: 0 },
  ]);
});

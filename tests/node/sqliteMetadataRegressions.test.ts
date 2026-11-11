import { fork } from "node:child_process";
import { join } from "node:path";
import { build } from "esbuild";
import { describe, expect, it, vi } from "vitest";
import { sqliteWorkerConfig } from "../../esbuild.config.mjs";
import type { ConnectionConfig } from "../../src/extension/connectionManager";
import { isMutationNotExecutedError } from "../../src/extension/dbDrivers/mutationExecutionState";
import { SQLiteCoreDriver } from "../../src/extension/dbDrivers/sqliteCore";
import type { SQLiteWorkerResponse } from "../../src/extension/dbDrivers/sqliteWorkerProtocol";
import { createProjectTempDir } from "../runtime/tempDirectories";

const config = {
  id: "sqlite-metadata-regressions",
  name: "SQLite metadata regressions",
  type: "sqlite",
  filePath: "/tmp/sqlite-metadata-regressions.sqlite",
} as ConnectionConfig;

function driverWithDb(
  all: (sql: string, params?: unknown[]) => unknown[],
  get: (sql: string, params?: unknown[]) => unknown = () => null,
  attached = ["aux"],
) {
  const driver = new SQLiteCoreDriver(config);
  const databaseList = (sql: string, params?: unknown[]) =>
    sql === "PRAGMA database_list"
      ? [
          { seq: 0, name: "main", file: "/tmp/main.sqlite" },
          ...attached.map((name, index) => ({
            seq: index + 2,
            name,
            file: `/tmp/${index}.sqlite`,
          })),
        ]
      : all(sql, params);
  (driver as unknown as { db: unknown }).db = {
    isOpen: true,
    all: vi.fn(databaseList),
    get: vi.fn(get),
  };
  return driver;
}

describe("SQLite trigger metadata", () => {
  it("matches the trigger table case-insensitively", async () => {
    const all = vi.fn((sql: string, params?: unknown[]) => {
      expect(sql).toBe(
        `SELECT name, sql FROM "main".sqlite_master WHERE type='trigger' AND tbl_name = ? COLLATE NOCASE ORDER BY name`,
      );
      expect(params).toEqual(["users"]);
      return [
        {
          name: "tr",
          sql: "CREATE TRIGGER tr AFTER INSERT ON USERS BEGIN SELECT 1; END",
        },
      ];
    });
    const get = vi.fn((sql: string, params?: unknown[]) => {
      expect(sql).toBe(
        `SELECT sql FROM "main".sqlite_master WHERE type='trigger' AND tbl_name = ? COLLATE NOCASE AND name = ?`,
      );
      expect(params).toEqual(["users", "tr"]);
      return {
        sql: "CREATE TRIGGER tr AFTER INSERT ON USERS BEGIN SELECT 1; END",
      };
    });
    const driver = driverWithDb(all, get);

    await expect(driver.getTriggers("main", "main", "users")).resolves.toEqual([
      expect.objectContaining({
        name: "tr",
        timing: "after",
        events: ["insert"],
      }),
    ]);
    await expect(
      driver.getTriggerDDL("main", "main", "users", "tr"),
    ).resolves.toBe(
      "CREATE TRIGGER tr AFTER INSERT ON USERS BEGIN SELECT 1; END;",
    );
  });

  it.each([
    [
      "CREATE TRIGGER tr UPDATE OF a, b ON t BEGIN SELECT 1; END",
      "before",
      ["update"],
    ],
    ["CREATE TRIGGER tr DELETE ON t BEGIN SELECT 1; END", "before", ["delete"]],
    [
      "CREATE TRIGGER v_tr INSTEAD   OF INSERT ON v BEGIN SELECT 1; END",
      "instead_of",
      ["insert"],
    ],
    [
      "CREATE TRIGGER v_tr INSTEAD\nOF DELETE ON v BEGIN SELECT 1; END",
      "instead_of",
      ["delete"],
    ],
    [
      "CREATE TEMP TRIGGER IF NOT EXISTS tr BEFORE DELETE ON t BEGIN SELECT 1; END",
      "before",
      ["delete"],
    ],
    [
      'CREATE TEMPORARY TRIGGER "after insert" AFTER UPDATE ON t BEGIN SELECT 1; END',
      "after",
      ["update"],
    ],
    [
      'CREATE TRIGGER main."tr.update" /* c */ AFTER INSERT ON t BEGIN SELECT 1; END',
      "after",
      ["insert"],
    ],
    [
      "CREATE TRIGGER [aux].[before] INSERT ON t BEGIN SELECT 1; END",
      "before",
      ["insert"],
    ],
    [
      "CREATE TRIGGER after AFTER DELETE ON t BEGIN SELECT 1; END",
      "after",
      ["delete"],
    ],
  ])("parses timing and event from %s", async (sql, timing, events) => {
    const driver = driverWithDb(() => [{ name: "tr", sql }]);
    await expect(driver.getTriggers("main", "main", "t")).resolves.toEqual([
      expect.objectContaining({ timing, events }),
    ]);
  });
});

describe("SQLite row identity order", () => {
  it("ignores same-named triggers when checking for a rowid table", async () => {
    const driver = new SQLiteCoreDriver(config);
    const query = vi.spyOn(driver, "query").mockResolvedValue({
      columns: ["type"],
      rows: [{ __col_0: "table" }],
      rowCount: 1,
      executionTimeMs: 0,
    });

    await expect(
      driver.getTableRowIdentityOrderExpressions("main", "main", "users", []),
    ).resolves.toEqual(["rowid"]);
    expect(query).toHaveBeenCalledWith(
      `SELECT type FROM "main".sqlite_master WHERE type IN ('table','view') AND name = ?`,
      ["users"],
    );
  });
});

describe("SQLite schema resolution", () => {
  const tableXInfo = (sql: string) => {
    if (sql.includes(".table_xinfo(")) {
      return [
        {
          name: "id",
          type: "INTEGER",
          notnull: 0,
          dflt_value: null,
          pk: 1,
          hidden: 0,
        },
      ];
    }
    return [];
  };

  it("uses an attached schema requested explicitly", async () => {
    const all = vi.fn(tableXInfo);
    const driver = driverWithDb(all);
    await driver.describeTable("main", "aux", "t");
    await driver.describeTable("aux", "aux", "t");
    await driver.describeTable("aux", "AUX", "t");
    expect(
      all.mock.calls
        .map(([sql]) => sql)
        .filter((sql) => sql.includes(".table_xinfo(")),
    ).toEqual([
      'PRAGMA "aux".table_xinfo("t")',
      'PRAGMA "aux".table_xinfo("t")',
      'PRAGMA "aux".table_xinfo("t")',
    ]);
  });

  it("resolves attached schema names with surrounding spaces exactly", async () => {
    const all = vi.fn(tableXInfo);
    const driver = driverWithDb(all, undefined, [" aux ", "other"]);
    await driver.describeTable("main", " aux ", "t");
    await driver.describeTable("main", " AUX ", "t");
    await driver.describeTable("main", "other ", "t");
    await driver.describeTable(" aux ", "", "t");
    await driver.describeTable("main", "   ", "t");
    expect(
      all.mock.calls
        .map(([sql]) => sql)
        .filter((sql) => sql.includes(".table_xinfo(")),
    ).toEqual([
      'PRAGMA " aux ".table_xinfo("t")',
      'PRAGMA " aux ".table_xinfo("t")',
      'PRAGMA "other".table_xinfo("t")',
      'PRAGMA " aux ".table_xinfo("t")',
      'PRAGMA "main".table_xinfo("t")',
    ]);
    await expect(driver.describeTable("main", "aux", "t")).rejects.toThrow(
      'SQLite schema "aux" is not attached',
    );
  });

  it("rejects an explicitly requested schema that is not attached", async () => {
    const all = vi.fn(tableXInfo);
    const driver = driverWithDb(all);
    await expect(driver.describeTable("aux", "missing", "t")).rejects.toThrow(
      'SQLite schema "missing" is not attached',
    );
    await expect(driver.getTriggers("main", "missing", "t")).rejects.toThrow(
      'SQLite schema "missing" is not attached',
    );
    expect(
      all.mock.calls.some(([sql]) => sql.includes('"main".table_xinfo')),
    ).toBe(false);
  });

  it("defaults to the attached database or main when no schema is given", async () => {
    const all = vi.fn(tableXInfo);
    const driver = driverWithDb(all);
    await driver.describeTable("aux", "", "t");
    await driver.describeTable("db.sqlite", "", "t");
    await driver.describeTable("main", "temp", "t");
    expect(
      all.mock.calls
        .map(([sql]) => sql)
        .filter((sql) => sql.includes(".table_xinfo(")),
    ).toEqual([
      'PRAGMA "aux".table_xinfo("t")',
      'PRAGMA "main".table_xinfo("t")',
      'PRAGMA "temp".table_xinfo("t")',
    ]);
    await expect(driver.listSchemas("aux")).resolves.toEqual([{ name: "aux" }]);
    await expect(driver.listSchemas("db.sqlite")).resolves.toEqual([
      { name: "main" },
    ]);
  });
});

describe("SQLite query deadline", () => {
  it("reports an already expired query as not executed", async () => {
    const all = vi.fn(() => []);
    const driver = driverWithDb(all);
    const exec = vi.fn();
    (driver as unknown as { db: { exec: unknown } }).db.exec = exec;
    const error = await driver
      .query("DELETE FROM t", [], { deadline: Date.now() - 1 })
      .catch((caught: unknown) => caught);
    expect(error).toMatchObject({
      name: "MutationNotExecutedError",
      executionState: "not-executed",
      message: expect.stringContaining("deadline exceeded"),
    });
    expect(isMutationNotExecutedError(error)).toBe(true);
    expect(exec).not.toHaveBeenCalled();
    expect(all).not.toHaveBeenCalled();
  });
});

describe("SQLite worker deadline", () => {
  it("reports an already expired request as not executed", async () => {
    const directory = await createProjectTempDir("sqlite-worker-deadline");
    const workerFile = join(directory, "sqliteWorker.js");
    await build({ ...sqliteWorkerConfig, outfile: workerFile });
    const child = fork(workerFile, [], {
      execArgv: [],
      env: { ...process.env, ELECTRON_RUN_AS_NODE: "1" },
      serialization: "advanced",
      stdio: ["ignore", "ignore", "ignore", "ipc"],
    });
    try {
      const response = await new Promise<SQLiteWorkerResponse>(
        (resolve, reject) => {
          const timer = setTimeout(
            () => reject(new Error("SQLite worker did not respond")),
            5000,
          );
          child.once("message", (message: SQLiteWorkerResponse) => {
            clearTimeout(timer);
            resolve(message);
          });
          child.send({
            id: 1,
            method: "runTransaction",
            args: [[]],
            deadline: Date.now() - 1,
          });
        },
      );
      expect(response.error).toMatchObject({
        name: "MutationNotExecutedError",
        executionState: "not-executed",
        message: expect.stringContaining("deadline exceeded"),
      });
      expect(isMutationNotExecutedError(response.error)).toBe(true);
    } finally {
      const exit = new Promise<void>((resolve) =>
        child.once("exit", () => resolve()),
      );
      child.kill("SIGKILL");
      await exit;
    }
  }, 20000);
});

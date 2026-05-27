import { access, mkdir, readFile, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { SQLiteDriver } from "../../src/extension/dbDrivers/sqlite";
import {
  resolveBetterSqlite3LoadTargets,
  resolveSQLiteRuntimeTarget,
} from "../../src/extension/dbDrivers/sqliteRuntime";
import type { ConnectionConfig } from "../../src/shared/connectionConfig";
import { createProjectTempDir } from "../runtime/tempDirectories.ts";

const openDrivers = new Set<SQLiteDriver>();

afterEach(async () => {
  for (const driver of openDrivers) {
    await driver.disconnect();
  }
  openDrivers.clear();
});

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
  it("enables foreign keys and negotiates WAL on writable connections", async () => {
    const { driver } = await createDriver();

    await driver.connect();

    await driver.query("CREATE TABLE parents (id INTEGER PRIMARY KEY)");
    await driver.query(
      "CREATE TABLE children (id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES parents(id))",
    );

    expect(await readSingleValue(driver, "PRAGMA foreign_keys")).toBe(1);
    expect(await readSingleValue(driver, "PRAGMA journal_mode")).toBe("wal");
    await expect(
      driver.query("INSERT INTO children (id, parent_id) VALUES (1, 999)"),
    ).rejects.toThrow(/foreign key/i);
  });

  it("forces DELETE journal mode when sqliteWalMode is off", async () => {
    const { driver } = await createDriver({ sqliteWalMode: "off" });

    await driver.connect();

    expect(await readSingleValue(driver, "PRAGMA journal_mode")).toBe("delete");
  });

  it("opens readonly databases without mutating journal mode", async () => {
    const { driver: writableDriver, filePath } = await createDriver({
      sqliteWalMode: "off",
    });
    await writableDriver.connect();
    await writableDriver.query("CREATE TABLE sample (id INTEGER PRIMARY KEY)");
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
      sqliteWalMode: "auto",
    } as ConnectionConfig);
    openDrivers.add(readonlyDriver);

    await readonlyDriver.connect();

    expect(await readSingleValue(readonlyDriver, "PRAGMA foreign_keys")).toBe(
      1,
    );
    expect(await readSingleValue(readonlyDriver, "PRAGMA journal_mode")).toBe(
      "delete",
    );
  });

  it("keeps WAL journal mode intact for readonly opens even when WAL is disabled in config", async () => {
    const { driver: writableDriver, filePath } = await createDriver({
      sqliteWalMode: "auto",
    });
    await writableDriver.connect();
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
      sqliteWalMode: "off",
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
      sqliteWalMode: "auto",
    } as ConnectionConfig);
    openDrivers.add(driver);

    const error = await driver.connect().catch((failure: unknown) => failure);

    expect(error).toBeInstanceOf(Error);
    expect((error as Error).message).toContain(filePath);
    expect((error as Error).message).toContain("read-only mode");
    await expect(access(filePath)).rejects.toThrow();
  });

  it("ships better-sqlite3 runtime helper packages in the VSIX", async () => {
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
    expect(vscodeIgnore).toContain("!.rapidb-vscode/better-sqlite3/**");
    expect(vscodeIgnore).not.toContain("!node_modules/better-sqlite3/**");
  });

  it("prefers the staged VS Code SQLite runtime before the default package", async () => {
    const rootDir = await createProjectTempDir("sqlite-runtime-stage");
    const stagedPackage = join(
      rootDir,
      ".rapidb-vscode",
      "better-sqlite3",
      "darwin-arm64",
      "node_modules",
      "better-sqlite3",
    );
    await mkdir(stagedPackage, { recursive: true });
    await writeFile(
      join(rootDir, "package.json"),
      JSON.stringify({ name: "sqlite-runtime-stage" }),
      "utf8",
    );
    await writeFile(
      join(stagedPackage, "package.json"),
      JSON.stringify({ name: "better-sqlite3" }),
      "utf8",
    );

    expect(
      resolveBetterSqlite3LoadTargets(join(rootDir, "dist"), true),
    ).toEqual([stagedPackage, "better-sqlite3"]);
    expect(
      resolveBetterSqlite3LoadTargets(join(rootDir, "dist"), false),
    ).toEqual(["better-sqlite3"]);
  });

  it("resolves staged runtime targets across the supported platform matrix", () => {
    expect(resolveSQLiteRuntimeTarget("win32", "x64")).toBe("win32-x64");
    expect(resolveSQLiteRuntimeTarget("win32", "arm64")).toBe("win32-arm64");
    expect(resolveSQLiteRuntimeTarget("linux", "x64", "glibc")).toBe(
      "linux-x64",
    );
    expect(resolveSQLiteRuntimeTarget("linux", "arm64", "glibc")).toBe(
      "linux-arm64",
    );
    expect(resolveSQLiteRuntimeTarget("linux", "arm", "glibc")).toBeNull();
    expect(resolveSQLiteRuntimeTarget("linux", "x64", "musl")).toBe(
      "alpine-x64",
    );
    expect(resolveSQLiteRuntimeTarget("linux", "arm64", "musl")).toBe(
      "alpine-arm64",
    );
    expect(resolveSQLiteRuntimeTarget("darwin", "x64")).toBe("darwin-x64");
    expect(resolveSQLiteRuntimeTarget("darwin", "arm64")).toBe("darwin-arm64");
  });
});

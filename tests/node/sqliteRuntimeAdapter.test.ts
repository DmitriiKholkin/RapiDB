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

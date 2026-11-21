import { mkdir, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { afterEach, expect, it, vi } from "vitest";
import {
  allowSQLiteDevelopmentRuntime,
  openSQLiteDatabase,
  SQLiteRuntimeLoadError,
} from "../../src/extension/dbDrivers/sqliteRuntime";
import { createProjectTempDir } from "../runtime/tempDirectories";

const installer = vi.hoisted(() => ({
  ensure: null as null | (() => Promise<string | null>),
}));

vi.mock("../../src/extension/utils/sqliteInstaller", async (importOriginal) => {
  const actual =
    await importOriginal<
      typeof import("../../src/extension/utils/sqliteInstaller")
    >();
  return {
    ...actual,
    ensureSQLiteRuntimeInstalled: () =>
      installer.ensure
        ? installer.ensure()
        : actual.ensureSQLiteRuntimeInstalled(),
  };
});

afterEach(() => {
  installer.ensure = null;
  allowSQLiteDevelopmentRuntime(true);
});

function loadedDevelopmentPackages(): string[] {
  return Object.keys(require.cache).filter((path) =>
    /[\\/]node_modules[\\/]better-sqlite3[\\/]/.test(path),
  );
}

async function brokenManagedPackage(): Promise<string> {
  const root = await createProjectTempDir("sqlite-production-runtime");
  const packagePath = join(root, "node_modules", "better-sqlite3");
  await mkdir(join(packagePath, "lib"), { recursive: true });
  await writeFile(
    join(packagePath, "package.json"),
    JSON.stringify({ name: "better-sqlite3", main: "lib/index.js" }),
  );
  await writeFile(
    join(packagePath, "lib", "index.js"),
    "throw new Error('managed runtime failed to load');\n",
  );
  return packagePath;
}

it.each([
  [
    "the installation fails",
    async () => {
      throw new Error("SQLite runtime installation was not authorized.");
    },
    "was not authorized",
  ],
  [
    "the managed runtime fails to load",
    async () => brokenManagedPackage(),
    "managed runtime failed to load",
  ],
  [
    "no managed runtime is configured",
    async () => null,
    "No verified SQLite runtime",
  ],
])("never falls back to better-sqlite3 from node_modules in production when %s", async (_name, ensure, message) => {
  allowSQLiteDevelopmentRuntime(false);
  installer.ensure = ensure;
  const before = loadedDevelopmentPackages();

  const error = await openSQLiteDatabase({ filePath: ":memory:" }).catch(
    (caught) => caught,
  );

  expect(error).toBeInstanceOf(SQLiteRuntimeLoadError);
  expect(error.message).toContain(message);
  const attempted =
    /Attempted load targets: (.*?)\. Installed runtime probe/
      .exec(error.message)?.[1]
      ?.split(", ") ?? [];
  expect(attempted).not.toContain("better-sqlite3");
  expect(loadedDevelopmentPackages()).toEqual(before);
});

it("uses the development package only when explicitly allowed", async () => {
  installer.ensure = async () => brokenManagedPackage();
  const database = await openSQLiteDatabase({ filePath: ":memory:" });
  try {
    expect(database.get("SELECT 1 AS value")).toEqual({ value: 1 });
  } finally {
    database.close();
  }
});

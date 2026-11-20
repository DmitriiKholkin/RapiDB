import { fork } from "node:child_process";
import { once } from "node:events";
import { access, cp, mkdir, writeFile } from "node:fs/promises";
import { dirname, join } from "node:path";
import { expect, it, vi } from "vitest";
import {
  type SQLiteWorkerRequest,
  sqliteConnectionNow,
} from "../../src/extension/dbDrivers/sqliteWorkerProtocol";
import { createProjectTempDir } from "../runtime/tempDirectories";

it.each([
  "missing",
  "wrong",
])("uses the development package if repair of a cache with a %s manifest is declined", async (manifest) => {
  vi.resetModules();
  const {
    configureSQLiteInstaller,
    probeInstalledBetterSqlite3Runtime,
    resetSQLiteInstallerForTests,
  } = await import("../../src/extension/utils/sqliteInstaller");
  const { openSQLiteDatabase } = await import(
    "../../src/extension/dbDrivers/sqliteRuntime"
  );
  const storageRoot = await createProjectTempDir("sqlite-runtime-load-targets");
  const allowInstall = vi.fn(() => false);
  configureSQLiteInstaller({ storageRoot, allowInstall });
  try {
    const { installedPackagePath, installedBinaryPath } =
      probeInstalledBetterSqlite3Runtime(__dirname);
    if (!installedPackagePath || !installedBinaryPath) {
      throw new Error("Could not resolve managed SQLite fixture paths");
    }
    const runtimeRoot = dirname(dirname(installedPackagePath));
    await mkdir(dirname(installedBinaryPath), { recursive: true });
    for (const name of ["better-sqlite3", "bindings", "file-uri-to-path"]) {
      const source = dirname(require.resolve(`${name}/package.json`));
      await cp(source, join(runtimeRoot, "node_modules", name), {
        recursive: true,
        filter: (path) =>
          !["build", "src", "deps", "node_modules"].includes(
            path.slice(source.length + 1).split(/[\\/]/)[0],
          ),
      });
    }
    // The normal JS constructor is present, but the native artifact is incomplete.
    await writeFile(installedBinaryPath, "");
    if (manifest === "wrong") {
      await writeFile(join(runtimeRoot, "runtime.json"), '{"abi":"0"}');
    }

    const db = await openSQLiteDatabase({ filePath: ":memory:" });
    try {
      expect(db.get("SELECT 1 AS value")).toEqual({ value: 1 });
    } finally {
      db.close();
    }
    expect(allowInstall).toHaveBeenCalledOnce();
    expect(
      require.cache[require.resolve(installedPackagePath)],
    ).toBeUndefined();
  } finally {
    resetSQLiteInstallerForTests();
  }
});

it("checks the deadline again after the native probe and before opening the file", async () => {
  const directory = await createProjectTempDir("sqlite-worker-late-probe");
  const stubRuntime = join(directory, "stub-runtime.cjs");
  const filePath = join(directory, "must-not-open.sqlite");
  const probed = join(directory, "probe-completed");
  const deadline = Math.ceil(sqliteConnectionNow()) + 60_000;
  const lateNs = BigInt(deadline + 1) * 1_000_000n;
  await writeFile(
    stubRuntime,
    `module.exports = class Database {
      constructor(path) {
        if (path !== ":memory:") require("node:fs").writeFileSync(path, "");
      }
      prepare() { return { get: () => ({ ok: 1 }) }; }
      close() {
        require("node:fs").writeFileSync(${JSON.stringify(probed)}, "");
        process.hrtime.bigint = () => ${lateNs}n;
      }
    };`,
  );
  const workerPath = process.env.RAPIDB_SQLITE_WORKER_PATH;
  if (!workerPath) throw new Error("The SQLite worker bundle was not built");
  const worker = fork(workerPath, [], { execArgv: [] });
  const exit = once(worker, "exit");
  try {
    const message = once(worker, "message").then(([response]) => response);
    worker.send({
      method: "connect",
      id: 1,
      deadline,
      args: [],
      config: {
        id: "late-probe",
        name: "Late probe",
        type: "sqlite",
        filePath,
      },
      runtimeTargets: [stubRuntime],
    } satisfies SQLiteWorkerRequest);
    await expect(message).resolves.toMatchObject({
      id: 1,
      error: { executionState: "not-executed" },
    });
    await expect(access(probed)).resolves.toBeUndefined();
    await expect(access(filePath)).rejects.toMatchObject({ code: "ENOENT" });
  } finally {
    worker.kill();
    await exit;
  }
});

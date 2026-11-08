import { execFile } from "node:child_process";
import {
  chmodSync,
  mkdirSync,
  realpathSync,
  symlinkSync,
  writeFileSync,
} from "node:fs";
import { readFile, writeFile } from "node:fs/promises";
import { delimiter, dirname, join, relative } from "node:path";
import { promisify } from "node:util";
import { afterEach, describe, expect, it, vi } from "vitest";
import { SQLiteWorkerClient } from "../../src/extension/dbDrivers/sqliteWorkerClient";
import { sqlReadOnlyQueryGuard } from "../../src/extension/utils/readOnlyGuards";
import { resolveSQLiteBuildTools } from "../../src/extension/utils/sqliteBuildTools";
import {
  configureSQLiteInstaller,
  ensureSQLiteRuntimeInstalled,
  probeInstalledBetterSqlite3Runtime,
  resetSQLiteInstallerForTests,
  sqliteNodeGypArguments,
} from "../../src/extension/utils/sqliteInstaller";
import type { ConnectionConfig } from "../../src/shared/connectionConfig";
import { createProjectTempDir } from "../runtime/tempDirectories";

const clients = new Set<SQLiteWorkerClient>();

afterEach(async () => {
  vi.restoreAllMocks();
  await Promise.all(Array.from(clients, (client) => client.close()));
  clients.clear();
  resetSQLiteInstallerForTests();
  vi.unstubAllEnvs();
});

async function startClient() {
  const directory = await createProjectTempDir("g8-sqlite-worker-client");
  const workerPath = join(directory, "fakeSqliteWorker.cjs");
  const logPath = join(directory, "requests.jsonl");
  await writeFile(
    workerPath,
    `const fs = require("node:fs");
const logPath = process.env.RAPIDB_SQLITE_WORKER_TEST_LOG;
process.on("message", (request) => {
  const sql = request.args && request.args[0];
  fs.appendFileSync(logPath, JSON.stringify({ pid: process.pid, method: request.method, sql }) + "\\n");
  if (request.method === "connect") {
    process.send({ id: request.id, value: "connected" });
    return;
  }
  if (sql === "HOLD") return;
  if (typeof sql === "string" && sql.startsWith("SLOW")) {
    setTimeout(() => process.send({ id: request.id, value: sql }), 150);
    return;
  }
  process.send({ id: request.id, value: sql ?? "ok" });
});
process.on("disconnect", () => process.exit(0));
`,
    "utf8",
  );
  vi.stubEnv("RAPIDB_SQLITE_WORKER_PATH", workerPath);
  vi.stubEnv("RAPIDB_SQLITE_WORKER_TEST_LOG", logPath);
  const config: ConnectionConfig = {
    id: "g8-sqlite-worker-client",
    name: "G8 SQLite worker client",
    type: "sqlite",
    filePath: join(directory, "db.sqlite"),
  };
  const client = new SQLiteWorkerClient();
  clients.add(client);
  await client.open(config, 2_000);
  return { client, logPath };
}

async function requests(logPath: string) {
  const content = await readFile(logPath, "utf8").catch(() => "");
  return content
    .split("\n")
    .filter(Boolean)
    .map((line) => JSON.parse(line) as { pid: number; sql?: string });
}

function rejection<T>(promise: Promise<T>): Promise<Error> {
  return promise.then(
    () => {
      throw new Error("Expected SQLite worker request to reject");
    },
    (error: unknown) => error as Error,
  );
}

describe("SQLite worker AbortSignal detach", () => {
  it("detaches an aborted active read without stopping the worker or session", async () => {
    const { client, logPath } = await startClient();
    const controller = new AbortController();
    const read = client.call(
      "query",
      ["SLOW_READ"],
      2_000,
      undefined,
      controller.signal,
      undefined,
      true,
    );
    const order: string[] = [];
    const next = client.call("query", ["after"], 2_000).then((value) => {
      order.push("next");
      return value;
    });
    await new Promise((resolve) => setTimeout(resolve, 20));
    controller.abort();
    const error = await rejection(read);
    order.push("read-rejected");
    expect(error.name).toBe("AbortError");
    expect(client.connected).toBe(true);
    expect(client.sessionLost).toBe(false);
    await expect(next).resolves.toBe("after");
    expect(order).toEqual(["read-rejected", "next"]);
    const log = (await requests(logPath)).filter((entry) => entry.sql);
    expect(log.map((entry) => entry.sql)).toEqual(["SLOW_READ", "after"]);
    expect(new Set(log.map((entry) => entry.pid)).size).toBe(1);
    await expect(client.call("query", ["again"], 2_000)).resolves.toBe("again");
  });

  it("stops the worker with a clear reason when a detached read exceeds its timeout", async () => {
    const { client } = await startClient();
    const controller = new AbortController();
    const read = client.call(
      "query",
      ["HOLD"],
      150,
      undefined,
      controller.signal,
      undefined,
      true,
    );
    const queued = rejection(client.call("query", ["queued"], 2_000));
    controller.abort();
    expect((await rejection(read)).name).toBe("AbortError");
    expect(client.connected).toBe(true);
    expect(await queued).toMatchObject({
      code: "NOT_EXECUTED",
      reason: "worker-stopped",
      message: expect.stringMatching(
        /cancelled background SQLite read exceeded its timeout/,
      ),
    });
    expect(client.connected).toBe(false);
    expect(client.sessionLost).toBe(true);
  });

  it("stops the worker when an explicit cancel targets a request queued behind a detached read", async () => {
    const { client } = await startClient();
    const controller = new AbortController();
    const read = client.call(
      "query",
      ["HOLD"],
      10_000,
      undefined,
      controller.signal,
      undefined,
      true,
    );
    const target = rejection(client.call("query", ["editor"], 10_000, 9));
    const other = rejection(client.call("query", ["other"], 10_000, 10));
    controller.abort();
    expect((await rejection(read)).name).toBe("AbortError");
    await client.cancel({
      reason: "manual",
      operationName: "query",
      requestToken: 9,
    });
    for (const [error, reason] of [
      [await target, "cancelled"],
      [await other, "worker-stopped"],
    ] as const) {
      expect(error).toMatchObject({
        code: "NOT_EXECUTED",
        reason,
        message: expect.stringMatching(
          /cancelled background SQLite read was still running/,
        ),
      });
    }
    expect(client.connected).toBe(false);
  });

  it("still removes aborted queued reads without executing them", async () => {
    const { client, logPath } = await startClient();
    const controller = new AbortController();
    const active = client.call("query", ["SLOW_ACTIVE"], 2_000);
    const queued = client.call(
      "query",
      ["queued"],
      2_000,
      undefined,
      controller.signal,
      undefined,
      true,
    );
    controller.abort();
    expect(await rejection(queued)).toMatchObject({ code: "NOT_EXECUTED" });
    await expect(active).resolves.toBe("SLOW_ACTIVE");
    expect((await requests(logPath)).map((entry) => entry.sql)).not.toContain(
      "queued",
    );
  });

  it("keeps kill semantics for aborted active mutations", async () => {
    const { client } = await startClient();
    const controller = new AbortController();
    const write = client.call(
      "query",
      ["SLOW_WRITE"],
      2_000,
      undefined,
      controller.signal,
    );
    await new Promise((resolve) => setTimeout(resolve, 20));
    controller.abort();
    expect(await rejection(write)).toMatchObject({
      code: "OUTCOME_UNKNOWN",
      executionState: "unknown",
    });
    expect(client.connected).toBe(false);
  });

  it("keeps kill semantics for explicit cancel of a detachable read", async () => {
    const { client } = await startClient();
    const read = client.call(
      "query",
      ["SLOW_READ"],
      2_000,
      7,
      new AbortController().signal,
      undefined,
      true,
    );
    await new Promise((resolve) => setTimeout(resolve, 20));
    await client.cancel({
      reason: "manual",
      operationName: "query",
      requestToken: 7,
    });
    expect(await rejection(read)).toMatchObject({ code: "OUTCOME_UNKNOWN" });
    expect(client.connected).toBe(false);
  });
});

describe("SQLite worker response delivery", () => {
  it("delivers a response that arrives after the deadline but before the timer fires", async () => {
    const { client } = await startClient();
    const now = Date.now.bind(Date);
    const pending = client.call(
      "query",
      ["SLOW_COMMIT"],
      2_000,
      undefined,
      undefined,
      now() + 1_000,
    );
    vi.spyOn(Date, "now").mockImplementation(() => now() + 10_000);
    await expect(pending).resolves.toBe("SLOW_COMMIT");
    vi.mocked(Date.now).mockRestore();
    expect(client.connected).toBe(true);
  });

  it("reports cancellation when disconnect happens while runtime installation is rejected", async () => {
    let allow!: (value: boolean) => void;
    const directory = await createProjectTempDir("g8-sqlite-install-cancel");
    configureSQLiteInstaller({
      storageRoot: join(directory, "storage"),
      allowInstall: () =>
        new Promise<boolean>((resolve) => {
          allow = resolve;
        }),
    });
    const client = new SQLiteWorkerClient();
    clients.add(client);
    const opening = rejection(
      client.open(
        {
          id: "g8-install-cancel",
          name: "G8 install cancel",
          type: "sqlite",
          filePath: join(directory, "db.sqlite"),
        },
        2_000,
      ),
    );
    for (let i = 0; i < 100 && !allow; i++)
      await new Promise((resolve) => setTimeout(resolve, 5));
    expect(allow).toBeTypeOf("function");
    await client.close();
    allow(false);
    expect((await opening).message).toMatch(/cancelled/);
    expect(client.connected).toBe(false);
  });
});

describe("SQLite read-only PRAGMA classification", () => {
  it.each([
    "PRAGMA table_info(t)",
    "PRAGMA main.table_info(t)",
    "PRAGMA \"main\".table_info('t')",
    "PRAGMA [main].table_xinfo(t)",
    "PRAGMA main . table_info (t)",
    "PRAGMA index_list(t)",
    "PRAGMA main.index_list('t')",
    "PRAGMA temp.index_info(idx)",
    "PRAGMA main.index_xinfo(idx)",
    "PRAGMA main.foreign_key_list(t)",
    "PRAGMA main.user_version",
    "PRAGMA main.journal_mode",
  ])("allows %s", (sql) => {
    expect(sqlReadOnlyQueryGuard(sql, "sqlite").allowed).toBe(true);
  });

  it.each([
    "PRAGMA user_version = 5",
    "PRAGMA main.user_version = 5",
    "PRAGMA main.user_version(5)",
    "PRAGMA journal_mode(WAL)",
    "PRAGMA main.journal_mode(WAL)",
    'PRAGMA "main".journal_mode(WAL)',
    "PRAGMA [main].journal_mode(WAL)",
    "PRAGMA main . journal_mode (WAL)",
    "PRAGMA main.journal_mode = WAL",
    "PRAGMA main.foreign_keys(ON)",
    "PRAGMA journal_mode WAL",
    "PRAGMA main.optimize",
    "PRAGMA main.wal_checkpoint(TRUNCATE)",
  ])("denies %s", (sql) => {
    expect(sqlReadOnlyQueryGuard(sql, "sqlite").allowed).toBe(false);
  });
});

async function installerFixture() {
  const root = await createProjectTempDir("g8-sqlite-installer-manifest");
  writeFileSync(join(root, "package.json"), "{}");
  const bundled = join(root, "node_modules", "better-sqlite3");
  mkdirSync(bundled, { recursive: true });
  writeFileSync(
    join(bundled, "package.json"),
    JSON.stringify({ name: "better-sqlite3", version: "12.10.0" }),
  );
  const allowInstall = vi.fn(() => false);
  configureSQLiteInstaller({
    storageRoot: join(root, "storage"),
    allowInstall,
  });
  const baseDir = join(root, "dist");
  const probe = probeInstalledBetterSqlite3Runtime(baseDir);
  if (!probe.installedPackagePath || !probe.installedBinaryPath)
    throw new Error("Expected managed runtime paths");
  mkdirSync(dirname(probe.installedBinaryPath), { recursive: true });
  writeFileSync(
    join(probe.installedPackagePath, "package.json"),
    JSON.stringify({ name: "better-sqlite3", version: "12.10.0" }),
  );
  writeFileSync(probe.installedBinaryPath, "cached binary");
  const manifestPath = join(
    dirname(dirname(probe.installedPackagePath)),
    "runtime.json",
  );
  return {
    baseDir,
    allowInstall,
    installedPackagePath: probe.installedPackagePath,
    manifestPath,
  };
}

function expectedManifest(overrides: Record<string, string> = {}) {
  return JSON.stringify({
    betterSqlite3Version: "12.10.0",
    runtime: process.versions.electron ? "electron" : "node",
    abi: process.versions.modules,
    platform: process.platform,
    arch: process.arch,
    libc: "",
    ...overrides,
  });
}

describe("SQLite cached runtime manifest validation", () => {
  it.each([
    ["corrupted", "{not json"],
    ["null", "null"],
    ["mismatched", expectedManifest({ abi: "0" })],
  ])("reinstalls through the authorization gate when runtime.json is %s", async (_name, manifest) => {
    const f = await installerFixture();
    writeFileSync(f.manifestPath, manifest);
    await expect(ensureSQLiteRuntimeInstalled(f.baseDir)).rejects.toThrow(
      "SQLite runtime installation was not authorized",
    );
    expect(f.allowInstall).toHaveBeenCalledOnce();
  });

  it("reinstalls when runtime.json is missing", async () => {
    const f = await installerFixture();
    await expect(ensureSQLiteRuntimeInstalled(f.baseDir)).rejects.toThrow(
      "SQLite runtime installation was not authorized",
    );
    expect(f.allowInstall).toHaveBeenCalledOnce();
  });

  it.skipIf(process.platform === "linux")(
    "uses the cached runtime without prompting when the manifest matches",
    async () => {
      const f = await installerFixture();
      writeFileSync(f.manifestPath, expectedManifest());
      await expect(ensureSQLiteRuntimeInstalled(f.baseDir)).resolves.toBe(
        f.installedPackagePath,
      );
      expect(f.allowInstall).not.toHaveBeenCalled();
    },
  );
});

function executable(path: string, content: string) {
  mkdirSync(dirname(path), { recursive: true });
  writeFileSync(path, content);
  chmodSync(path, 0o700);
}

function nodeInstallation(root: string) {
  const bin = join(root, "bin");
  const npmCli = join(root, "lib/node_modules/npm/bin/npm-cli.js");
  executable(join(bin, "node"), "fixture node, never executed");
  mkdirSync(dirname(npmCli), { recursive: true });
  writeFileSync(npmCli, "fixture npm, never executed");
  symlinkSync(relative(bin, npmCli), join(bin, "npm"));
  return bin;
}

async function buildToolsFixture() {
  const root = await createProjectTempDir("g8-sqlite-build-python");
  const packageRoot = join(root, "extension");
  const runtimeRoot = join(root, "runtime");
  const workspace = join(root, "workspace");
  for (const path of [packageRoot, runtimeRoot, workspace]) mkdirSync(path);
  return {
    root,
    workspace,
    nodeBin: nodeInstallation(join(root, "node-installation")),
    resolve(path: string[]) {
      return resolveSQLiteBuildTools(
        packageRoot,
        runtimeRoot,
        { PATH: path.join(delimiter) },
        [workspace],
      );
    },
  };
}

describe.skipIf(process.platform === "win32")(
  "SQLite source build Python resolution",
  () => {
    it("rejects a PATH python3 that resolves into the workspace and selects a trusted one", async () => {
      const f = await buildToolsFixture();
      const workspacePython = join(f.workspace, ".venv/bin/python3");
      executable(workspacePython, "workspace python, never executed");
      const aliasBin = join(f.root, "alias-bin");
      mkdirSync(aliasBin);
      symlinkSync(workspacePython, join(aliasBin, "python3"));
      const trusted = join(f.root, "python-installation/bin/python3.12");
      executable(trusted, "trusted python, never executed");
      const trustedBin = join(f.root, "python-bin");
      mkdirSync(trustedBin);
      symlinkSync(trusted, join(trustedBin, "python3"));
      const tools = f.resolve([f.nodeBin, aliasBin, trustedBin]);
      expect(tools.python).toBe(realpathSync(trusted));
      expect(f.resolve([f.nodeBin, aliasBin]).python).toBeUndefined();
    });

    it("falls back to python and leaves python undefined when none is trusted", async () => {
      const f = await buildToolsFixture();
      expect(f.resolve([f.nodeBin]).python).toBeUndefined();
      const python = join(f.nodeBin, "python");
      executable(python, "python, never executed");
      expect(f.resolve([f.nodeBin]).python).toBe(realpathSync(python));
    });

    it("passes the selected python to node-gyp explicitly", async () => {
      const root = await createProjectTempDir("g8-sqlite-gyp-python");
      const python = join(root, "python", "bin", "python3");
      const args = sqliteNodeGypArguments(
        join(root, "tools"),
        join(root, "headers"),
        "42.0.0",
        process.platform,
        process.arch,
        python,
      );
      expect(args).toContain(`--python=${python}`);
      const script = `const gyp = require(${JSON.stringify(require.resolve("node-gyp"))})(); gyp.parseArgv([process.execPath, "node-gyp", ...${JSON.stringify(args.slice(1))}]); console.log(JSON.stringify(gyp.opts));`;
      const { stdout } = await promisify(execFile)(process.execPath, [
        "-e",
        script,
      ]);
      expect(JSON.parse(stdout).python).toBe(python);
    });
  },
);

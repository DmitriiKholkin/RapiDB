import { writeFileSync } from "node:fs";
import { readFile, writeFile } from "node:fs/promises";
import { dirname, join } from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { SQLiteWorkerClient } from "../../src/extension/dbDrivers/sqliteWorkerClient";
import { sqlReadOnlyQueryGuard } from "../../src/extension/utils/readOnlyGuards";
import {
  configureSQLiteInstaller,
  ensureSQLiteRuntimeInstalled,
  probeInstalledBetterSqlite3Runtime,
  resetSQLiteInstallerForTests,
  setSQLiteHostForTests,
} from "../../src/extension/utils/sqliteInstaller";
import type { ConnectionConfig } from "../../src/shared/connectionConfig";
import { createProjectTempDir } from "../runtime/tempDirectories";
import {
  FIXTURE_TARGET,
  hostProbe,
  mockRegistry,
  pinFixturePackage,
  restoreSQLiteArtifacts,
} from "./sqliteRuntimeFixtures";

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
  const storageRoot = join(root, "storage");
  configureSQLiteInstaller({ storageRoot, allowInstall: () => true });
  setSQLiteHostForTests({ probe: hostProbe(FIXTURE_TARGET) });
  const pkg = pinFixturePackage();
  mockRegistry(() => ({ data: pkg.data }));
  const installedPackagePath = (await ensureSQLiteRuntimeInstalled()) as string;
  vi.restoreAllMocks();
  resetSQLiteInstallerForTests();
  const allowInstall = vi.fn(() => false);
  configureSQLiteInstaller({ storageRoot, allowInstall });
  setSQLiteHostForTests({ probe: hostProbe(FIXTURE_TARGET) });
  const probe = probeInstalledBetterSqlite3Runtime();
  expect(probe.installedPackagePath).toBe(installedPackagePath);
  return {
    allowInstall,
    installedPackagePath,
    manifestPath: join(dirname(dirname(installedPackagePath)), "runtime.json"),
  };
}

describe("SQLite cached runtime manifest validation", () => {
  afterEach(restoreSQLiteArtifacts);

  it.each([
    ["corrupted", "{not json"],
    ["null", "null"],
    [
      "for another version",
      JSON.stringify({ betterSqlite3Version: "12.10.0" }),
    ],
  ])("reinstalls through the authorization gate when runtime.json is %s", async (_name, manifest) => {
    const f = await installerFixture();
    writeFileSync(f.manifestPath, manifest);
    await expect(ensureSQLiteRuntimeInstalled()).rejects.toThrow(
      "SQLite runtime installation was not authorized",
    );
    expect(f.allowInstall).toHaveBeenCalledOnce();
  });

  it("uses the verified cached runtime without prompting", async () => {
    const f = await installerFixture();
    const urls = mockRegistry(() => {
      throw new Error("Unexpected download");
    });
    await expect(ensureSQLiteRuntimeInstalled()).resolves.toBe(
      f.installedPackagePath,
    );
    expect(f.allowInstall).not.toHaveBeenCalled();
    expect(urls).toEqual([]);
  });
});

import { readFile, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { SQLiteWorkerClient } from "../../src/extension/dbDrivers/sqliteWorkerClient";
import type { ConnectionConfig } from "../../src/shared/connectionConfig";
import { createProjectTempDir } from "../runtime/tempDirectories";

const clients = new Set<SQLiteWorkerClient>();

afterEach(async () => {
  await Promise.all(Array.from(clients, (client) => client.close()));
  clients.clear();
  vi.unstubAllEnvs();
});

async function startClient(): Promise<{
  client: SQLiteWorkerClient;
  config: ConnectionConfig;
  logPath: string;
}> {
  const directory = await createProjectTempDir("sqlite-worker-client");
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
  if (sql === "HOLD" || (typeof sql === "string" && sql.startsWith("INSERT"))) return;
  if (sql === "ROLLBACK_FAILURE") {
    process.send({ id: request.id, error: { name: "SQLiteRollbackFailure", message: "SQLite operation and rollback both failed" } });
    return;
  }
  if (sql === "DELAY") {
    setTimeout(() => process.send({ id: request.id, value: sql }), 180);
    return;
  }
  if (sql === "CRASH") process.exit(23);
  process.send({ id: request.id, value: sql ?? "ok" });
});
process.on("disconnect", () => process.exit(0));
`,
    "utf8",
  );
  vi.stubEnv("RAPIDB_SQLITE_WORKER_PATH", workerPath);
  vi.stubEnv("RAPIDB_SQLITE_WORKER_TEST_LOG", logPath);

  const config: ConnectionConfig = {
    id: "sqlite-worker-client-test",
    name: "SQLite worker client test",
    type: "sqlite",
    filePath: join(directory, "db.sqlite"),
  };
  const client = new SQLiteWorkerClient();
  clients.add(client);
  await client.open(config, 2_000);
  return { client, config, logPath };
}

async function requests(logPath: string) {
  const content = await readFile(logPath, "utf8").catch(() => "");
  return content
    .split("\n")
    .filter(Boolean)
    .map(
      (line) =>
        JSON.parse(line) as { pid: number; method: string; sql?: string },
    );
}

function rejection<T>(promise: Promise<T>): Promise<Error> {
  return promise.then(
    () => {
      throw new Error("Expected SQLite worker request to reject");
    },
    (error: unknown) => error as Error,
  );
}

function diagnostic(error: Error) {
  return error as Error & {
    code?: string;
    executionState?: string;
    reason?: string;
    method?: string;
  };
}

describe("SQLite worker request queue failures", () => {
  it("marks a call before opening a connection as NOT_EXECUTED", async () => {
    const client = new SQLiteWorkerClient();
    clients.add(client);
    const error = await rejection(client.call("runTransaction", [[]], 2_000));
    expect(diagnostic(error)).toMatchObject({
      code: "NOT_EXECUTED",
      executionState: "not-executed",
      method: "runTransaction",
      reason: "disconnected",
    });
    expect(error.message).toContain("Use Connect to reconnect explicitly");
  });

  it("does not send mutations after an explicit disconnect", async () => {
    const { client, logPath } = await startClient();
    await client.close();
    const error = await rejection(client.call("runTransaction", [[]], 2_000));
    expect(diagnostic(error)).toMatchObject({
      code: "NOT_EXECUTED",
      executionState: "not-executed",
      reason: "disconnected",
    });
    expect((await requests(logPath)).map(({ method }) => method)).toEqual([
      "connect",
      "disconnect",
    ]);
  });

  it("stops the worker after rollback failure instead of reusing an unsafe session", async () => {
    const { client, logPath } = await startClient();
    const active = rejection(client.call("query", ["ROLLBACK_FAILURE"], 2_000));
    const queued = rejection(
      client.call("query", ["UPDATE sample SET value = 2"], 2_000),
    );

    const [activeError, queuedError] = await Promise.all([active, queued]);

    expect(activeError.message).toContain("rollback both failed");
    expect(diagnostic(activeError)).toMatchObject({
      code: "OUTCOME_UNKNOWN",
      executionState: "unknown",
    });
    expect(activeError.message).toContain("connection is closed");
    expect(diagnostic(queuedError)).toMatchObject({
      code: "NOT_EXECUTED",
      reason: "worker-stopped",
      method: "query",
    });
    expect(client.connected).toBe(false);
    expect((await requests(logPath)).map(({ method }) => method)).toEqual([
      "connect",
      "query",
    ]);
  });

  it("rejects queued mutations as NOT_EXECUTED when an active request times out, then reconnects in a new worker", async () => {
    const { client, config, logPath } = await startClient();
    const active = rejection(
      client.call("query", ["INSERT INTO sample VALUES (1)"], 200),
    );
    const queued = rejection(
      client.call("query", ["INSERT INTO sample VALUES (2)"], 2_000),
    );

    const activeError = await active;
    const queuedError = await queued;
    expect(activeError.name).toBe("DriverTimeoutError");
    expect(diagnostic(activeError)).toMatchObject({
      code: "OUTCOME_UNKNOWN",
      executionState: "unknown",
    });
    expect(activeError.message).toContain("OUTCOME_UNKNOWN");
    expect(activeError.message).toContain(
      "may have partially or fully persisted",
    );
    expect(activeError.message).toContain("connection is closed");
    expect(diagnostic(queuedError)).toMatchObject({
      name: "SQLiteWorkerNotExecutedError",
      code: "NOT_EXECUTED",
      executionState: "not-executed",
      reason: "worker-stopped",
      method: "query",
    });

    const beforeReconnect = await requests(logPath);
    expect(beforeReconnect.map(({ method }) => method)).toEqual([
      "connect",
      "query",
    ]);
    expect(client.connected).toBe(false);

    const unsentError = await rejection(
      client.call("runTransaction", [[]], 2_000),
    );
    expect(diagnostic(unsentError)).toMatchObject({
      code: "NOT_EXECUTED",
      executionState: "not-executed",
      method: "runTransaction",
    });
    expect(unsentError.message).not.toMatch(/OUTCOME_UNKNOWN|timed out/);
    expect(await requests(logPath)).toEqual(beforeReconnect);

    await client.open(config, 2_000);
    expect(client.sessionLost).toBe(false);
    await expect(client.call("query", ["SELECT 1"], 2_000)).resolves.toBe(
      "SELECT 1",
    );
    const afterReconnect = await requests(logPath);
    expect(afterReconnect.map(({ method }) => method)).toEqual([
      "connect",
      "query",
      "connect",
      "query",
    ]);
    expect(afterReconnect[0]?.pid).not.toBe(afterReconnect[2]?.pid);
  });

  it("expires a queued request at its own earlier deadline without poisoning the active worker", async () => {
    const { client, logPath } = await startClient();
    const active = client.call("query", ["DELAY"], 2_000);
    const queued = rejection(
      client.call(
        "query",
        ["INSERT INTO sample VALUES (2)"],
        2_000,
        undefined,
        undefined,
        Date.now() + 35,
      ),
    );

    const queueError = diagnostic(await queued);
    expect(queueError).toMatchObject({
      name: "DriverTimeoutError",
      code: "NOT_EXECUTED",
      executionState: "not-executed",
      reason: "expired",
    });
    expect(queueError.message).toContain("expired in the SQLite worker queue");
    await expect(active).resolves.toBe("DELAY");
    expect(client.connected).toBe(true);
    expect((await requests(logPath)).map(({ sql }) => sql)).toEqual([
      undefined,
      "DELAY",
    ]);
  });

  it("keeps an active unknown outcome separate from queued requests when the worker crashes", async () => {
    const { client, logPath } = await startClient();
    const active = rejection(client.call("query", ["CRASH"], 2_000));
    const queued = rejection(
      client.call("query", ["UPDATE sample SET value = 2"], 2_000),
    );

    const [activeError, queuedError] = await Promise.all([active, queued]);
    expect(activeError.message).toContain("process exited");
    expect(diagnostic(activeError).code).toBe("OUTCOME_UNKNOWN");
    expect(diagnostic(queuedError)).toMatchObject({
      code: "NOT_EXECUTED",
      reason: "worker-stopped",
    });
    const unsentError = await rejection(
      client.call("runTransaction", [[]], 2_000),
    );
    expect(diagnostic(unsentError)).toMatchObject({
      code: "NOT_EXECUTED",
      executionState: "not-executed",
    });
    expect(unsentError.message).not.toMatch(/OUTCOME_UNKNOWN|process exited/);
    expect((await requests(logPath)).map(({ method }) => method)).toEqual([
      "connect",
      "query",
    ]);
  });

  it("marks queued work NOT_EXECUTED when cancelling the active operation", async () => {
    const { client, logPath } = await startClient();
    const active = rejection(client.call("query", ["HOLD"], 5_000));
    const queued = rejection(
      client.call("query", ["DELETE FROM sample"], 5_000),
    );

    await vi.waitFor(async () =>
      expect((await requests(logPath)).map(({ method }) => method)).toContain(
        "query",
      ),
    );
    await client.cancel({ reason: "manual", operationName: "query" });
    const [activeError, queuedError] = await Promise.all([active, queued]);
    expect(diagnostic(activeError).code).toBe("OUTCOME_UNKNOWN");
    expect(diagnostic(queuedError)).toMatchObject({
      code: "NOT_EXECUTED",
      reason: "cancelled",
    });
    expect((await requests(logPath)).map(({ method }) => method)).toEqual([
      "connect",
      "query",
    ]);
  });

  it("rejects queued work as NOT_EXECUTED when disconnect stops an active worker", async () => {
    const { client, logPath } = await startClient();
    const active = rejection(client.call("query", ["HOLD"], 5_000));
    const queued = rejection(
      client.call("query", ["INSERT INTO sample VALUES (3)"], 5_000),
    );

    await vi.waitFor(async () =>
      expect((await requests(logPath)).map(({ method }) => method)).toContain(
        "query",
      ),
    );
    await client.close();
    const [activeError, queuedError] = await Promise.all([active, queued]);
    expect(activeError.message).toContain("SQLite disconnected");
    expect(diagnostic(activeError).code).toBe("OUTCOME_UNKNOWN");
    expect(diagnostic(queuedError)).toMatchObject({
      code: "NOT_EXECUTED",
      reason: "disconnected",
    });
    expect((await requests(logPath)).map(({ method }) => method)).toEqual([
      "connect",
      "query",
    ]);
  });
});

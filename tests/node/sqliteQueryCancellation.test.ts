import { readFile, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { SQLiteDriver } from "../../src/extension/dbDrivers/sqlite";
import { resetSQLiteInstallerForTests } from "../../src/extension/utils/sqliteInstaller";
import { createProjectTempDir } from "../runtime/tempDirectories";

let driver: SQLiteDriver | undefined;

afterEach(async () => {
  await driver?.disconnect();
  driver = undefined;
  resetSQLiteInstallerForTests();
  vi.unstubAllEnvs();
});

async function startDriver() {
  const directory = await createProjectTempDir("sqlite-query-cancellation");
  const worker = join(directory, "worker.cjs");
  const log = join(directory, "requests.jsonl");
  await writeFile(
    worker,
    `const fs = require("node:fs");
process.on("message", (request) => {
  fs.appendFileSync(process.env.RAPIDB_SQLITE_WORKER_TEST_LOG, JSON.stringify(request) + "\\n");
  if (request.args[0] !== "HOLD") process.send({ id: request.id, value: { columns: [], rows: [], rowCount: 0, executionTimeMs: 0 } });
});
process.on("disconnect", () => process.exit(0));
`,
  );
  vi.stubEnv("RAPIDB_SQLITE_WORKER_PATH", worker);
  vi.stubEnv("RAPIDB_SQLITE_WORKER_TEST_LOG", log);
  driver = new SQLiteDriver({
    id: "sqlite-query-cancellation",
    name: "SQLite query cancellation",
    type: "sqlite",
    filePath: join(directory, "db.sqlite"),
  });
  await driver.connect();
  const current = driver;
  const requests = async () =>
    (await readFile(log, "utf8"))
      .trim()
      .split("\n")
      .map((line) => JSON.parse(line));
  return { driver: current, requests };
}

describe("SQLiteDriver query cancellation and deadlines", () => {
  it("passes serializable query options and the effective deadline to the worker", async () => {
    const { driver, requests } = await startDriver();
    const controller = new AbortController();
    const deadline = Date.now() + 10_000;
    await driver.query("SELECT 1", [], {
      signal: controller.signal,
      deadline,
      requestToken: 42,
      hardCap: 12,
    });
    const request = (await requests())[1];
    expect(request.deadline).toBe(deadline);
    expect(request.args[2]).toEqual({
      deadline,
      requestToken: 42,
      hardCap: 12,
    });
  });

  it("does not dispatch an already aborted or expired query", async () => {
    const { driver, requests } = await startDriver();
    const controller = new AbortController();
    controller.abort();
    await expect(
      driver.query("ABORTED", [], { signal: controller.signal }),
    ).rejects.toMatchObject({
      code: "NOT_EXECUTED",
      reason: "cancelled",
    });
    await expect(
      driver.query("EXPIRED", [], { deadline: Date.now() - 1 }),
    ).rejects.toMatchObject({
      code: "NOT_EXECUTED",
      reason: "expired",
    });
    expect((await requests()).map((request) => request.method)).toEqual([
      "connect",
    ]);
    expect(driver.isConnected()).toBe(true);
  });

  it("cancels a queued query without stopping the active worker", async () => {
    const { driver, requests } = await startDriver();
    const active = driver.query("HOLD");
    const activeRejection = expect(active).rejects.toMatchObject({
      code: "OUTCOME_UNKNOWN",
    });
    const controller = new AbortController();
    const queued = driver.query("QUEUED", [], { signal: controller.signal });
    controller.abort();
    await expect(queued).rejects.toMatchObject({
      code: "NOT_EXECUTED",
      reason: "cancelled",
    });
    expect(
      (await requests()).every((request) => request.args[0] !== "QUEUED"),
    ).toBe(true);
    expect(driver.isConnected()).toBe(true);
    await driver.cancelCurrentOperation();
    await activeRejection;
  });

  it("expires a queued query without stopping the active worker", async () => {
    const { driver, requests } = await startDriver();
    const active = driver.query("HOLD");
    const activeRejection = expect(active).rejects.toMatchObject({
      code: "OUTCOME_UNKNOWN",
    });
    await expect(
      driver.query("QUEUED", [], { deadline: Date.now() + 50 }),
    ).rejects.toMatchObject({ code: "NOT_EXECUTED", reason: "expired" });
    expect(
      (await requests()).every((request) => request.args[0] !== "QUEUED"),
    ).toBe(true);
    expect(driver.isConnected()).toBe(true);
    await driver.cancelCurrentOperation();
    await activeRejection;
  });

  it.each([
    "abort",
    "deadline",
  ] as const)("stops an active query on %s", async (reason) => {
    const { driver, requests } = await startDriver();
    const controller = new AbortController();
    const pending = driver.query("HOLD", [], {
      signal: controller.signal,
      deadline: Date.now() + (reason === "deadline" ? 500 : 10_000),
    });
    const result = expect(pending).rejects.toMatchObject({
      code: "OUTCOME_UNKNOWN",
      executionState: "unknown",
    });
    await vi.waitFor(async () =>
      expect(
        (await requests()).some((request) => request.args[0] === "HOLD"),
      ).toBe(true),
    );
    if (reason === "abort") controller.abort();
    await result;
    expect(driver.isConnected()).toBe(false);
  });
});

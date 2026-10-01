import { join } from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { registerConnectionCommands } from "../../src/extension/commands/connectionCommands";
import { ConnectionManager } from "../../src/extension/connectionManager";
import type { IDBDriver } from "../../src/extension/dbDrivers/types";
import { QueryPanelController } from "../../src/extension/panels/queryPanelController";
import { TableMutationService } from "../../src/extension/table/tableMutationService";
import { TableReadService } from "../../src/extension/table/tableReadService";
import { connectWithProgress } from "../../src/extension/utils/connectOrchestration";
import { RAPIDB_COMMANDS } from "../../src/shared/commandIds";
import type { ConnectionConfig } from "../../src/shared/connectionConfig";
import { createProjectTempDir } from "../runtime/tempDirectories";
import {
  createExtensionContextStub,
  FakeConnectionManagerStore,
} from "../support/fakeConnectionManagerStore";

vi.mock("vscode", async () => {
  const { createMockVscodeModule } = await import("../support/mockVscode");
  return createMockVscodeModule().module;
});

const CPU_SUM =
  "WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<100000000) SELECT sum(x) FROM n";
const managers: ConnectionManager[] = [];
const controllers: QueryPanelController[] = [];

afterEach(async () => {
  await Promise.all(
    controllers.splice(0).map((controller) => controller.dispose()),
  );
  await Promise.all(managers.splice(0).map((manager) => manager.dispose()));
});

async function setup(filePath = ":memory:", type: "sqlite" | "pg" = "sqlite") {
  const context = createExtensionContextStub();
  const store = new FakeConnectionManagerStore();
  const connection: ConnectionConfig = {
    id: "session",
    name: "Session",
    type,
    filePath,
    ...(type === "pg"
      ? { host: "localhost", port: 5432, database: "test", username: "test" }
      : {}),
  };
  store.setConnections([connection]);
  store.setTimeoutSettings({ dbOperationTimeoutSeconds: 1 });
  const manager = new ConnectionManager(context as never, store);
  managers.push(manager);
  const postMessage = vi.fn();
  const controller = new QueryPanelController(manager, {
    getActiveConnectionId: () => connection.id,
    getInitialConnectionId: () => connection.id,
    getLastQueryResult: () => null,
    postMessage,
    setActiveConnectionId: vi.fn(),
    setLastQueryResult: vi.fn(),
    syncTitle: vi.fn(),
  });
  controllers.push(controller);
  const execute = (queryText: string) =>
    controller.handleMessage({
      type: "executeQuery",
      payload: { queryText, connectionId: connection.id },
    });
  const lastResult = () => {
    const message = postMessage.mock.calls
      .filter(([message]) => message.type === "queryResult")
      .at(-1)?.[0];
    if (!message) throw new Error("Controller did not post a query result");
    return message.payload as {
      error?: string;
      rows: Record<string, unknown>[];
    };
  };
  const factory = vi.spyOn(
    manager as unknown as {
      createDriver(config: ConnectionConfig): IDBDriver;
    },
    "createDriver",
  );
  const reads = new TableReadService(manager);
  const mutations = new TableMutationService(manager, reads);
  const commands = new Map<string, (...args: unknown[]) => unknown>();
  registerConnectionCommands(
    { context: context as never, connectionManager: manager, refresh: vi.fn() },
    (command, callback) => {
      commands.set(command, callback as (...args: unknown[]) => unknown);
      return { dispose() {} };
    },
  );
  const explicitConnect = async () => {
    const connect = commands.get(RAPIDB_COMMANDS.connect);
    if (!connect) throw new Error("Connect command was not registered");
    await connect({ connectionId: connection.id });
  };
  return {
    manager,
    controller,
    connection,
    store,
    execute,
    lastResult,
    factory,
    reads,
    mutations,
    explicitConnect,
  };
}

describe("SQLite lost session recovery through controller/manager/worker", () => {
  it.each([
    "timeout",
    "cancel",
  ] as const)("blocks Execute after %s without replacing memory or writing; explicit Connect intentionally resets memory", async (reason) => {
    const test = await setup();
    await test.manager.connectTo(test.connection.id, "explicit");
    const original = test.manager.getDriver(test.connection.id);
    if (!original) throw new Error("Missing real SQLite driver");
    await original.query(
      "CREATE TABLE kept(value INTEGER); INSERT INTO kept VALUES(42)",
    );
    await test.execute("SELECT value FROM kept");
    expect(test.lastResult().rows[0].__col_0).toBe(42);
    const query = vi.spyOn(original, "query");
    test.factory.mockClear();

    const running = test.execute(CPU_SUM);
    if (reason === "cancel") {
      await vi.waitFor(() =>
        expect(query).toHaveBeenCalledWith(
          CPU_SUM,
          undefined,
          expect.any(Object),
        ),
      );
      await new Promise((resolve) => setTimeout(resolve, 50));
      // Execute supersedes the CPU query through the controller's own targeted
      // cancellation handle. The successor must not reopen/write an empty DB.
      await test.execute(
        "CREATE TABLE kept(value INTEGER); INSERT INTO kept VALUES(99)",
      );
    }
    await running;
    if (reason === "timeout")
      expect(test.lastResult().error).toMatch(/timed out/);

    for (const sql of [
      "SELECT count(*) FROM sqlite_master WHERE name='kept'",
      "CREATE TABLE kept(value INTEGER); INSERT INTO kept VALUES(99)",
    ]) {
      await test.execute(sql);
      expect(test.lastResult().error).toMatch(
        /Automatic reconnect is disabled.*Use Connect.*new empty in-memory/,
      );
      expect(test.lastResult().rows).toEqual([]);
    }
    expect(query).toHaveBeenCalledTimes(1);
    expect(test.factory).not.toHaveBeenCalled();
    expect(test.manager.getDriver(test.connection.id)).toBe(original);
    expect(test.manager.isConnected(test.connection.id)).toBe(false);

    // Loss survives disposal of the old instance, not just its private flag.
    await test.manager.disconnectFrom(test.connection.id);
    expect(test.manager.getDriver(test.connection.id)).toBeUndefined();
    await test.execute(
      "CREATE TABLE kept(value INTEGER); INSERT INTO kept VALUES(99)",
    );
    expect(test.lastResult().error).toMatch(/Automatic reconnect is disabled/);
    expect(test.factory).not.toHaveBeenCalled();
    await expect(
      connectWithProgress(
        test.manager,
        test.connection.id,
        "Table auto-connect",
        true,
      ),
    ).rejects.toThrow(/Use Connect/);
    await expect(
      test.reads.getPage(test.connection.id, "main", "main", "kept", 0, 25, []),
    ).rejects.toThrow(/Use Connect/);
    await expect(
      test.mutations.insertRow(test.connection.id, "main", "main", "kept", {
        value: 99,
      }),
    ).rejects.toThrow(/Use Connect/);

    await test.explicitConnect();
    expect(test.manager.isConnected(test.connection.id)).toBe(true);
    expect(test.manager.getDriver(test.connection.id)).not.toBe(original);
    expect(
      test.manager.getAutomaticReconnectBlockReason(test.connection.id),
    ).toBeUndefined();
    await test.execute("SELECT count(*) FROM sqlite_master WHERE name='kept'");
    expect(test.lastResult().error).toBeUndefined();
    expect(test.lastResult().rows[0].__col_0).toBe(0);
    await test.execute(
      "CREATE TABLE kept(value INTEGER); INSERT INTO kept VALUES(99); SELECT value FROM kept",
    );
    expect(test.lastResult().error).toBeUndefined();
    expect(test.lastResult().rows[0].__col_0).toBe(99);
  });

  it("retains the recovery latch after a failed replacement and permits a new connection deliberately", async () => {
    const test = await setup();
    await test.manager.connectTo(test.connection.id, "explicit");
    await test.execute(CPU_SUM);
    expect(test.lastResult().error).toMatch(/timed out/);
    const workerPath = process.env.RAPIDB_SQLITE_WORKER_PATH;
    if (!workerPath) throw new Error("Missing test worker bundle");
    vi.stubEnv("RAPIDB_SQLITE_WORKER_PATH", join(workerPath, "missing.js"));
    await expect(
      test.manager.connectTo(test.connection.id, "explicit"),
    ).rejects.toThrow(/process exited/);
    expect(test.manager.getDriver(test.connection.id)).toBeUndefined();
    test.factory.mockClear();
    await test.execute(
      "CREATE TABLE kept(value INTEGER); INSERT INTO kept VALUES(99)",
    );
    expect(test.lastResult().error).toMatch(/Automatic reconnect is disabled/);
    expect(test.factory).not.toHaveBeenCalled();
    vi.stubEnv("RAPIDB_SQLITE_WORKER_PATH", workerPath);

    const fresh = await test.manager.saveConnection({
      ...test.connection,
      id: "fresh",
      name: "Fresh",
    });
    await test.controller.handleMessage({
      type: "executeQuery",
      payload: {
        connectionId: fresh.id,
        queryText: "SELECT count(*) FROM sqlite_master",
      },
    });
    expect(test.lastResult().error).toBeUndefined();
    expect(test.lastResult().rows[0].__col_0).toBe(0);
    expect(
      test.manager.getAutomaticReconnectBlockReason(test.connection.id),
    ).toMatch(/Use Connect/);
    await test.explicitConnect();
    expect(
      test.manager.getAutomaticReconnectBlockReason(test.connection.id),
    ).toBeUndefined();
  });

  it("requires explicit recovery for file ATTACH/session loss while retaining file data", async () => {
    const directory = await createProjectTempDir("sqlite-controller-session");
    const test = await setup(join(directory, "db.sqlite"));
    await test.manager.connectTo(test.connection.id, "explicit");
    const driver = test.manager.getDriver(test.connection.id);
    if (!driver) throw new Error("Missing real SQLite driver");
    await driver.query(
      "CREATE TABLE kept(value INTEGER); INSERT INTO kept VALUES(42); ATTACH ':memory:' AS attached; CREATE TABLE attached.session(value INTEGER); INSERT INTO attached.session VALUES(7)",
    );
    const query = vi.spyOn(driver, "query");
    const running = test.execute(CPU_SUM);
    await vi.waitFor(() =>
      expect(query).toHaveBeenCalledWith(
        CPU_SUM,
        undefined,
        expect.any(Object),
      ),
    );
    await test.execute("UPDATE kept SET value=99");
    await running;
    expect(test.lastResult().error).toMatch(
      /Use Connect.*ATTACH.*session state must be recreated/,
    );
    await test.manager.disconnectFrom(test.connection.id);
    test.factory.mockClear();
    await test.execute("UPDATE kept SET value=99");
    expect(test.factory).not.toHaveBeenCalled();
    expect(test.manager.isConnected(test.connection.id)).toBe(false);
    await test.explicitConnect();
    await test.execute("SELECT value FROM kept");
    expect(test.lastResult().rows[0].__col_0).toBe(42);
    const reopened = test.manager.getDriver(test.connection.id);
    expect(
      (await reopened?.listDatabases())?.map((database) => database.name),
    ).not.toContain("attached");
  });

  it("preserves ordinary automatic reconnect for other drivers", async () => {
    const test = await setup(undefined, "pg");
    test.factory.mockImplementation(() => {
      let connected = false;
      return {
        connect: async () => {
          connected = true;
        },
        disconnect: async () => {
          connected = false;
        },
        isConnected: () => connected,
        query: async () => ({
          columns: ["value"],
          rows: [{ __col_0: 42 }],
          rowCount: 1,
          executionTimeMs: 0,
        }),
      } as unknown as IDBDriver;
    });
    for (let attempt = 0; attempt < 2; attempt++) {
      await test.execute("SELECT 42");
      expect(test.manager.isConnected(test.connection.id)).toBe(true);
      expect(test.lastResult().error).toBeUndefined();
      expect(test.lastResult().rows[0].__col_0).toBe(42);
      await test.manager.disconnectFrom(test.connection.id);
    }
    expect(
      test.manager.getAutomaticReconnectBlockReason(test.connection.id),
    ).toBeUndefined();
  });
});

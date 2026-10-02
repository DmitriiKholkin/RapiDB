import type { createClient } from "redis";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { ConnectionManager } from "../../src/extension/connectionManager";
import { RedisDriver } from "../../src/extension/dbDrivers/redis";
import { createTimeoutAwareDriver } from "../../src/extension/dbDrivers/timeout";
import type { DriverTablePageRequest } from "../../src/extension/dbDrivers/types";
import type { DriverConnectionConfig } from "../../src/extension/driverRuntimeConfig";
import { TableReadService } from "../../src/extension/table/tableReadService";
import { logger } from "../../src/extension/utils/logger";

type Value = { type: string; value: unknown; ttl: number };
const harness = vi.hoisted(() => ({
  clients: [] as FakeRedis[],
  databases: new Map<number, Map<string, Value>>(),
  nextConnect: undefined as (() => Promise<void>) | undefined,
}));

vi.mock("redis", async (importOriginal) => {
  const actual = await importOriginal<typeof import("redis")>();
  return {
    ...actual,
    createClient: vi.fn((options: Parameters<typeof createClient>[0]) => {
      // Exercise the installed library's actual URI/option precedence without
      // opening a socket. In-memory command replies are deliberately DB-aware.
      const effective = actual.createClient(options).options;
      const client = new FakeRedis(effective);
      harness.clients.push(client);
      return client;
    }),
  };
});

function deferred() {
  let resolve: () => void = () => undefined;
  const promise = new Promise<void>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}

class FakeRedis {
  isOpen = false;
  database: number;
  scanGate?: Promise<void>;
  commandGate?: Promise<void>;
  readonly opening = harness.nextConnect;
  constructor(readonly options: ReturnType<typeof createClient>["options"]) {
    this.database = options.database ?? 0;
    harness.nextConnect = undefined;
  }
  on = vi.fn();
  connect = vi.fn(async () => {
    this.isOpen = true;
    await this.opening?.();
    // Deliberately model an uncancellable late socket open.
    this.isOpen = true;
    return this;
  });
  close = vi.fn(async () => {
    this.isOpen = false;
  });
  destroy = vi.fn(() => {
    this.isOpen = false;
  });
  select = vi.fn(async (database: number) => {
    this.database = database;
  });
  private store() {
    if (!this.isOpen) throw new Error("socket closed");
    let store = harness.databases.get(this.database);
    if (!store) {
      store = new Map();
      harness.databases.set(this.database, store);
    }
    return store;
  }
  info = vi.fn(async () =>
    [...harness.databases.keys()].map((db) => `db${db}:keys=3`).join("\n"),
  );
  scan = vi.fn(async (_cursor: string, options: { MATCH: string }) => {
    await this.scanGate;
    const prefix = options.MATCH.slice(0, -1);
    return {
      cursor: "0",
      keys: [...this.store().keys()].filter((key) => key.startsWith(prefix)),
    };
  });
  type = vi.fn(async (key: string) => this.store().get(key)?.type ?? "none");
  get = vi.fn(async (key: string) => this.store().get(key)?.value ?? null);
  ttl = vi.fn(async (key: string) => this.store().get(key)?.ttl ?? -2);
  lRange = vi.fn(async (key: string) => this.store().get(key)?.value ?? []);
  xRange = vi.fn(async (key: string) => this.store().get(key)?.value ?? []);
  set = vi.fn(
    async (
      key: string,
      value: string,
      options?: { NX?: boolean; EX?: number },
    ) => {
      const store = this.store();
      if (options?.NX && store.has(key)) return null;
      store.set(key, { type: "string", value, ttl: options?.EX ?? -1 });
      return "OK";
    },
  );
  del = vi.fn(async (key: string) => (this.store().delete(key) ? 1 : 0));
  sendCommand = vi.fn(async (args: string[]): Promise<unknown> => {
    await this.commandGate;
    const store = this.store();
    if (args[0] === "SELECT") {
      this.database = Number(args[1]);
      return "OK";
    }
    if (args[0] === "GET") return this.get(args[1]);
    if (args[0] === "SET") {
      const exIndex = args.indexOf("EX", 3);
      return this.set(args[1], args[2], {
        NX: args.includes("NX", 3),
        ...(exIndex === -1 ? {} : { EX: Number(args[exIndex + 1]) }),
      });
    }
    if (args[0] === "EVAL") {
      const source = args[3];
      const target = args[4];
      const value = store.get(source);
      if (!value) return 0;
      if (source !== target && store.has(target)) return -1;
      if (args[7] === "1") value.value = JSON.parse(args[8]);
      if (args[9] === "set") value.ttl = Number(args[10]);
      if (args[9] === "persist") value.ttl = -1;
      store.delete(source);
      store.set(target, value);
      return 1;
    }
    throw new Error(`Unexpected command ${args[0]}`);
  });
  withAbortSignal = vi.fn((signal: AbortSignal) => {
    const run = async <T>(action: () => Promise<T>): Promise<T> => {
      signal.throwIfAborted();
      let onAbort: () => void = () => undefined;
      try {
        return await Promise.race([
          action(),
          new Promise<never>((_resolve, reject) => {
            onAbort = () => reject(signal.reason);
            signal.addEventListener("abort", onAbort, { once: true });
          }),
        ]);
      } finally {
        signal.removeEventListener("abort", onAbort);
      }
    };
    return {
      sendCommand: (args: string[]) => run(() => this.sendCommand(args)),
      type: (key: string) => run(() => this.type(key)),
      set: (
        key: string,
        value: string,
        options?: { NX?: boolean; EX?: number },
      ) => run(() => this.set(key, value, options)),
      del: (key: string) => run(() => this.del(key)),
    };
  });
}

const config: DriverConnectionConfig = {
  id: "redis-scope",
  name: "Redis",
  type: "redis",
  host: "localhost",
};
const drivers: RedisDriver[] = [];
function driverWith(overrides: Partial<DriverConnectionConfig> = {}) {
  const driver = new RedisDriver({ ...config, ...overrides }, () => ({
    connectionTimeoutMs: 1000,
    connectionTimeoutSeconds: 1,
    dbOperationTimeoutMs: 1000,
    dbOperationTimeoutSeconds: 1,
  }));
  drivers.push(driver);
  return driver;
}
function page(database: string, table = "users"): DriverTablePageRequest {
  return {
    database,
    schema: "",
    table,
    page: 1,
    pageSize: 10,
    filters: [],
    sort: null,
    skipCount: false,
  };
}
function stored(database: number, key = "users:shared") {
  return harness.databases.get(database)?.get(key);
}

beforeEach(() => {
  harness.clients.length = 0;
  harness.databases.clear();
  harness.nextConnect = undefined;
  for (const db of [0, 1]) {
    harness.databases.set(
      db,
      new Map([
        [
          "users:shared",
          { type: "string", value: `value-${db}`, ttl: 10 + db },
        ],
        [
          "typed:shared",
          {
            type: db === 0 ? "string" : "list",
            value: db === 0 ? "plain" : ["one"],
            ttl: -1,
          },
        ],
        [
          "events:shared",
          {
            type: "stream",
            value: [{ id: "1-0", message: { db: String(db) } }],
            ttl: -1,
          },
        ],
        [`only${db}:key`, { type: "string", value: `only-${db}`, ttl: -1 }],
      ]),
    );
  }
});
afterEach(async () => {
  await Promise.allSettled(
    drivers.splice(0).map((driver) => driver.disconnect()),
  );
  vi.useRealTimers();
});

describe("B09 Redis reserved default prefix", () => {
  it("keeps all-keys and the real prefix distinct in discovery, both page paths, metadata and export", async () => {
    const store = new Map<string, Value>([
      ["orphan", { type: "list", value: ["outside"], ttl: -1 }],
      ["default:a", { type: "string", value: "a", ttl: -1 }],
      ["default:b", { type: "string", value: "b", ttl: -1 }],
      ["users:a", { type: "string", value: "outside", ttl: -1 }],
    ]);
    harness.databases.set(0, store);
    const driver = driverWith();
    await driver.connect();
    expect(
      (await driver.listObjects("db0")).map((entry) => entry.name),
    ).toEqual(["default", "default:", "users"]);
    expect(
      (await driver.readTablePage(page("db0", "default"))).totalCount,
    ).toBe(4);
    const service = new TableReadService({
      getConnection: () => config,
      getDriver: () => driver,
    } as unknown as ConnectionManager);
    for (const filters of [
      [],
      [{ column: "key", operator: "like" as const, value: "default:%" }],
    ]) {
      const result = await service.getPage(
        config.id,
        "db0",
        "",
        "default:",
        1,
        10,
        filters,
        { column: "value", direction: "desc" },
      );
      expect(result.rows.map((row) => row.key)).toEqual([
        "default:b",
        "default:a",
      ]);
    }
    expect(
      (await driver.readTablePage(page("db0", "default:"))).rows.map(
        (row) => row.key,
      ),
    ).toEqual(["default:a", "default:b"]);
    expect(
      (await driver.describeColumns("db0", "", "default:")).find(
        (column) => column.name === "value",
      )?.nativeType,
    ).toBe("string");
    expect(
      (await driver.describeColumns("db0", "", "default")).find(
        (column) => column.name === "value",
      )?.nativeType,
    ).toBe("mixed(string, list)");
    const keys = [];
    for await (const chunk of service.exportAll(
      config.id,
      "db0",
      "",
      "default:",
      1,
    ))
      keys.push(...chunk.rows.map((row) => row.key));
    expect(keys).toEqual(["default:a", "default:b"]);
    store.delete("orphan");
    expect(
      (await driver.listObjects("db0")).map((entry) => entry.name),
    ).toEqual(["default:", "users"]);
  });
});

describe("B02 Redis logical database isolation", () => {
  it("scopes discovery, metadata, both page paths, stream reads and export", async () => {
    const driver = driverWith();
    await driver.connect();
    expect((await driver.listDatabases()).map((db) => db.name)).toEqual([
      "db0",
      "db1",
    ]);
    const [zero, one] = await Promise.all([
      driver.listObjects("db0"),
      driver.listObjects("1"),
    ]);
    expect(zero.map((table) => table.name)).toContain("only0");
    expect(zero.map((table) => table.name)).not.toContain("only1");
    expect(one.map((table) => table.name)).toContain("only1");
    expect(
      (await driver.describeColumns("db1", "", "typed")).find(
        (col) => col.name === "value",
      )?.nativeType,
    ).toBe("list");
    expect(
      (await driver.describeTable("0", "", "typed")).find(
        (col) => col.name === "value",
      )?.type,
    ).toBe("string");
    expect((await driver.readTablePage(page("db0"))).rows).toEqual([
      { key: "users:shared", value: "value-0", ttl: 10 },
    ]);
    expect(
      (
        await driver.readTablePage({
          ...page("db1"),
          filters: [{ column: "value", operator: "eq", value: "value-1" }],
        })
      ).rows,
    ).toEqual([{ key: "users:shared", value: "value-1", ttl: 11 }]);
    expect(
      (await driver.readTablePage(page("db1", "events"))).rows[0]?.value,
    ).toContain('"db":"1"');
    const service = new TableReadService({
      getConnection: () => config,
      getDriver: () => driver,
    } as unknown as ConnectionManager);
    const chunks = [];
    for await (const chunk of service.exportAll(
      config.id,
      "db1",
      "",
      "users",
      1,
    ))
      chunks.push(...chunk.rows);
    expect(chunks).toEqual([
      { key: "users:shared", value: "value-1", ttl: 11 },
    ]);
    expect(harness.clients).toHaveLength(3); // editor base + two native clients
    for (const client of harness.clients)
      expect(client.select).not.toHaveBeenCalled();
  });

  it("keeps CRUD and type-aware previews in the requested DB with identical keys", async () => {
    const driver = driverWith();
    await driver.connect();
    expect(
      await driver.buildMutationPreviewStatements(
        "update",
        "db1",
        "",
        "typed",
        { primaryKeys: { key: "typed:shared" }, changes: { value: '["new"]' } },
      ),
    ).toEqual(['DEL "typed:shared"', 'RPUSH "typed:shared" "new"']);
    expect(
      await driver.buildMutationPreviewStatements("insert", "0", "", "typed", {
        values: { key: "typed:new", value: "new" },
      }),
    ).toEqual(['SET "typed:new" "new" "NX"']);
    expect(
      await driver.buildMutationPreviewStatements("insert", "1", "", "typed", {
        values: { key: "typed:new", value: '["new"]' },
      }),
    ).toEqual(['SET "typed:new" "[\\"new\\"]" "NX"']);
    await driver.updateRows({
      database: "db1",
      schema: "",
      table: "users",
      updates: [
        {
          primaryKeys: { key: "users:shared" },
          changes: { value: "changed", ttl: 50 },
          originalValues: { value: "value-1" },
        },
      ],
    });
    expect(stored(0)?.value).toBe("value-0");
    expect(stored(1)).toEqual({ type: "string", value: "changed", ttl: 50 });
    await driver.insertRow({
      database: "1",
      schema: "",
      table: "users",
      values: { key: "users:new", value: "new", ttl: 30 },
    });
    expect(stored(0, "users:new")).toBeUndefined();
    expect(stored(1, "users:new")?.ttl).toBe(30);
    await driver.deleteRows({
      database: "db1",
      schema: "",
      table: "users",
      primaryKeyValuesList: [{ key: "users:shared" }],
    });
    expect(stored(1)).toBeUndefined();
    expect(stored(0)?.value).toBe("value-0");
  });

  it("deduplicates in-flight opens for concurrent panels and holds each operation's client", async () => {
    const driver = driverWith();
    await driver.connect();
    const opening = deferred();
    harness.nextConnect = () => opening.promise;
    const first = driver.readTablePage(page("db1"));
    const second = driver.describeColumns("1", "", "users");
    const zero = driver.readTablePage(page("db0"));
    expect(harness.clients).toHaveLength(3);
    await driver.query("SELECT 1");
    expect((await zero).rows[0]?.value).toBe("value-0");
    opening.resolve();
    expect((await first).rows[0]?.value).toBe("value-1");
    await second;
    const one = harness.clients[1];
    const gate = deferred();
    one.scanGate = gate.promise;
    const blocked = driver.readTablePage(page("db1"));
    await driver.query("SELECT 0");
    expect((await driver.readTablePage(page("db0"))).rows[0]?.value).toBe(
      "value-0",
    );
    gate.resolve();
    expect((await blocked).rows[0]?.value).toBe("value-1");
    expect(one.database).toBe(1);
  });

  it("uses URI DB selection for the base and default scope while editor SELECT stays independent", async () => {
    const driver = driverWith({
      connectionUri: "redis://uri-user:p%40ss@redis.internal:6380/1",
    });
    await driver.connect();
    expect((await driver.query("GET users:shared")).rows[0]?.__col_0).toBe(
      "value-1",
    );
    await driver.query("SELECT 0");
    expect((await driver.query("GET users:shared")).rows[0]?.__col_0).toBe(
      "value-0",
    );
    expect((await driver.readTablePage(page(""))).rows[0]?.value).toBe(
      "value-1",
    );
    expect((await driver.readTablePage(page("db1"))).rows[0]?.value).toBe(
      "value-1",
    );
    expect(harness.clients[1].options).toMatchObject({
      database: 1,
      username: "uri-user",
      password: "p@ss",
      socket: { host: "redis.internal", port: 6380 },
    });
    await expect(
      driver.query("SELECT 0; GET users:shared", [], { database: "db1" }),
    ).resolves.toMatchObject({ rowCount: 1 });
    expect(harness.clients[2].destroy).toHaveBeenCalled();
    expect((await driver.readTablePage(page("db1"))).rows[0]?.value).toBe(
      "value-1",
    );
    expect((await driver.query("GET users:shared")).rows[0]?.__col_0).toBe(
      "value-0",
    );
    await driver.runTransaction(
      [{ sql: "SET users:shared scoped" }],
      undefined,
      { database: "db1" },
    );
    expect(stored(1)?.value).toBe("scoped");
    expect(stored(0)?.value).toBe("value-0");
  });

  it.each([
    "0",
    "db0",
  ])("explicit config %s overrides a URI DB and fallback discovery reports it", async (database) => {
    const driver = driverWith({
      database,
      connectionUri: "redis://localhost/1",
    });
    await driver.connect();
    expect(harness.clients[0].database).toBe(0);
    harness.clients[0].info.mockRejectedValueOnce(new Error("INFO denied"));
    expect(await driver.listDatabases()).toEqual([
      { name: "db0", schemas: [] },
    ]);
  });

  it("reuses URI auth, forwarded endpoint and TLS policy for every native DB client", async () => {
    const driver = driverWith({
      connectionUri: "rediss://uri-user:p%40ss@redis.internal:6380/1",
      tls: { mode: "requireTrustServerCertificate" },
      runtimeOverrides: {
        tlsServername: "redis.internal",
        transport: {
          kind: "tcpForward",
          localHost: "127.0.0.1",
          localPort: 16379,
          remoteHost: "redis.internal",
          remotePort: 6380,
        },
      },
    });
    await driver.connect();
    await Promise.all([
      driver.readTablePage(page("0")),
      driver.readTablePage(page("db1")),
    ]);
    for (const client of harness.clients) {
      expect(client.options).toMatchObject({
        username: "uri-user",
        password: "p@ss",
        socket: {
          host: "127.0.0.1",
          port: 16379,
          tls: true,
          servername: "redis.internal",
          rejectUnauthorized: false,
        },
      });
      expect(new URL(client.options.url ?? "").pathname).toBe(
        `/${client.database}`,
      );
    }
  });

  it.each([
    "db-1",
    "-1",
    "1.5",
    "dbx",
    "9007199254740992",
  ])("rejects invalid DB %s without falling back to the base", async (database) => {
    const driver = driverWith();
    await driver.connect();
    await expect(driver.readTablePage(page(database))).rejects.toThrow(
      "Invalid Redis logical database",
    );
    expect(harness.clients).toHaveLength(1);
  });

  it("evicts failed opens, preserves the original error and allows retry", async () => {
    const cleanupLog = vi
      .spyOn(logger, "error")
      .mockReturnValue(new Error("logged cleanup"));
    const driver = driverWith();
    await driver.connect();
    harness.nextConnect = async () => {
      throw new Error("DB access denied");
    };
    const failed = driver.readTablePage(page("db1"));
    harness.clients[1].destroy.mockImplementationOnce(() => {
      throw new Error("cleanup failed");
    });
    await expect(failed).rejects.toThrow("DB access denied");
    expect(cleanupLog).toHaveBeenCalledWith(
      "Redis client cleanup error",
      expect.any(Error),
    );
    // The failed cleanup remains tracked for a later disconnect retry.
    expect(harness.clients[1].isOpen).toBe(true);
    expect((driver as unknown as { clients: Set<unknown> }).clients.size).toBe(
      2,
    );
    expect(driver.isConnected()).toBe(true);
    expect((await driver.readTablePage(page("db1"))).rows[0]?.value).toBe(
      "value-1",
    );
    expect(harness.clients).toHaveLength(3);
  });

  it("rejects invalid configured and URI DB indices through the connect promise", async () => {
    for (const overrides of [
      { database: "db-1" },
      { connectionUri: "redis://localhost/1.5" },
    ]) {
      const driver = driverWith(overrides);
      await expect(driver.connect()).rejects.toThrow(
        "Invalid Redis logical database",
      );
      expect(driver.isConnected()).toBe(false);
    }
    expect(harness.clients).toHaveLength(0);
  });

  it("destroys all DB clients despite a cleanup error and retains the failed entry for retry", async () => {
    vi.spyOn(logger, "error").mockReturnValue(new Error("logged cleanup"));
    const driver = driverWith();
    await driver.connect();
    await Promise.all([
      driver.readTablePage(page("db0")),
      driver.readTablePage(page("db1")),
    ]);
    harness.clients[1].destroy.mockImplementationOnce(() => {
      throw new Error("destroy failed");
    });
    await expect(driver.disconnect()).rejects.toThrow("destroy failed");
    expect(driver.isConnected()).toBe(false);
    expect(harness.clients[1].isOpen).toBe(true);
    expect((driver as unknown as { clients: Set<unknown> }).clients.size).toBe(
      1,
    );
    for (const client of harness.clients)
      expect(client.destroy).toHaveBeenCalledTimes(1);
    await driver.disconnect();
    expect(harness.clients.every((client) => !client.isOpen)).toBe(true);
    expect((driver as unknown as { clients: Set<unknown> }).clients.size).toBe(
      0,
    );
    await driver.connect();
    await driver.readTablePage(page("db1"));
    expect(harness.clients).toHaveLength(5);
  });

  it("cleans in-flight scoped clients and late opens after disconnect without affecting a reconnect", async () => {
    const driver = driverWith();
    await driver.connect();
    const opening = deferred();
    harness.nextConnect = () => opening.promise;
    const pending = driver.readTablePage(page("db1"));
    const rejected = expect(pending).rejects.toThrow("cancelled");
    const late = harness.clients[1];
    await driver.disconnect();
    await rejected;
    await driver.connect();
    opening.resolve();
    await vi.waitFor(() => {
      expect(late.destroy.mock.calls.length).toBeGreaterThanOrEqual(2);
      expect(late.isOpen).toBe(false);
    });
    expect(driver.isConnected()).toBe(true);
    expect((await driver.readTablePage(page("db1"))).rows[0]?.value).toBe(
      "value-1",
    );
  });

  it("targets a cancelled base connect attempt, including late timeout cleanup", async () => {
    const driver = driverWith();
    const opening = deferred();
    harness.nextConnect = () => opening.promise;
    const pending = driver.connect();
    expect(driver.connect()).toBe(pending);
    const rejected = expect(pending).rejects.toThrow("cancelled");
    const late = harness.clients[0];
    driver.cancelConnectionAttempt(pending);
    await rejected;
    await driver.connect();
    opening.resolve();
    await vi.waitFor(() => expect(late.isOpen).toBe(false));
    driver.cancelConnectionAttempt(pending);
    expect(driver.isConnected()).toBe(true);
    expect(harness.clients[1].isOpen).toBe(true);
  });

  it("bounds scoped opens and cleans a socket which opens after its timeout", async () => {
    vi.useFakeTimers();
    const driver = driverWith();
    await driver.connect();
    const opening = deferred();
    harness.nextConnect = () => opening.promise;
    const pending = driver.readTablePage(page("db1"));
    const rejected = expect(pending).rejects.toThrow("timed out");
    const late = harness.clients[1];
    await vi.advanceTimersByTimeAsync(1001);
    await rejected;
    expect(driver.isConnected()).toBe(true);
    opening.resolve();
    await vi.advanceTimersByTimeAsync(0);
    expect(late.isOpen).toBe(false);
    expect((await driver.readTablePage(page("db1"))).rows[0]?.value).toBe(
      "value-1",
    );
  });

  it("cancels only the timed-out editor query while other DB panels keep working", async () => {
    vi.useFakeTimers();
    const driver = driverWith();
    await driver.connect();
    const gate = deferred();
    harness.clients[0].commandGate = gate.promise;
    const wrapped = createTimeoutAwareDriver(driver, () => ({
      connectionTimeoutMs: 1000,
      connectionTimeoutSeconds: 1,
      dbOperationTimeoutMs: 100,
      dbOperationTimeoutSeconds: 1,
    }));
    const pending = wrapped.query(
      "GET users:shared; SET users:shared wrong",
      [],
      { requestToken: 42 },
    );
    const rejected = expect(pending).rejects.toThrow("timed out");
    await vi.advanceTimersByTimeAsync(101);
    await rejected;
    expect((await driver.readTablePage(page("db1"))).rows[0]?.value).toBe(
      "value-1",
    );
    gate.resolve();
    await vi.advanceTimersByTimeAsync(0);
    expect(stored(0)?.value).toBe("value-0");
    expect(harness.clients[0].destroy).not.toHaveBeenCalled();
    expect(driver.isConnected()).toBe(true);
  });

  it("aborting a mutation waiting for a shared open leaves another panel's client usable", async () => {
    const driver = driverWith();
    await driver.connect();
    const opening = deferred();
    harness.nextConnect = () => opening.promise;
    const abort = new AbortController();
    const mutation = driver.deleteRows(
      {
        database: "db1",
        schema: "",
        table: "users",
        primaryKeyValuesList: [{ key: "users:shared" }],
      },
      { signal: abort.signal, deadline: Infinity },
    );
    const panel = driver.readTablePage(page("1"));
    const rejected = expect(mutation).rejects.toThrow("cancel mutation");
    abort.abort(new Error("cancel mutation"));
    await rejected;
    opening.resolve();
    expect((await panel).rows[0]?.value).toBe("value-1");
    expect(harness.clients[1].del).not.toHaveBeenCalled();
    expect(harness.clients[1].destroy).not.toHaveBeenCalled();
  });

  it("cancels a scoped query during connect and cleans its late open without closing native panels", async () => {
    const driver = driverWith();
    await driver.connect();
    await driver.readTablePage(page("db1"));
    const opening = deferred();
    harness.nextConnect = () => opening.promise;
    const pending = driver.query("SET users:shared wrong", [], {
      database: "1",
      requestToken: 90,
    });
    const rejected = expect(pending).rejects.toThrow();
    const late = harness.clients[2];
    driver.cancelCurrentOperation({
      reason: "timeout",
      timeoutKind: "dbOperation",
      operationName: "query",
      requestToken: 90,
    });
    await rejected;
    opening.resolve();
    await vi.waitFor(() => {
      expect(late.destroy.mock.calls.length).toBeGreaterThanOrEqual(2);
      expect(late.isOpen).toBe(false);
    });
    expect(late.sendCommand).not.toHaveBeenCalled();
    expect((await driver.readTablePage(page("db1"))).rows[0]?.value).toBe(
      "value-1",
    );
    expect(harness.clients[1].destroy).not.toHaveBeenCalled();
  });

  it("propagates transaction cancellation to a scoped session before its next command", async () => {
    const driver = driverWith();
    await driver.connect();
    const opening = deferred();
    const commands = deferred();
    harness.nextConnect = () => opening.promise;
    const abort = new AbortController();
    const pending = driver.runTransaction(
      [{ sql: "GET users:shared; SET users:shared wrong" }],
      { signal: abort.signal, deadline: Infinity },
      { database: "db1" },
    );
    const rejected = expect(pending).rejects.toThrow("transaction cancelled");
    const session = harness.clients[1];
    session.commandGate = commands.promise;
    opening.resolve();
    await vi.waitFor(() =>
      expect(session.sendCommand).toHaveBeenCalledTimes(1),
    );
    abort.abort(new Error("transaction cancelled"));
    await rejected;
    commands.resolve();
    expect(session.destroy).toHaveBeenCalled();
    expect(session.sendCommand).toHaveBeenCalledTimes(1);
    expect(stored(1)?.value).toBe("value-1");
    expect((await driver.readTablePage(page("db0"))).rows[0]?.value).toBe(
      "value-0",
    );
  });
});

describe("B06 Redis insert preview/execution parity", () => {
  const groups: Array<[string, string[]]> = [
    ["string", ["string", "string"]],
    ["hash", ["hash", "hash"]],
    ["list", ["list", "list"]],
    ["set", ["set", "set"]],
    ["zset", ["zset", "zset"]],
    ["stream", ["stream", "stream"]],
    ["mixed", ["hash", "list", "set", "zset", "string"]],
    ["empty", []],
  ];

  it.each(
    groups,
  )("inserts a string using SET NX in a %s group", async (_label, types) => {
    const store = harness.databases.get(1);
    if (!store) throw new Error("Missing db1 fixture");
    types.forEach((type, index) => {
      store.set(`group:${index}`, { type, value: "existing", ttl: 77 });
    });
    const before = [...store];
    const driver = driverWith();
    await driver.connect();
    const values = { key: "group:new", value: { nested: [1, false] }, ttl: 60 };
    const expected =
      'SET "group:new" "{\\"nested\\":[1,false]}" "EX" "60" "NX"';
    const sync = driver.buildMutationPreviewStatement(
      "insert",
      "db1",
      "",
      "group",
      { values },
    );
    const asyncPreview = await driver.buildMutationPreviewStatements(
      "insert",
      "db1",
      "",
      "group",
      { values },
    );
    expect(sync).toBe(expected);
    expect(asyncPreview).toEqual([expected]);
    expect(harness.clients).toHaveLength(1);

    await expect(
      driver.insertRow({ database: "db1", schema: "", table: "group", values }),
    ).resolves.toEqual({ affectedRows: 1 });
    const inserted = { type: "string", value: '{"nested":[1,false]}', ttl: 60 };
    expect(stored(1, values.key)).toEqual(inserted);
    expect(stored(0, values.key)).toBeUndefined();
    expect(harness.clients[1].set).toHaveBeenCalledExactlyOnceWith(
      values.key,
      inserted.value,
      { NX: true, EX: 60 },
    );
    for (const client of harness.clients) {
      expect(client.scan).not.toHaveBeenCalled();
      expect(client.type).not.toHaveBeenCalled();
      expect(client.del).not.toHaveBeenCalled();
    }

    // Replaying either preview must produce the same type, value and TTL.
    for (const preview of [sync, asyncPreview.join("\n")]) {
      store.delete(values.key);
      await driver.query(preview, [], { database: "db1" });
      expect(stored(1, values.key)).toEqual(inserted);
    }
    store.delete(values.key);
    expect([...store]).toEqual(before);
  });

  it.each([
    {},
    { ttl: 60 },
    { ttl: " 60 " },
    { ttl: null },
    { ttl: undefined },
    { ttl: -1 },
    { ttl: "-1" },
    { ttl: "" },
  ])("preserves a colliding typed key and its TTL with options %j", async (ttlValues) => {
    const driver = driverWith();
    await driver.connect();
    const values = { key: "typed:shared", value: "replacement", ...ttlValues };
    const expectedOptions =
      ttlValues.ttl === 60 || ttlValues.ttl === " 60 "
        ? { NX: true, EX: 60 }
        : { NX: true };
    const expected =
      expectedOptions.EX === undefined
        ? 'SET "typed:shared" "replacement" "NX"'
        : 'SET "typed:shared" "replacement" "EX" "60" "NX"';
    const sync = driver.buildMutationPreviewStatement(
      "insert",
      "db1",
      "",
      "typed",
      { values },
    );
    const asyncPreview = await driver.buildMutationPreviewStatements(
      "insert",
      "db1",
      "",
      "typed",
      { values },
    );
    expect(sync).toBe(expected);
    expect(asyncPreview).toEqual([expected]);
    const existing = { type: "list", value: ["one"], ttl: 83 };
    const store = harness.databases.get(1);
    if (!store) throw new Error("Missing db1 fixture");
    store.set(values.key, { ...existing });
    await expect(
      driver.insertRow({ database: "db1", schema: "", table: "typed", values }),
    ).resolves.toEqual({ affectedRows: 0 });
    expect(stored(1, values.key)).toEqual(existing);
    expect(harness.clients[1].set).toHaveBeenCalledExactlyOnceWith(
      values.key,
      values.value,
      expectedOptions,
    );
    for (const preview of [sync, asyncPreview.join("\n")]) {
      await driver.query(preview, [], { database: "1" });
      expect(stored(1, values.key)).toEqual(existing);
    }
    expect(stored(0, values.key)).toEqual({
      type: "string",
      value: "plain",
      ttl: -1,
    });
  });

  it.each([
    undefined,
    null,
    "",
    42,
    false,
    {},
  ])("rejects invalid insert key %j consistently", async (key) => {
    const driver = driverWith();
    const values = { key, value: "new" };
    expect(() =>
      driver.buildMutationPreviewStatement("insert", "db1", "", "group", {
        values,
      }),
    ).toThrow("Redis insert requires a 'key' field.");
    await expect(
      driver.buildMutationPreviewStatements("insert", "db1", "", "group", {
        values,
      }),
    ).rejects.toThrow("Redis insert requires a 'key' field.");
    await expect(
      driver.insertRow({ database: "db1", schema: "", table: "group", values }),
    ).rejects.toThrow("Redis insert requires a 'key' field.");
    expect(harness.clients).toHaveLength(0);
  });

  it.each([
    0,
    -2,
    1.5,
    "1.5",
    "invalid",
    {},
    "9007199254740992",
  ])("rejects invalid insert TTL %j consistently", async (ttl) => {
    const driver = driverWith();
    const values = { key: "group:new", value: "new", ttl };
    expect(() =>
      driver.buildMutationPreviewStatement("insert", "db1", "", "group", {
        values,
      }),
    ).toThrow("Redis TTL inserts require");
    await expect(
      driver.buildMutationPreviewStatements("insert", "db1", "", "group", {
        values,
      }),
    ).rejects.toThrow("Redis TTL inserts require");
    await expect(
      driver.insertRow({ database: "db1", schema: "", table: "group", values }),
    ).rejects.toThrow("Redis TTL inserts require");
    expect(harness.clients).toHaveLength(0);
  });
});

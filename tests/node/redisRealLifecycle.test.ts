import net from "node:net";
import { setTimeout as delay } from "node:timers/promises";
import tls from "node:tls";
import type { createClient } from "redis";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { RedisDriver } from "../../src/extension/dbDrivers/redis";
import { createTimeoutAwareDriver } from "../../src/extension/dbDrivers/timeout";
import { logger } from "../../src/extension/utils/logger";

type Client = ReturnType<typeof createClient>;
type Entry = { client: Client; abort: AbortController };
function state(driver: RedisDriver) {
  return driver as unknown as {
    clients: Set<Entry>;
    queries: Map<number, AbortController>;
    databaseClients: Map<number, Entry>;
  };
}

// Only TCP DNS timing is intercepted. Redis clients, transport, handshake,
// command queue, AbortSignal handling and destroy/close are the real library.
class RespServer {
  readonly sockets = new Set<net.Socket>();
  readonly commands: Array<{
    socket: net.Socket;
    database: number;
    args: string[];
  }> = [];
  readonly server = net.createServer((socket) => {
    this.sockets.add(socket);
    socket.on("close", () => this.sockets.delete(socket));
    socket.on("error", () => undefined);
    let input = Buffer.alloc(0);
    let database = 0;
    socket.on("data", (data) => {
      input = Buffer.concat([
        input,
        Buffer.isBuffer(data) ? data : Buffer.from(data),
      ]);
      while (input.length) {
        const parsed = this.parse(input);
        if (!parsed) break;
        input = input.subarray(parsed.consumed);
        const args = parsed.args;
        if (args[0] === "SELECT") database = Number(args[1]);
        this.commands.push({ socket, database, args });
        if (
          args[0] === "BLPOP" ||
          args.includes("hang") ||
          args.includes("blocked:*")
        )
          continue;
        if (args[0] === "HELLO") {
          socket.write("%1\r\n+server\r\n+redis\r\n");
        } else if (args[0] === "GET") {
          socket.write(this.bulk(`db-${database}`));
        } else if (args[0] === "SCAN") {
          socket.write(
            `*2\r\n${this.bulk("0")}*1\r\n${this.bulk("users:shared")}`,
          );
        } else if (args[0] === "TYPE") {
          socket.write("+string\r\n");
        } else if (args[0] === "TTL") {
          socket.write(":-1\r\n");
        } else {
          socket.write("+OK\r\n");
        }
      }
    });
  });

  private bulk(value: string) {
    return `$${Buffer.byteLength(value)}\r\n${value}\r\n`;
  }

  private parse(input: Buffer) {
    if (input[0] !== 42) return undefined;
    const firstEnd = input.indexOf("\r\n");
    if (firstEnd === -1) return undefined;
    const count = Number(input.subarray(1, firstEnd).toString());
    let offset = firstEnd + 2;
    const args: string[] = [];
    for (let index = 0; index < count; index += 1) {
      const headerEnd = input.indexOf("\r\n", offset);
      if (headerEnd === -1) return undefined;
      const size = Number(input.subarray(offset + 1, headerEnd).toString());
      offset = headerEnd + 2;
      if (input.length < offset + size + 2) return undefined;
      args.push(input.subarray(offset, offset + size).toString());
      offset += size + 2;
    }
    return { args, consumed: offset };
  }

  async listen() {
    await new Promise<void>((resolve) =>
      this.server.listen(0, "127.0.0.1", resolve),
    );
    return (this.server.address() as net.AddressInfo).port;
  }

  async close() {
    for (const socket of this.sockets) socket.destroy();
    await new Promise<void>((resolve) => this.server.close(() => resolve()));
  }
}

async function bounded<T>(promise: Promise<T>, ms = 500): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([
      promise,
      new Promise<never>((_resolve, reject) => {
        timer = setTimeout(
          () => reject(new Error("lifecycle did not settle")),
          ms,
        );
      }),
    ]);
  } finally {
    if (timer) clearTimeout(timer);
  }
}

const page = (database: string, table = "users") => ({
  database,
  schema: "",
  table,
  page: 1,
  pageSize: 10,
  filters: [],
  sort: null,
  skipCount: false,
});
let server: RespServer;
let port: number;
let delayNext: boolean;
let releaseLookups: Array<() => void>;
let sockets: net.Socket[];
let drivers: RedisDriver[];

function makeDriver(connectionTimeoutMs = 2000, tlsConnection = false) {
  const driver = new RedisDriver(
    {
      id: "real-redis",
      name: "Real Redis",
      type: "redis",
      connectionUri: `${tlsConnection ? "rediss" : "redis"}://127.0.0.1:${port}/0`,
      ...(tlsConnection
        ? { tls: { mode: "requireTrustServerCertificate" as const } }
        : {}),
    },
    () => ({
      connectionTimeoutMs,
      connectionTimeoutSeconds: 2,
      dbOperationTimeoutMs: 150,
      dbOperationTimeoutSeconds: 1,
    }),
  );
  drivers.push(driver);
  return driver;
}

beforeEach(async () => {
  vi.spyOn(logger, "error").mockReturnValue(new Error("logged"));
  server = new RespServer();
  port = await server.listen();
  sockets = [];
  drivers = [];
  delayNext = false;
  releaseLookups = [];
  const createConnection = net.createConnection;
  const connectTls = tls.connect;
  vi.spyOn(tls, "connect").mockImplementation((...args: unknown[]) => {
    const socket = connectTls(args[0] as tls.ConnectionOptions);
    sockets.push(socket);
    return socket;
  });
  vi.spyOn(net, "createConnection").mockImplementation((...args: unknown[]) => {
    let options = args[0] as net.TcpNetConnectOpts;
    if (delayNext) {
      delayNext = false;
      options = {
        ...options,
        host: "delayed.redis.test",
        lookup: (_hostname, lookupOptions, callback) => {
          releaseLookups.push(() => {
            callback(
              null,
              lookupOptions.all
                ? [{ address: "127.0.0.1", family: 4 }]
                : "127.0.0.1",
              4,
            );
          });
        },
      };
    }
    const socket = createConnection(options);
    sockets.push(socket);
    return socket;
  });
});

afterEach(async () => {
  for (const driver of drivers) {
    await bounded(driver.disconnect()).catch(() => undefined);
  }
  // Test teardown also cleans sockets when a regression assertion fails.
  for (const socket of sockets) socket.destroy();
  for (const release of releaseLookups.splice(0)) release();
  await server.close();
});

describe("B06 insert previews match real node-redis wire commands", () => {
  it.each([
    [{}, ""],
    [{ value: null }, ""],
    [{ value: false }, "false"],
    [{ value: 42 }, "42"],
    [{ value: { a: [1, false] }, ttl: 60 }, '{"a":[1,false]}'],
    [{ value: ["a", "b"], ttl: " 60 " }, '["a","b"]'],
    [{ json: { a: 1 }, ttl: -1 }, '{"a":1}'],
    [
      { text: '  quoted "value"; \\path  ', ttl: null },
      '  quoted "value"; \\path  ',
    ],
    [{ value: "", json: "ignored", text: "ignored", ttl: "" }, ""],
    [{ value: null, json: false, text: "ignored", ttl: undefined }, "false"],
    [{ value: "line1\nline2\t\0", ttl: 60 }, "line1\nline2\t\0"],
    [{ key: "group:\n\r\t\0", value: "\b\f\u0001\u001f" }, "\b\f\u0001\u001f"],
    [
      { key: "group:雪😀\u2028\u2029", value: "雪😀\u2028\u2029" },
      "雪😀\u2028\u2029",
    ],
    [
      {
        key: String.raw`group:\n\r\t\u0000\u96EA`,
        value: String.raw`\n\r\t\u0000\u96EA`,
      },
      String.raw`\n\r\t\u0000\u96EA`,
    ],
    [
      {
        key: 'group:\n\\n\0\\u0000";雪😀',
        value: '\r\t\\t\0\\u0000";雪😀',
        ttl: 60,
      },
      '\r\t\\t\0\\u0000";雪😀',
    ],
  ] as const)("matches normalized arguments and options for %j", async (input, storedValue) => {
    const driver = makeDriver();
    await driver.connect();
    const values = { key: ' group: "new"; \\key ', ...input };
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
    expect(asyncPreview).toEqual([sync]);
    const start = server.commands.length;
    await expect(
      driver.insertRow({ database: "db1", schema: "", table: "group", values }),
    ).resolves.toEqual({ affectedRows: 1 });
    const actual = server.commands
      .slice(start)
      .filter(({ args }) => !["CLIENT", "SELECT", "HELLO"].includes(args[0]));
    const ttl = "ttl" in input && (input.ttl === 60 || input.ttl === " 60 ");
    expect(actual).toHaveLength(1);
    expect(actual[0]).toMatchObject({
      database: 1,
      args: [
        "SET",
        values.key,
        storedValue,
        ...(ttl ? ["EX", "60"] : []),
        "NX",
      ],
    });

    for (const preview of [sync, asyncPreview.join("\n")]) {
      const previewStart = server.commands.length;
      await driver.query(preview, [], { database: "db1" });
      const replayed = server.commands
        .slice(previewStart)
        .filter(({ args }) => !["CLIENT", "SELECT", "HELLO"].includes(args[0]));
      expect(
        replayed.map(({ database, args }) => ({ database, args })),
      ).toEqual(actual.map(({ database, args }) => ({ database, args })));
    }
  });

  it("replays explicit Unicode escapes with the same wire bytes as native insertion", async () => {
    const driver = makeDriver();
    await driver.connect();
    const values = {
      key: 'group:\0雪😀";\\u0000\n',
      value: '\t\r\b\f\0雪😀";\\n',
    };
    const start = server.commands.length;
    await driver.insertRow({
      database: "db1",
      schema: "",
      table: "group",
      values,
    });
    // JSON permits Unicode escapes for every character, including surrogate
    // pairs. Exercise that spelling in both preview entry points too.
    const escapeUnicode = (preview: string) =>
      preview
        .replaceAll("雪", String.raw`\u96EA`)
        .replaceAll("😀", String.raw`\uD83D\uDE00`);
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
    for (const preview of [sync, asyncPreview.join("\n")]) {
      await driver.query(escapeUnicode(preview), [], { database: "db1" });
    }
    const commands = server.commands
      .slice(start)
      .filter(({ args }) => !["CLIENT", "SELECT", "HELLO"].includes(args[0]));
    expect(commands.map(({ database, args }) => ({ database, args }))).toEqual(
      Array.from({ length: 3 }, () => ({
        database: 1,
        args: ["SET", values.key, values.value, "NX"],
      })),
    );
  });
});

describe("B02 real node-redis 6 transport lifecycle", () => {
  it("cancels TCP creation before physical connect and prevents a late ready socket", async () => {
    const driver = makeDriver();
    delayNext = true;
    const pending = driver.connect();
    const rejected = expect(pending).rejects.toThrow("cancelled");
    await vi.waitFor(() => expect(releaseLookups).toHaveLength(1));
    const entry = [...state(driver).clients][0];
    const socket = sockets[0];
    expect(socket.connecting).toBe(true);
    driver.cancelConnectionAttempt(pending);
    await bounded(rejected);
    for (const release of releaseLookups.splice(0)) release();
    await delay(80);
    expect(socket.destroyed).toBe(true);
    expect(socket.readyState).toBe("closed");
    expect(entry.client.isOpen).toBe(false);
    expect(entry.client.isReady).toBe(false);
    expect(state(driver).clients.size).toBe(0);
    expect(server.sockets.size).toBe(0);
    await driver.connect();
    driver.cancelConnectionAttempt(pending);
    expect((await driver.query("GET shared")).rows[0]?.__col_0).toBe("db-0");
  });

  it("physically cancels a scoped TCP open after its connection timeout", async () => {
    const driver = makeDriver(80);
    await driver.connect();
    delayNext = true;
    const pending = driver.readTablePage(page("db1"));
    const rejected = expect(pending).rejects.toThrow("timed out");
    await vi.waitFor(() => expect(releaseLookups).toHaveLength(1));
    const entry = [...state(driver).clients][1];
    const socket = sockets[1];
    await bounded(rejected);
    for (const release of releaseLookups.splice(0)) release();
    await delay(80);
    expect(socket.destroyed).toBe(true);
    expect(socket.readyState).toBe("closed");
    expect(entry.client.isOpen).toBe(false);
    expect(entry.client.isReady).toBe(false);
    expect(state(driver).clients.size).toBe(1);
    expect(server.sockets.size).toBe(1);
    expect((await driver.readTablePage(page("db1"))).rows[0]?.value).toBe(
      "db-1",
    );
  });

  it("cancels a physical TLS socket waiting for secureConnect", async () => {
    // This TCP endpoint intentionally never completes the TLS handshake.
    const driver = makeDriver(2000, true);
    const pending = driver.connect();
    const rejected = expect(pending).rejects.toThrow("cancelled");
    await vi.waitFor(() => expect(server.sockets.size).toBe(1));
    const entry = [...state(driver).clients][0];
    expect(entry.client.isReady).toBe(false);
    expect(sockets[0].destroyed).toBe(false);
    driver.cancelConnectionAttempt(pending);
    await bounded(rejected);
    await vi.waitFor(() => {
      expect(sockets[0].destroyed).toBe(true);
      expect(sockets[0].readyState).toBe("closed");
      expect(entry.client.isOpen).toBe(false);
      expect(entry.client.isReady).toBe(false);
      expect(state(driver).clients.size).toBe(0);
      expect(server.sockets.size).toBe(0);
    });
  });

  it("timeout-wrapper connect cancellation preserves a replacement session", async () => {
    const driver = makeDriver();
    const wrapped = createTimeoutAwareDriver(driver, () => ({
      connectionTimeoutMs: 80,
      connectionTimeoutSeconds: 1,
      dbOperationTimeoutMs: 2000,
      dbOperationTimeoutSeconds: 2,
    }));
    delayNext = true;
    const rejected = expect(wrapped.connect()).rejects.toThrow("timed out");
    await vi.waitFor(() => expect(releaseLookups).toHaveLength(1));
    const entry = [...state(driver).clients][0];
    const oldSocket = sockets[0];
    await bounded(rejected);
    await vi.waitFor(() => expect(entry.abort.signal.aborted).toBe(true));
    await driver.connect();
    for (const release of releaseLookups.splice(0)) release();
    await delay(80);
    expect(oldSocket.destroyed).toBe(true);
    expect(oldSocket.readyState).toBe("closed");
    expect(entry.client.isOpen).toBe(false);
    expect(entry.client.isReady).toBe(false);
    expect(state(driver).clients.size).toBe(1);
    expect(server.sockets.size).toBe(1);
    expect((await driver.query("GET shared")).rows[0]?.__col_0).toBe("db-0");
  });

  it("cancels a sent base GET without destroying the base or native DB session", async () => {
    const driver = makeDriver();
    await driver.connect();
    await driver.readTablePage(page("db1"));
    const entry = [...state(driver).clients][0];
    const pending = driver.query("GET hang; SET shared wrong", [], {
      requestToken: 101,
    });
    const rejected = expect(pending).rejects.toThrow();
    await vi.waitFor(() =>
      expect(server.commands.some(({ args }) => args.includes("hang"))).toBe(
        true,
      ),
    );
    driver.cancelCurrentOperation({
      reason: "manual",
      operationName: "query",
      requestToken: 101,
    });
    await bounded(rejected);
    expect(state(driver).queries.size).toBe(0);
    expect(state(driver).clients.size).toBe(2);
    expect(entry.client.isOpen).toBe(true);
    expect(entry.client.isReady).toBe(true);
    expect(sockets[0].destroyed).toBe(false);
    expect((await driver.readTablePage(page("db1"))).rows[0]?.value).toBe(
      "db-1",
    );
    // A late response is still drained by node-redis without corrupting the
    // next command's reply, but cannot resume the cancelled query's SET.
    const stalled = server.commands.find(({ args }) => args.includes("hang"));
    stalled?.socket.write("$4\r\nlate\r\n");
    expect((await driver.query("GET shared")).rows[0]?.__col_0).toBe("db-0");
    expect(server.commands.some(({ args }) => args[0] === "SET")).toBe(false);
  });

  it.each([
    "timeout",
    "cancel",
  ])("%s destroys only a sent/no-reply disposable BLPOP session", async (mode) => {
    const driver = makeDriver();
    await driver.connect();
    await driver.readTablePage(page("db1"));
    const wrapped = createTimeoutAwareDriver(driver, () => ({
      connectionTimeoutMs: 2000,
      connectionTimeoutSeconds: 2,
      dbOperationTimeoutMs: 200,
      dbOperationTimeoutSeconds: 1,
    }));
    const pending = wrapped.query("BLPOP blocked 0; SET shared wrong", [], {
      database: "db1",
      requestToken: 71,
    });
    const rejected = expect(pending).rejects.toThrow();
    await vi.waitFor(() =>
      expect(server.commands.some(({ args }) => args[0] === "BLPOP")).toBe(
        true,
      ),
    );
    const entry = [...state(driver).clients][2];
    const socket = sockets[2];
    expect(entry.client.isReady).toBe(true);
    if (mode === "cancel")
      driver.cancelCurrentOperation({
        reason: "manual",
        operationName: "query",
        requestToken: 71,
      });
    await bounded(rejected);
    await vi.waitFor(() => {
      expect(state(driver).queries.size).toBe(0);
      expect(state(driver).clients.size).toBe(2);
      expect(server.sockets.size).toBe(2);
      expect(socket.readyState).toBe("closed");
    });
    expect(socket.destroyed).toBe(true);
    expect(entry.client.isOpen).toBe(false);
    expect(entry.client.isReady).toBe(false);
    expect((await driver.query("GET shared")).rows[0]?.__col_0).toBe("db-0");
    expect((await driver.readTablePage(page("db1"))).rows[0]?.value).toBe(
      "db-1",
    );
    expect(server.commands.some(({ args }) => args[0] === "SET")).toBe(false);
  });

  it("disconnect forces base, native and disposable sent/no-reply sockets closed", async () => {
    const driver = makeDriver();
    await driver.connect();
    await driver.readTablePage(page("db1"));
    const pending = [
      driver.query("GET hang", [], { requestToken: 1 }),
      driver.readTablePage(page("db1", "blocked")),
      driver.query("GET hang", [], { database: "db0", requestToken: 2 }),
    ];
    const settled = Promise.allSettled(pending);
    await vi.waitFor(() =>
      expect(
        server.commands.filter(
          ({ args }) => args.includes("hang") || args.includes("blocked:*"),
        ).length,
      ).toBe(3),
    );
    const entries = [...state(driver).clients];
    expect(entries).toHaveLength(3);
    await bounded(driver.disconnect());
    expect(
      (await bounded(settled)).every((result) => result.status === "rejected"),
    ).toBe(true);
    await vi.waitFor(() => {
      expect(server.sockets.size).toBe(0);
      expect(state(driver).clients.size).toBe(0);
      expect(state(driver).queries.size).toBe(0);
      expect(state(driver).databaseClients.size).toBe(0);
    });
    for (const entry of entries) {
      expect(entry.client.isOpen).toBe(false);
      expect(entry.client.isReady).toBe(false);
    }
    for (const socket of sockets) {
      expect(socket.destroyed).toBe(true);
      expect(socket.readyState).toBe("closed");
    }
  });

  it("disconnect cleans a physically open socket even after close marks its client logically closed", async () => {
    const driver = makeDriver();
    await driver.connect();
    await driver.readTablePage(page("db1"));
    const entry = [...state(driver).clients][1];
    const pending = entry.client.get("hang");
    const rejected = expect(pending).rejects.toThrow();
    await vi.waitFor(() =>
      expect(server.commands.some(({ args }) => args.includes("hang"))).toBe(
        true,
      ),
    );
    void entry.client.close().catch(() => undefined);
    expect(entry.client.isOpen).toBe(false);
    expect(entry.client.isReady).toBe(true);
    await bounded(driver.disconnect());
    await bounded(rejected);
    await vi.waitFor(() => {
      expect(server.sockets.size).toBe(0);
      expect(state(driver).clients.size).toBe(0);
      expect(entry.client.isReady).toBe(false);
      expect(sockets.every((socket) => socket.destroyed)).toBe(true);
    });
  });
});

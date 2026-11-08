import net from "node:net";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { RedisDriver } from "../../src/extension/dbDrivers/redis";
import { REDIS_ALL_KEYS_TABLE } from "../../src/extension/dbDrivers/redisKeyspace";
import type { DriverTablePageRequest } from "../../src/extension/dbDrivers/types";
import { DriverUpdateRowsError } from "../../src/extension/dbDrivers/updateRowOutcomes";
import { logger } from "../../src/extension/utils/logger";
import { redisBinarySnapshotDisplay } from "../../src/shared/redisValueSnapshot";
import { REDIS_READ_BUDGET } from "../../src/shared/safetyContracts";
import { withRedisReplyMapping } from "../support/redisReplyMapping";

function bulk(value: Buffer | string): Buffer {
  const bytes = Buffer.isBuffer(value) ? value : Buffer.from(value);
  return Buffer.concat([
    Buffer.from(`$${bytes.length}\r\n`),
    bytes,
    Buffer.from("\r\n"),
  ]);
}

function array(items: Buffer[]): Buffer {
  return Buffer.concat([Buffer.from(`*${items.length}\r\n`), ...items]);
}

const binaryMember = Buffer.from([0xff, 0x00]);

const values: Record<string, { type: string; reply: Buffer; size: number }> = {
  "h:proto": {
    type: "hash",
    size: 2,
    reply: Buffer.concat([
      Buffer.from("%2\r\n"),
      bulk("__proto__"),
      bulk("polluted"),
      bulk("name"),
      bulk("ok"),
    ]),
  },
  "l:bin": {
    type: "list",
    size: 2,
    reply: array([bulk("text"), bulk(binaryMember)]),
  },
  "s:events": {
    type: "stream",
    size: 1,
    reply: array([
      array([
        bulk("1-0"),
        array([bulk("__proto__"), bulk("v"), bulk("f"), bulk("w")]),
      ]),
    ]),
  },
  "z:inf": {
    type: "zset",
    size: 3,
    reply: array([
      array([bulk("low"), Buffer.from(",-inf\r\n")]),
      array([bulk("mid"), Buffer.from(",1.5\r\n")]),
      array([bulk("high"), Buffer.from(",inf\r\n")]),
    ]),
  },
};

class ReplyServer {
  readonly sockets = new Set<net.Socket>();
  readonly commands: string[][] = [];
  replies = new Map<string, string | Buffer>();
  readonly server = net.createServer((socket) => {
    this.sockets.add(socket);
    socket.on("close", () => this.sockets.delete(socket));
    socket.on("error", () => undefined);
    let input = Buffer.alloc(0);
    socket.on("data", (chunk) => {
      input = Buffer.concat([
        input,
        Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk),
      ]);
      while (input.length) {
        const parsed = this.parse(input);
        if (!parsed) break;
        input = input.subarray(parsed.consumed);
        this.commands.push(parsed.args);
        socket.write(this.reply(parsed.args));
      }
    });
  });

  private reply(args: string[]): string | Buffer {
    const command = args[0].toUpperCase();
    const entry = values[args[1]];
    const custom =
      this.replies.get(args.join(" ")) ?? this.replies.get(command);
    if (custom !== undefined) return custom;
    switch (command) {
      case "HELLO":
        return "%1\r\n+server\r\n+redis\r\n";
      case "SCAN":
        return array([
          bulk("0"),
          array(Object.keys(values).map((key) => bulk(key))),
        ]);
      case "TYPE":
        return `+${entry?.type ?? "none"}\r\n`;
      case "HLEN":
      case "LLEN":
      case "SCARD":
      case "ZCARD":
      case "XLEN":
        return `:${entry?.size ?? 0}\r\n`;
      case "MEMORY":
        return ":100\r\n";
      case "TTL":
        return ":-1\r\n";
      case "HGETALL":
      case "LRANGE":
      case "SMEMBERS":
      case "ZRANGE":
      case "XRANGE":
        return values[args[1]]?.reply ?? "*0\r\n";
      default:
        return "+OK\r\n";
    }
  }

  private parse(input: Buffer) {
    const firstEnd = input.indexOf("\r\n");
    if (firstEnd < 0) return undefined;
    const count = Number(input.subarray(1, firstEnd).toString());
    let offset = firstEnd + 2;
    const args: string[] = [];
    for (let index = 0; index < count; index++) {
      const headerEnd = input.indexOf("\r\n", offset);
      if (headerEnd < 0) return undefined;
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
    const { port } = this.server.address() as net.AddressInfo;
    return `redis://127.0.0.1:${port}/0`;
  }

  async close() {
    for (const socket of this.sockets) socket.destroy();
    await new Promise<void>((resolve) => this.server.close(() => resolve()));
  }
}

const pageRequest: DriverTablePageRequest = {
  database: "db0",
  schema: "",
  table: REDIS_ALL_KEYS_TABLE,
  page: 1,
  pageSize: 10,
  filters: [],
  sort: null,
  skipCount: false,
};

let server: ReplyServer;
let driver: RedisDriver;

beforeEach(async () => {
  vi.spyOn(logger, "error").mockReturnValue(new Error("logged"));
  server = new ReplyServer();
  driver = new RedisDriver({
    id: "reply-fidelity",
    name: "Reply fidelity",
    type: "redis",
    connectionUri: await server.listen(),
  });
  await driver.connect();
});

afterEach(async () => {
  await driver.disconnect();
  await server.close();
});

async function rowsByKey() {
  const page = await driver.readTablePage(pageRequest);
  return new Map(page.rows.map((row) => [String(row.key), row]));
}

describe("Redis query reply fidelity", () => {
  it("shows nested EXEC errors instead of empty objects", async () => {
    server.replies.set("MULTI", "+OK\r\n");
    server.replies.set("SET", "+QUEUED\r\n");
    server.replies.set("INCR", "+QUEUED\r\n");
    server.replies.set(
      "EXEC",
      "*2\r\n+OK\r\n-ERR value is not an integer or out of range\r\n",
    );
    const result = await driver.query("MULTI; SET a 1; INCR text; EXEC");
    expect(JSON.parse(String(result.rows[0].__col_0))).toEqual([
      "OK",
      "QUEUED",
      "QUEUED",
      ["OK", "(error) ERR value is not an integer or out of range"],
    ]);
  });

  it("keeps integer replies above 2^53 exact", async () => {
    server.replies.set("INCRBY", ":9007199254740993\r\n");
    const result = await driver.query("INCRBY big 1");
    expect(result.rows[0].__col_0).toBe("9007199254740993");
  });

  it("keeps safe integer replies numeric and integer-like strings textual", async () => {
    server.replies.set("DBSIZE", ":42\r\n");
    server.replies.set("TTL", ":-1\r\n");
    server.replies.set("GET", bulk("123"));
    server.replies.set("PING", "+123\r\n");
    server.replies.set("INCRBY", ":9007199254740993\r\n");
    const single = await driver.query("DBSIZE");
    expect(single.rows[0].__col_0).toBe(42);
    const multiple = await driver.query(
      "DBSIZE; TTL a; GET a; PING; INCRBY big 1",
    );
    expect(JSON.parse(String(multiple.rows[0].__col_0))).toEqual([
      42,
      -1,
      "123",
      "123",
      "9007199254740993",
    ]);
  });

  it("keeps a __proto__ hash field in query results", async () => {
    const result = await driver.query("HGETALL h:proto");
    expect(JSON.parse(String(result.rows[0].__col_0))).toEqual({
      ["__proto__"]: "polluted",
      name: "ok",
    });
  });

  it("returns completed statements with the error when a later statement fails", async () => {
    server.replies.set(
      "INCR",
      "-ERR value is not an integer or out of range\r\n",
    );
    const result = await driver.query("SET a 1; INCR notanumber; SET b 2");
    expect(JSON.parse(String(result.rows[0].__col_0))).toEqual([
      "OK",
      "(error) ERR value is not an integer or out of range",
    ]);
    expect(result.error).toContain("Redis statement 2 of 3 failed");
    expect(
      server.commands
        .filter((args) => args[0] === "SET")
        .map((args) => args[1]),
    ).toEqual(["a"]);
  });

  it("still throws when the first statement fails", async () => {
    server.replies.set("INCR", "-ERR value is not an integer\r\n");
    await expect(driver.query("INCR notanumber; SET b 2")).rejects.toThrow(
      "value is not an integer",
    );
  });
});

describe("Redis table value fidelity", () => {
  it("keeps __proto__ hash and stream fields", async () => {
    const rows = await rowsByKey();
    expect(JSON.parse(String(rows.get("h:proto")?.value))).toEqual({
      ["__proto__"]: "polluted",
      name: "ok",
    });
    expect(JSON.parse(String(rows.get("s:events")?.value))).toEqual([
      { id: "1-0", message: { ["__proto__"]: "v", f: "w" } },
    ]);
  });

  it("represents infinite sorted set scores and accepts them on edit", async () => {
    const rows = await rowsByKey();
    const original = rows.get("z:inf")?.value;
    expect(JSON.parse(String(original))).toEqual([
      { value: "low", score: "-inf" },
      { value: "mid", score: 1.5 },
      { value: "high", score: "+inf" },
    ]);
    server.replies.set("EVAL", ":1\r\n");
    await expect(
      driver.updateRows({
        database: "db0",
        schema: "",
        table: REDIS_ALL_KEYS_TABLE,
        updates: [
          {
            primaryKeys: { key: "z:inf" },
            changes: {
              value:
                '[{"value":"low","score":"-inf"},{"value":"high","score":"+inf"}]',
            },
            originalValues: { value: original },
          },
        ],
      }),
    ).resolves.toMatchObject({ affectedRows: 1 });
    const evalArgs = server.commands.find((args) => args[0] === "EVAL");
    expect(JSON.parse(evalArgs?.[7] ?? "")).toEqual([
      { score: "-inf", value: "low" },
      { score: 1.5, value: "mid" },
      { score: "+inf", value: "high" },
    ]);
    expect(JSON.parse(evalArgs?.[9] ?? "")).toEqual([
      { score: "-inf", value: "low" },
      { score: "+inf", value: "high" },
    ]);
  });

  it("marks binary collection members and keeps those values read-only", async () => {
    const rows = await rowsByKey();
    const value = rows.get("l:bin")?.value;
    expect(redisBinarySnapshotDisplay(value, "list")).toBe(
      '["text","RedisBinary(0xff00)"]',
    );
    expect(redisBinarySnapshotDisplay(rows.get("h:proto")?.value, "hash")).toBe(
      undefined,
    );
    await expect(
      driver.updateRows({
        database: "db0",
        schema: "",
        table: REDIS_ALL_KEYS_TABLE,
        updates: [
          {
            primaryKeys: { key: "l:bin" },
            changes: { value: '["text","changed"]' },
            originalValues: { value },
          },
        ],
      }),
    ).rejects.toThrow("Redis list values with binary members are read-only");
    expect(server.commands.some((args) => args[0] === "EVAL")).toBe(false);
  });
});

describe("Redis edit preflight rejections", () => {
  async function editFailure(reply: string) {
    server.replies.set("TYPE", "+string\r\n");
    server.replies.set("EVAL", reply);
    try {
      await driver.updateRows({
        database: "db0",
        schema: "",
        table: REDIS_ALL_KEYS_TABLE,
        updates: [{ primaryKeys: { key: "a" }, changes: { ttl: 60 } }],
      });
    } catch (error) {
      return error as DriverUpdateRowsError;
    }
    throw new Error("Expected the edit to fail");
  }

  it("reports script rejections that write nothing as not applied", async () => {
    const error = await editFailure(
      "-RAPIDB_NOT_APPLIED Redis edit staging key already exists.\r\n",
    );
    expect(error).toBeInstanceOf(DriverUpdateRowsError);
    expect(error.message).toBe("Redis edit staging key already exists.");
    expect(error.result.updateRowOutcomes?.[0]?.status).toBe("not_applied");
  });

  it("keeps other script failures as possibly applied", async () => {
    const error = await editFailure("-ERR unexpected failure\r\n");
    expect(error.result.updateRowOutcomes?.[0]?.status).toBe("unknown");
  });
});

describe("Redis SCAN budget", () => {
  it("uses a fixed SCAN COUNT so sparse matches in large keyspaces complete", async () => {
    const scanDriver = new RedisDriver({
      id: "scan-budget",
      name: "Scan budget",
      type: "redis",
      host: "localhost",
    });
    const slots = 3_000_000;
    const client = {
      scan: vi.fn(async (cursor: string, options: { COUNT: number }) => {
        const start = Number(cursor);
        const end = Math.min(slots, start + options.COUNT);
        return {
          cursor: end >= slots ? "0" : String(end),
          keys: Array.from({ length: end - start }, (_, index) => start + index)
            .filter((slot) => slot % 600_000 === 0)
            .map((slot) => `users:${slot}`),
        };
      }),
    };
    const mapped = withRedisReplyMapping(client);
    Object.assign(scanDriver, {
      client: mapped,
      connected: true,
      getDatabaseClient: async () => mapped,
    });
    const tables = await scanDriver.listObjects();
    expect(tables.map((table) => table.name)).toContain("users");
    expect(client.scan.mock.calls.length).toBeLessThan(
      REDIS_READ_BUDGET.maxScanIterations,
    );
    expect(
      new Set(client.scan.mock.calls.map(([, options]) => options.COUNT)),
    ).toEqual(new Set([1000]));
  });
});

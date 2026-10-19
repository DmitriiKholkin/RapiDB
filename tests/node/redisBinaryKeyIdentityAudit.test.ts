import net from "node:net";
import { createClient, RESP_TYPES } from "redis";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { RedisDriver } from "../../src/extension/dbDrivers/redis";
import type { DriverTablePageRequest } from "../../src/extension/dbDrivers/types";
import { loadDatabaseScope } from "../../src/extension/schema/schemaLoaders";
import { logger } from "../../src/extension/utils/logger";

const binaryKey = Buffer.concat([Buffer.from("audit:"), Buffer.from([0xff])]);
const validKey = Buffer.from("audit:�");
const binaryKeyError = /non-UTF-8 key/;
const pageRequest: DriverTablePageRequest = {
  database: "db0",
  schema: "",
  table: "audit",
  page: 1,
  pageSize: 10,
  filters: [],
  sort: null,
  skipCount: false,
};

function bulk(bytes: Buffer | string): Buffer {
  const value = Buffer.isBuffer(bytes) ? bytes : Buffer.from(bytes);
  return Buffer.concat([
    Buffer.from(`$${value.length}\r\n`),
    value,
    Buffer.from("\r\n"),
  ]);
}

// Real node-redis parser, RESP3 handshake, socket, reply mappings and wire
// arguments. The tiny server deliberately preserves key bytes (never strings).
class BinaryKeyServer {
  readonly sockets = new Set<net.Socket>();
  readonly commands: Buffer[][] = [];
  entries = [
    { key: binaryKey, value: Buffer.from("BINARY") },
    { key: validKey, value: Buffer.from("VALID") },
  ];
  scanPages?: Buffer[][];
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
        const args = parsed.args;
        this.commands.push(args);
        const entry = this.entries.find(({ key }) => key.equals(args[1]));
        switch (args[0].toString()) {
          case "HELLO":
            socket.write("%1\r\n+server\r\n+redis\r\n");
            break;
          case "SCAN": {
            const index = Number(args[1].toString());
            const keys =
              this.scanPages?.[index] ?? this.entries.map((e) => e.key);
            const cursor =
              this.scanPages && index + 1 < this.scanPages.length
                ? String(index + 1)
                : "0";
            socket.write(
              Buffer.concat([
                Buffer.from("*2\r\n"),
                bulk(cursor),
                Buffer.from(`*${keys.length}\r\n`),
                ...keys.map(bulk),
              ]),
            );
            break;
          }
          case "TYPE":
            socket.write(entry ? "+string\r\n" : "+none\r\n");
            break;
          case "GET":
            socket.write(entry ? bulk(entry.value) : "_\r\n");
            break;
          case "TTL":
            socket.write(":-1\r\n");
            break;
          case "EVAL":
            // TTL-only edit: the live regression below also executes real Lua.
            socket.write(":1\r\n");
            break;
          case "DEL":
            this.entries = this.entries.filter(
              ({ key }) => !key.equals(args[1]),
            );
            socket.write(entry ? ":1\r\n" : ":0\r\n");
            break;
          default:
            socket.write("+OK\r\n");
        }
      }
    });
  });

  private parse(input: Buffer) {
    const firstEnd = input.indexOf("\r\n");
    if (firstEnd < 0) return undefined;
    const count = Number(input.subarray(1, firstEnd).toString());
    let offset = firstEnd + 2;
    const args: Buffer[] = [];
    for (let index = 0; index < count; index++) {
      const headerEnd = input.indexOf("\r\n", offset);
      if (headerEnd < 0) return undefined;
      const size = Number(input.subarray(offset + 1, headerEnd).toString());
      offset = headerEnd + 2;
      if (input.length < offset + size + 2) return undefined;
      args.push(Buffer.from(input.subarray(offset, offset + size)));
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

let server: BinaryKeyServer;
let driver: RedisDriver;
let url: string;

beforeEach(async () => {
  vi.spyOn(logger, "error").mockReturnValue(new Error("logged"));
  server = new BinaryKeyServer();
  url = await server.listen();
  driver = new RedisDriver({
    id: "binary-key-audit",
    name: "Audit",
    type: "redis",
    connectionUri: url,
  });
  await driver.connect();
});

afterEach(async () => {
  await driver.disconnect();
  await server.close();
});

describe("Redis binary key identity: real node-redis transport", () => {
  it("independently demonstrates why ordinary SCAN decoding is unsafe", async () => {
    const client = createClient({ url });
    try {
      await client.connect();
      const raw = await client
        .withTypeMapping({ [RESP_TYPES.BLOB_STRING]: Buffer })
        .scan("0");
      expect(raw.keys.map((key) => key.toString("hex"))).toEqual([
        binaryKey.toString("hex"),
        validKey.toString("hex"),
      ]);
      const decoded = await client.scan("0");
      expect(decoded.keys).toEqual(["audit:�", "audit:�"]);
      expect(
        await Promise.all(decoded.keys.map((key) => client.get(key))),
      ).toEqual(["VALID", "VALID"]);
    } finally {
      client.destroy();
    }
  });

  it.each([
    "page",
    "value-sorted page",
    "describeTable",
    "describeColumns",
    "listObjects",
  ])("fails closed before reading values or exposing identities: %s", async (operation) => {
    const result =
      operation === "page"
        ? driver.readTablePage(pageRequest)
        : operation === "value-sorted page"
          ? driver.readTablePage({
              ...pageRequest,
              sort: { column: "value", direction: "asc" },
            })
          : operation === "describeTable"
            ? driver.describeTable("db0", "", "audit")
            : operation === "describeColumns"
              ? driver.describeColumns("db0", "", "audit")
              : driver.listObjects("db0");
    await expect(result).rejects.toThrow(binaryKeyError);
    expect(
      server.commands
        .map((args) => args[0].toString())
        .filter(
          (command) => !["HELLO", "CLIENT", "SELECT", "SCAN"].includes(command),
        ),
    ).toEqual([]);
  });

  it("validates later SCAN batches before any value read", async () => {
    server.scanPages = [[validKey], [binaryKey]];
    await expect(driver.readTablePage(pageRequest)).rejects.toThrow(
      binaryKeyError,
    );
    const scans = server.commands.filter(
      (args) => args[0].toString() === "SCAN",
    );
    expect(scans.map((args) => args[1].toString())).toEqual(["0", "1"]);
    expect(server.commands.some((args) => args[0].toString() === "TYPE")).toBe(
      false,
    );
  });

  it("reports binary discovery as a metadata error, not an empty loaded schema", async () => {
    const failed = await loadDatabaseScope(driver, "db0", "baseline");
    expect(failed.loadedSchemas).toEqual([]);
    expect(failed.failedSchemas).toHaveLength(1);
    expect(failed.failedSchemas?.[0].name).toBe("db0");
    expect(failed.failedSchemas?.[0].error).toBeInstanceOf(Error);
    expect((failed.failedSchemas?.[0].error as Error).message).toMatch(
      binaryKeyError,
    );
    expect(server.commands.some((args) => args[0].toString() === "TYPE")).toBe(
      false,
    );

    server.entries = [{ key: validKey, value: Buffer.from("VALID") }];
    const recovered = await loadDatabaseScope(driver, "db0", "baseline");
    expect(recovered.failedSchemas).toEqual([]);
    expect(recovered.loadedSchemas).toHaveLength(1);
    expect(recovered.loadedSchemas[0].objects).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ name: "audit", type: "table" }),
      ]),
    );
  });

  it("does not replace binary identities with a hexadecimal display key", async () => {
    server.entries = [{ key: binaryKey, value: Buffer.from("BINARY") }];
    await expect(driver.readTablePage(pageRequest)).rejects.toThrow(
      binaryKeyError,
    );
    expect(
      server.commands.some((args) =>
        ["GET", "EVAL", "DEL"].includes(args[0].toString()),
      ),
    ).toBe(false);
  });

  it("never string-coerces a binary identity in update or delete requests", async () => {
    const request = { database: "db0", schema: "", table: "audit" };
    await expect(
      driver.updateRows({
        ...request,
        updates: [
          { primaryKeys: { key: binaryKey }, changes: { value: "WRONG" } },
        ],
      }),
    ).resolves.toMatchObject({ affectedRows: 0 });
    await expect(
      driver.deleteRows({
        ...request,
        primaryKeyValuesList: [{ key: binaryKey }],
      }),
    ).rejects.toThrow("Redis delete requires a non-empty key");
    expect(
      server.commands.some((args) =>
        ["TYPE", "GET", "EVAL", "DEL"].includes(args[0].toString()),
      ),
    ).toBe(false);
    expect(server.entries.map((entry) => entry.value.toString())).toEqual([
      "BINARY",
      "VALID",
    ]);
  });

  it("preserves valid replacement characters, Unicode, NUL and exact mutation arguments", async () => {
    server.entries = [
      { key: validKey, value: Buffer.from("VALID") },
      { key: Buffer.from("audit:雪😀\0"), value: Buffer.from("UNICODE") },
    ];
    const page = await driver.readTablePage(pageRequest);
    expect(page.totalCount).toBe(2);
    expect(page.rows.find((row) => row.key === "audit:�")?.value).toBe("VALID");
    expect(page.rows.find((row) => row.key === "audit:雪😀\0")?.value).toBe(
      "UNICODE",
    );
    const key = page.rows.find((row) => row.key === "audit:�")?.key;
    await driver.updateRows({
      database: "db0",
      schema: "",
      table: "audit",
      updates: [{ primaryKeys: { key }, changes: { ttl: 60 } }],
    });
    await driver.deleteRows({
      database: "db0",
      schema: "",
      table: "audit",
      primaryKeyValuesList: [{ key }],
    });
    const edit = server.commands.find((args) => args[0].toString() === "EVAL");
    const deletion = server.commands.find(
      (args) => args[0].toString() === "DEL",
    );
    expect(edit?.[3]).toEqual(validKey);
    expect(edit?.[4]).toEqual(validKey);
    expect(deletion?.[1]).toEqual(validKey);
    expect(server.entries.map((entry) => entry.key.toString())).toEqual([
      "audit:雪😀\0",
    ]);
  });
});

describe("Redis binary key identity: SCAN validation unit cases", () => {
  it.each([
    "ff",
    "c0af",
    "e282",
    "eda080",
    "f4908080",
    "80",
  ])("rejects malformed UTF-8 %s before truncating a SCAN batch", async (hex) => {
    const scan = vi.fn().mockResolvedValue({
      cursor: Buffer.from("0"),
      keys: [
        validKey,
        Buffer.concat([Buffer.from("audit:"), Buffer.from(hex, "hex")]),
      ],
    });
    const withTypeMapping = vi.fn(() => ({ scan }));
    const scanKeys = driver as unknown as {
      scanKeys(
        client: unknown,
        pattern: string,
        limit: number,
      ): Promise<string[]>;
    };
    await expect(
      scanKeys.scanKeys({ withTypeMapping }, "audit:*", 1),
    ).rejects.toThrow(binaryKeyError);
    expect(withTypeMapping).toHaveBeenCalledWith({
      [RESP_TYPES.BLOB_STRING]: Buffer,
    });
    expect(scan).toHaveBeenCalledWith("0", { MATCH: "audit:*", COUNT: 1 });
  });
});

// Opt-in live verification. Only uniquely named keys are created/deleted; this
// never flushes a database or uses the project's shared Docker services.
it.skipIf(!process.env.RAPIDB_REDIS_BINARY_KEY_AUDIT_URL)(
  "rejects binary aliases on live Redis and keeps both values untouched",
  async () => {
    const liveUrl = process.env.RAPIDB_REDIS_BINARY_KEY_AUDIT_URL;
    const client = createClient({ url: liveUrl });
    const prefix = `rapidb-binary-audit-${process.pid}-${Date.now()}`;
    const binary = Buffer.concat([
      Buffer.from(`${prefix}:`),
      Buffer.from([0xff]),
    ]);
    const valid = `${prefix}:�`;
    const liveDriver = new RedisDriver({
      id: prefix,
      name: "Audit",
      type: "redis",
      connectionUri: liveUrl,
    });
    try {
      await client.connect();
      await liveDriver.connect();
      await client.set(binary, "BINARY");
      await client.set(valid, "VALID");
      await expect(
        liveDriver.readTablePage({ ...pageRequest, table: prefix }),
      ).rejects.toThrow(binaryKeyError);
      expect(await client.get(binary)).toBe("BINARY");
      expect(await client.get(valid)).toBe("VALID");
      // A valid U+FFFD key remains editable when addressed explicitly.
      await liveDriver.updateRows({
        database: "db0",
        schema: "",
        table: prefix,
        updates: [
          { primaryKeys: { key: valid }, changes: { value: "CHANGED" } },
        ],
      });
      expect(await client.get(binary)).toBe("BINARY");
      expect(await client.get(valid)).toBe("CHANGED");
      await liveDriver.deleteRows({
        database: "db0",
        schema: "",
        table: prefix,
        primaryKeyValuesList: [{ key: valid }],
      });
      expect(await client.get(binary)).toBe("BINARY");
      expect(await client.get(valid)).toBeNull();
    } finally {
      await liveDriver.disconnect();
      if (client.isReady) await client.del([binary, valid]);
      client.destroy();
    }
  },
);

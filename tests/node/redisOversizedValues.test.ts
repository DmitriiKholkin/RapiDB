import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { ErrorReply } from "redis";
import { describe, expect, it, vi } from "vitest";
import {
  REDIS_PAGED_COLLECTION_BYTES,
  REDIS_PAGED_COUNT,
  REDIS_PAGED_PREFLIGHT_BYTES,
  RedisDriver,
} from "../../src/extension/dbDrivers/redis";
import {
  REDIS_DELETE_EVIDENCE_BYTES,
  REDIS_OBSERVE_DELETE,
} from "../../src/extension/dbDrivers/redisDeleteObservation";
import type { DriverTableExportChunk } from "../../src/extension/dbDrivers/types";
import { TableReadService } from "../../src/extension/table/tableReadService";
import {
  formatTableCsvExportValue,
  normalizeJsonExportValue,
} from "../../src/extension/utils/exportValueFormat";
import {
  isRedisOversizedValueSnapshot,
  redisBinarySnapshotDisplay,
} from "../../src/shared/redisValueSnapshot";
import { REDIS_READ_BUDGET } from "../../src/shared/safetyContracts";
import {
  redisCompactReply,
  withRedisReplyMapping,
} from "../support/redisReplyMapping";

const { maxValueEntries, maxValueBytes, maxTotalValueBytes } =
  REDIS_READ_BUDGET;

type Entry = {
  type: string;
  value: unknown;
  size?: number;
  encoding?: string;
  allocation?: number;
};

function sizeOf(entry: Entry | undefined): number {
  if (!entry) return 0;
  if (entry.size !== undefined) return entry.size;
  if (typeof entry.value === "string") return Buffer.byteLength(entry.value);
  if (Array.isArray(entry.value)) return entry.value.length;
  return Object.keys(entry.value as object).length;
}

function compactResponse(entry: Entry | undefined, type: string) {
  if (!entry || entry.type !== type) return null;
  if (entry.encoding && entry.encoding !== "listpack") return null;
  if (sizeOf({ ...entry, size: undefined }) > maxValueEntries) return null;
  const fields =
    type === "hash"
      ? Object.entries(entry.value as Record<string, string>).flat()
      : type === "zset"
        ? (entry.value as { value: string; score: number }[]).flatMap(
            ({ value, score }) => [value, String(score)],
          )
        : (entry.value as string[]);
  const bytes =
    entry.allocation ??
    fields.reduce((sum, field) => sum + Buffer.byteLength(field), 0);
  return bytes <= REDIS_DELETE_EVIDENCE_BYTES
    ? redisCompactReply(type, fields)
    : null;
}

function members(entry: Entry): Buffer[] {
  const raw =
    entry.type === "hash"
      ? Object.entries(entry.value as Record<string, string>).flat()
      : entry.type === "zset"
        ? (entry.value as { value: string; score: number }[]).flatMap(
            ({ value, score }) => [value, String(score)],
          )
        : (entry.value as Array<string | Buffer>);
  return raw.map((item) => (Buffer.isBuffer(item) ? item : Buffer.from(item)));
}

// Emulates the commands of the paged collection fallback (Redis < 7,
// non-compact encodings): byte preflight, encoding/config checks, and
// cursor/window pages that hold at most COUNT entries per reply.
function pagedCommand(
  entries: Record<string, Entry>,
  args: string[],
  config: { listCompressDepth?: string } = {},
): unknown {
  const [command, ...rest] = args;
  const known = [
    "OBJECT",
    "CONFIG",
    "MEMORY",
    "LRANGE",
    "HSCAN",
    "SSCAN",
    "ZSCAN",
  ];
  if (!known.includes(command)) return 1;
  const entry = entries[rest[0]];
  if (command === "OBJECT") {
    const target = entries[rest[1]];
    return target ? Buffer.from(target.encoding ?? "listpack") : null;
  }
  if (command === "CONFIG") {
    return [
      Buffer.from("list-compress-depth"),
      Buffer.from(config.listCompressDepth ?? "0"),
    ];
  }
  if (command === "MEMORY") {
    const target = entries[rest[1]];
    if (!target) return null;
    return (
      target.allocation ??
      members(target).reduce((sum, item) => sum + item.length + 16, 0)
    );
  }
  if (!entry) return command === "LRANGE" ? [] : [Buffer.from("0"), []];
  if (command === "LRANGE") {
    if (entry.type !== "list") throw new ErrorReply("WRONGTYPE");
    return members(entry).slice(Number(rest[1]), Number(rest[2]) + 1);
  }
  const step = command === "SSCAN" ? 1 : 2;
  const wanted = { HSCAN: "hash", SSCAN: "set", ZSCAN: "zset" }[command];
  if (!wanted) return 1;
  if (entry.type !== wanted) throw new ErrorReply("WRONGTYPE");
  const all = members(entry);
  const start = Number(rest[1]) * step;
  const count = Number(rest[3]) * step;
  const next =
    start + count >= all.length
      ? "0"
      : String(Number(rest[1]) + Number(rest[3]));
  return [Buffer.from(next), all.slice(start, start + count)];
}

function harness(
  entries: Record<string, Entry>,
  extra: Record<string, unknown> = {},
) {
  const driver = new RedisDriver({
    id: "redis-oversized",
    name: "Redis Oversized",
    type: "redis",
    host: "localhost",
  });
  const size = vi.fn(async (key: string) => sizeOf(entries[key]));
  const slice = (key: string, start: number, end: number) =>
    (entries[key]?.value as unknown[]).slice(start, end + 1);
  const get = vi.fn(async (key: string) => entries[key]?.value ?? null);
  const client = {
    scan: vi.fn(async () => ({ cursor: "0", keys: Object.keys(entries) })),
    type: vi.fn(async (key: string) => entries[key]?.type ?? "none"),
    strLen: size,
    hLen: size,
    lLen: size,
    sCard: size,
    zCard: size,
    xLen: size,
    get,
    getRange: vi.fn(async (key: string, start: number, end: number) => {
      const value = await get(key);
      return (
        Buffer.isBuffer(value) ? value : Buffer.from(String(value ?? ""))
      ).subarray(start, end + 1);
    }),
    hGetAll: vi.fn(async (key: string) => entries[key]?.value),
    lRange: vi.fn(slice),
    sMembers: vi.fn(async (key: string) => entries[key]?.value),
    zRangeWithScores: vi.fn(slice),
    xRange: vi.fn(
      async (
        key: string,
        _start: string,
        _end: string,
        options: { COUNT: number },
      ) => (entries[key]?.value as unknown[]).slice(0, options.COUNT),
    ),
    ttl: vi.fn(async () => -1),
    sendCommand: vi.fn(
      async ([
        command,
        script,
        count,
        key,
        type,
      ]: string[]): Promise<unknown> => {
        if (command !== "EVAL_RO") {
          return pagedCommand(entries, [command, script, count, key, type]);
        }
        expect(script).toBe(REDIS_OBSERVE_DELETE);
        expect(count).toBe("1");
        return compactResponse(entries[key], type);
      },
    ),
    ...extra,
  };
  const mapped = withRedisReplyMapping(client);
  Object.assign(driver, {
    connected: true,
    client: mapped,
    getDatabaseClient: async () => mapped,
  });
  return { driver, client };
}

function readPage(
  driver: RedisDriver,
  overrides: Partial<Parameters<RedisDriver["readTablePage"]>[0]> = {},
) {
  return driver.readTablePage({
    database: "db0",
    schema: "db0",
    table: "big",
    page: 1,
    pageSize: 200,
    filters: [],
    sort: { column: "key", direction: "asc" },
    skipCount: false,
    ...overrides,
  });
}

function rowsByKey(rows: Record<string, unknown>[]) {
  return Object.fromEntries(rows.map((row) => [row.key, row.value]));
}

function oversized(displayValue: string) {
  return { __rapidbRedisOversized: 1, displayValue };
}

const tooMany = maxValueEntries + 1;

describe("RedisDriver — oversized values", () => {
  it("does not read oversized values of any type during describe or page reads", async () => {
    const { driver, client } = harness({
      "big:string": { type: "string", value: "x", size: maxValueBytes + 1 },
      "big:hash": { type: "hash", value: {}, size: tooMany },
      "big:list": { type: "list", value: [], size: 10_000_000 },
      "big:set": { type: "set", value: [], size: tooMany },
      "big:zset": { type: "zset", value: [], size: tooMany },
      "big:stream": { type: "stream", value: [], size: tooMany },
      "big:small": { type: "hash", value: { field: "value" } },
    });

    const columns = await driver.describeColumns("db0", "db0", "big");
    const page = await readPage(driver);

    expect(client.get).not.toHaveBeenCalled();
    expect(client.lRange).not.toHaveBeenCalled();
    expect(client.sMembers).not.toHaveBeenCalled();
    expect(client.zRangeWithScores).not.toHaveBeenCalled();
    expect(client.xRange).not.toHaveBeenCalled();
    expect(client.hGetAll).not.toHaveBeenCalled();
    expect(client.sendCommand.mock.calls.map(([args]) => args[3])).toEqual([
      "big:small",
      "big:small",
    ]);
    expect(columns.find((column) => column.name === "value")?.nativeType).toBe(
      "mixed(string, hash, list, set, zset, stream)",
    );
    expect(rowsByKey(page.rows)).toEqual({
      "big:string": oversized(
        `RedisValueTooLarge(string, ${maxValueBytes + 1} bytes)`,
      ),
      "big:hash": oversized(`RedisValueTooLarge(hash, ${tooMany} entries)`),
      "big:list": oversized("RedisValueTooLarge(list, 10000000 entries)"),
      "big:set": oversized(`RedisValueTooLarge(set, ${tooMany} entries)`),
      "big:zset": oversized(`RedisValueTooLarge(zset, ${tooMany} entries)`),
      "big:stream": oversized(`RedisValueTooLarge(stream, ${tooMany} entries)`),
      "big:small": '{"field":"value"}',
    });
    expect(page.rows.find((row) => row.key === "big:list")?.type).toBe("list");
  });

  it("loads values exactly at the limits and replaces values one past them", async () => {
    const atEntries = Array.from({ length: maxValueEntries }, (_, i) => `${i}`);
    const pastEntries = [...atEntries, "extra"];
    const { driver } = harness({
      "edge:list-at": { type: "list", value: atEntries },
      "edge:list-past": { type: "list", value: pastEntries },
      "edge:string-at": { type: "string", value: "a".repeat(maxValueBytes) },
      "edge:string-past": {
        type: "string",
        value: "a".repeat(maxValueBytes + 1),
      },
    });

    const values = rowsByKey((await readPage(driver)).rows);

    expect(values["edge:list-at"]).toBe(JSON.stringify(atEntries));
    expect(values["edge:list-past"]).toEqual(
      oversized(`RedisValueTooLarge(list, ${tooMany} entries)`),
    );
    expect(values["edge:string-at"]).toHaveLength(maxValueBytes);
    expect(values["edge:string-past"]).toEqual(
      oversized(`RedisValueTooLarge(string, ${maxValueBytes + 1} bytes)`),
    );
  });

  it("bounds value reads so a value growing after its size check is still replaced", async () => {
    const grown = Array.from({ length: 2 * maxValueEntries }, (_, i) => `${i}`);
    const { driver, client } = harness({
      "race:list": { type: "list", value: grown, size: 10 },
      "race:zset": {
        type: "zset",
        value: grown.map((value, score) => ({ value, score })),
        size: 10,
      },
      "race:stream": {
        type: "stream",
        value: grown.map((id) => ({ id, message: {} })),
        size: 10,
      },
      "race:string": {
        type: "string",
        value: "a".repeat(20),
        size: 10,
      },
    });

    const values = rowsByKey((await readPage(driver)).rows);

    expect(client.lRange).not.toHaveBeenCalled();
    expect(client.zRangeWithScores).not.toHaveBeenCalled();
    expect(client.xRange).not.toHaveBeenCalled();
    expect(client.getRange).toHaveBeenCalledExactlyOnceWith(
      "race:string",
      0,
      10,
    );
    // The paged fallback stops once the entry cap is crossed and shows a
    // marker instead of the prefix it has read.
    expect(values).toEqual({
      "race:list": oversized("RedisValueTooLarge(list, 1024 entries)"),
      "race:zset": oversized("RedisValueTooLarge(zset, 1024 entries)"),
      "race:stream": oversized(
        "RedisValueUnavailable(stream, no byte-bounded read)",
      ),
      "race:string": oversized(
        "RedisValueUnavailable(string, value changed during read)",
      ),
    });
  });

  it("does not decode the sentinel byte at the per-string cap", async () => {
    const { driver, client } = harness(
      {
        "race:string": { type: "string", value: "x", size: maxValueBytes },
      },
      {
        getRange: vi.fn(async (_key: string, _start: number, end: number) =>
          Buffer.alloc(end + 1, 0xff),
        ),
      },
    );
    expect(rowsByKey((await readPage(driver)).rows)).toEqual({
      "race:string": oversized(
        `RedisValueTooLarge(string, ${maxValueBytes + 1} bytes)`,
      ),
    });
    expect(client.getRange).toHaveBeenCalledExactlyOnceWith(
      "race:string",
      0,
      maxValueBytes,
    );
    expect(client.get).not.toHaveBeenCalled();
  });

  it("keeps strings, including empty and binary strings, when observation is unsupported", async () => {
    const { driver, client } = harness(
      {
        "legacy:binary": {
          type: "string",
          value: Buffer.from([0, 0xff]),
          size: 2,
        },
        "legacy:empty": { type: "string", value: "" },
        "legacy:text": { type: "string", value: "雪\t" },
      },
      {
        sendCommand: vi
          .fn()
          .mockRejectedValue(new ErrorReply("ERR unknown command 'EVAL_RO'")),
      },
    );
    const page = await readPage(driver, { observeNoSqlDelete: true });
    expect(rowsByKey(page.rows)).toEqual({
      "legacy:binary": {
        __rapidbRedisBinary: 1,
        displayValue: "RedisBinary(0x00ff)",
        binaryHex: "00ff",
      },
      "legacy:empty": "",
      "legacy:text": "雪\t",
    });
    expect(page.noSqlDeleteObservations).toEqual([null, null, null]);
    expect(
      client.getRange.mock.calls
        .map(([key, start, end]) => [key, start, end])
        .sort(),
    ).toEqual([
      ["legacy:binary", 0, 2],
      ["legacy:empty", 0, 0],
      ["legacy:text", 0, 4],
    ]);
  });

  it("does not replace a denied GETRANGE with GET", async () => {
    const { driver, client } = harness(
      {
        key: { type: "string", value: "text" },
      },
      {
        getRange: vi.fn().mockRejectedValue(new ErrorReply("NOPERM GETRANGE")),
      },
    );
    expect(rowsByKey((await readPage(driver)).rows)).toEqual({
      key: oversized(
        "RedisValueUnavailable(string, bounded read denied or unsupported)",
      ),
    });
    expect(client.get).not.toHaveBeenCalled();
  });

  it.each([
    "NOPERM MEMORY USAGE is not allowed",
    "NOPERM OBJECT ENCODING is not allowed",
    "ERR unknown command 'EVAL_RO'",
  ])("keeps collection metadata and uses no unbounded fallback when bounded reads are unavailable (%s)", async (message) => {
    const { driver, client } = harness(
      { "mem:hash": { type: "hash", value: { field: "value" } } },
      {
        sendCommand: vi.fn().mockRejectedValue(new ErrorReply(message)),
        ttl: vi.fn().mockResolvedValue(60),
      },
    );
    const page = await readPage(driver, { observeNoSqlDelete: true });
    expect(page.rows).toEqual([
      {
        key: "mem:hash",
        type: "hash",
        ttl: 60,
        value: oversized(
          "RedisValueUnavailable(hash, bounded read denied or unsupported)",
        ),
      },
    ]);
    expect(page.noSqlDeleteObservations).toEqual([null]);
    // The paged fallback needs a byte preflight before any member transfer.
    expect(client.sendCommand.mock.calls.map(([args]) => args)).toEqual([
      ["EVAL_RO", REDIS_OBSERVE_DELETE, "1", "mem:hash", "hash", "1", "1"],
      ["MEMORY", "USAGE", "mem:hash", "SAMPLES", "0"],
    ]);
    expect(client.hGetAll).not.toHaveBeenCalled();
    expect(client.getRange).not.toHaveBeenCalled();
  });

  it("reads non-compact collections only through the preflighted paged path", async () => {
    const memoryUsage = vi.fn().mockResolvedValue(1);
    const { driver, client } = harness(
      {
        "mem:table": {
          type: "hash",
          value: { field: "value" },
          encoding: "hashtable",
        },
        "mem:list": { type: "list", value: ["item"], encoding: "quicklist" },
        "mem:larger": {
          type: "set",
          value: ["item"],
          allocation: REDIS_DELETE_EVIDENCE_BYTES + 1,
        },
        "mem:edge": {
          type: "set",
          value: ["item"],
          allocation: REDIS_DELETE_EVIDENCE_BYTES,
        },
        "mem:module": { type: "ReJSON-RL", value: {} },
      },
      { memoryUsage },
    );
    const page = await readPage(driver, { observeNoSqlDelete: true });
    expect(rowsByKey(page.rows)).toEqual({
      "mem:table": '{"field":"value"}',
      "mem:list": '["item"]',
      "mem:larger": '["item"]',
      "mem:edge": '["item"]',
      "mem:module": oversized(
        "RedisValueUnavailable(ReJSON-RL, no byte-bounded read)",
      ),
    });
    // Only the atomic compact read yields delete evidence; the paged values
    // and the unread module value do not.
    const observed = Object.fromEntries(
      page.rows.map((row, index) => [
        row.key,
        page.noSqlDeleteObservations?.[index] !== null,
      ]),
    );
    expect(observed).toEqual({
      "mem:table": false,
      "mem:list": false,
      "mem:larger": false,
      "mem:edge": false, // the fixture's compact reply has no DUMP/expiry
      "mem:module": false,
    });
    expect(memoryUsage).not.toHaveBeenCalled();
    expect(client.hGetAll).not.toHaveBeenCalled();
    expect(client.lRange).not.toHaveBeenCalled();
    expect(client.sMembers).not.toHaveBeenCalled();
    const commands = client.sendCommand.mock.calls.map(([args]) => args[0]);
    expect(commands.filter((command) => command === "MEMORY")).toHaveLength(3);
  });

  it("reserves each bounded collection read and limits concurrency across simultaneous pages", async () => {
    let active = 0;
    let peak = 0;
    const entries = Object.fromEntries(
      Array.from({ length: 40 }, (_, i) => [
        `wait:${String(i).padStart(2, "0")}`,
        { type: "list", value: [`${i}`] },
      ]),
    );
    const { driver } = harness(entries, {
      sendCommand: vi.fn(async ([, , , key, type]: string[]) => {
        active += 1;
        peak = Math.max(peak, active);
        await new Promise((resolve) => setTimeout(resolve, 1));
        active -= 1;
        return compactResponse(entries[key], type);
      }),
    });

    const pages = await Promise.all([readPage(driver), readPage(driver)]);
    const values = pages.flatMap(({ rows }) => rows.map(({ value }) => value));

    expect(values.every((value) => typeof value === "string")).toBe(true);
    expect(peak).toBe(4);
  });

  it("stops loading values once the total read budget is spent", async () => {
    const count = maxTotalValueBytes / maxValueBytes + 1;
    const value = "a".repeat(maxValueBytes);
    const entries = Object.fromEntries(
      Array.from({ length: count }, (_, i) => [
        `budget:${String(i).padStart(3, "0")}`,
        { type: "string", value },
      ]),
    );
    const { driver } = harness(entries);

    const values = Object.values(rowsByKey((await readPage(driver)).rows));
    const loaded = values.filter((entry) => entry === value).length;

    expect(loaded).toBeGreaterThanOrEqual(
      Math.floor(
        (maxTotalValueBytes - (4 * maxValueBytes + 30)) / maxValueBytes,
      ),
    );
    expect(loaded * maxValueBytes).toBeLessThanOrEqual(maxTotalValueBytes);
    expect(values.filter((entry) => entry !== value)).toEqual(
      Array.from({ length: count - loaded }, () =>
        oversized(`RedisValueNotLoaded(string, ${maxValueBytes} bytes)`),
      ),
    );
  }, 30000);

  it("keeps the bytes actually loaded from collections within the total budget", async () => {
    const list = ['"'.repeat(60_000)];
    const count =
      Math.ceil(maxTotalValueBytes / Buffer.byteLength(JSON.stringify(list))) +
      12;
    const entries = Object.fromEntries(
      Array.from({ length: count }, (_, i) => [
        `bytes:${String(i).padStart(3, "0")}`,
        { type: "list", value: list },
      ]),
    );
    const { driver } = harness(entries);

    const values = Object.values(
      rowsByKey((await readPage(driver, { pageSize: count })).rows),
    );
    const loaded = values.filter((value) => typeof value === "string");

    expect(loaded.length).toBeGreaterThan(0);
    expect(loaded.length).toBeLessThan(count);
    expect(
      loaded.reduce(
        (total, value) => total + Buffer.byteLength(value as string),
        0,
      ),
    ).toBeLessThanOrEqual(maxTotalValueBytes);
    expect(
      values
        .filter((value) => typeof value !== "string")
        .every((value) =>
          (value as { displayValue: string }).displayValue.startsWith(
            "RedisValueNotLoaded(list,",
          ),
        ),
    ).toBe(true);
  });

  it("reads values only for the requested page when no filter or sort uses value", async () => {
    const value = "v".repeat(40_000);
    const entries = Object.fromEntries(
      Array.from({ length: REDIS_READ_BUDGET.maxValueReads }, (_, i) => [
        `page:${String(i).padStart(4, "0")}`,
        { type: "string", value },
      ]),
    );
    const { driver, client } = harness(entries);

    const page = await readPage(driver, {
      page: 40,
      pageSize: 50,
      filters: [{ column: "type", operator: "eq", value: "string" }],
      sort: { column: "ttl", direction: "asc" },
    });

    expect(page.totalCount).toBe(REDIS_READ_BUDGET.maxValueReads);
    expect(page.rows).toHaveLength(50);
    expect(page.rows.every((row) => row.value === value)).toBe(true);
    expect(client.get).toHaveBeenCalledTimes(50);
  });

  it("infers the value column category without oversized placeholders", async () => {
    const small = {
      "cat:a": { type: "list", value: ["1"] },
      "cat:b": { type: "list", value: ["2"] },
      "cat:c": { type: "list", value: ["3"] },
    };
    const valueColumn = async (entries: Record<string, Entry>) =>
      (await harness(entries).driver.describeColumns("db0", "db0", "cat")).find(
        (column) => column.name === "value",
      );

    const baseline = await valueColumn(small);
    const withOversized = await valueColumn({
      ...small,
      "cat:big": { type: "list", value: [], size: 5000 },
    });

    expect(baseline?.category).not.toBe("other");
    expect(withOversized).toEqual(baseline);
  });

  it("filters oversized rows by type without reading their values", async () => {
    const { driver, client } = harness({
      "big:list": { type: "list", value: [], size: tooMany },
      "big:small": { type: "hash", value: { field: "value" } },
    });

    const page = await readPage(driver, {
      filters: [{ column: "type", operator: "eq", value: "list" }],
      sort: null,
    });

    expect(page.rows.map((row) => row.key)).toEqual(["big:list"]);
    expect(client.lRange).not.toHaveBeenCalled();
  });

  it.each([
    {
      filters: [{ column: "value", operator: "like" as const, value: "a" }],
      sort: null,
    },
    { filters: [], sort: { column: "value", direction: "asc" as const } },
  ])("fails closed when a value filter or sort meets an oversized row (%#)", async (request) => {
    const { driver } = harness({
      "big:list": { type: "list", value: [], size: tooMany },
      "big:small": { type: "string", value: "a" },
    });

    await expect(readPage(driver, request)).rejects.toThrow(
      /Redis key 'big:list' has a value beyond the table viewer read limit.*filtering or sorting by value is unavailable/,
    );
  });

  it("keeps metadata without reading a value when a size command is rejected", async () => {
    const { driver, client } = harness(
      { "big:list": { type: "list", value: ["a"] } },
      {
        lLen: vi.fn(async () => {
          throw new ErrorReply("NOPERM this user has no permissions");
        }),
      },
    );

    expect((await readPage(driver)).rows).toEqual([
      {
        key: "big:list",
        ttl: null,
        type: "list",
        value: oversized(
          "RedisValueUnavailable(list, bounded read denied or unsupported)",
        ),
      },
    ]);
    expect(client.sendCommand).not.toHaveBeenCalled();
    expect(client.lRange).not.toHaveBeenCalled();
  });

  it("rejects value edits on oversized keys before reading or writing them", async () => {
    const { driver, client } = harness({
      "big:list": { type: "list", value: [], size: tooMany },
      "big:string": { type: "string", value: "x", size: maxValueBytes + 1 },
      "big:small": { type: "hash", value: { field: "value" } },
    });
    const update = (
      key: string,
      changes: Record<string, unknown>,
      originalValues?: Record<string, unknown>,
    ) =>
      driver.updateRows({
        database: "db0",
        schema: "db0",
        table: "big",
        updates: [
          {
            primaryKeys: { key },
            changes,
            ...(originalValues ? { originalValues } : {}),
          },
        ],
      });

    await expect(update("big:list", { value: '["a"]' })).rejects.toThrow(
      /read-only/,
    );
    await expect(update("big:string", { value: "short" })).rejects.toThrow(
      /read-only/,
    );
    await expect(
      update(
        "big:small",
        { value: '{"field":"next"}' },
        { value: oversized("RedisValueTooLarge(hash, 5000 entries)") },
      ),
    ).rejects.toThrow(/read-only/);

    expect(client.get).not.toHaveBeenCalled();
    expect(client.lRange).not.toHaveBeenCalled();
    expect(client.sendCommand).not.toHaveBeenCalled();
  });

  it("asks to reload before editing a value skipped by the total read budget", async () => {
    const { driver, client } = harness({
      "budget:list": { type: "list", value: ["a"] },
    });

    await expect(
      driver.updateRows({
        database: "db0",
        schema: "db0",
        table: "big",
        updates: [
          {
            primaryKeys: { key: "budget:list" },
            changes: { value: '["b"]' },
            originalValues: {
              value: oversized("RedisValueNotLoaded(list, 1 entries)"),
            },
          },
        ],
      }),
    ).rejects.toThrow(
      /not loaded because the page reached its total read limit\. Reload with a smaller page size/,
    );
    expect(client.sendCommand).not.toHaveBeenCalled();
  });

  it("keeps TTL and rename edits available without comparing the omitted value", async () => {
    const { driver, client } = harness({
      "big:list": { type: "list", value: [], size: 10_000_000 },
    });

    for (const changes of [{ ttl: "60" }, { key: "big:renamed" }]) {
      await expect(
        driver.updateRows({
          database: "db0",
          schema: "db0",
          table: "big",
          updates: [
            {
              primaryKeys: { key: "big:list" },
              changes,
              originalValues: {
                value: oversized("RedisValueTooLarge(list, 10000000 entries)"),
              },
            },
          ],
        }),
      ).resolves.toEqual({ affectedRows: 1 });
      const args = client.sendCommand.mock.calls.at(-1)?.[0] as unknown[];
      expect(args[7]).toBe("");
      expect(args.at(-1)).toBe("0");
    }
    expect(client.lLen).not.toHaveBeenCalled();
    expect(client.lRange).not.toHaveBeenCalled();
  });

  it("fails exports that meet an oversized value instead of writing its placeholder", () => {
    const value = oversized("RedisValueTooLarge(list, 5000 entries)");

    expect(() => formatTableCsvExportValue(value, "other", "list")).toThrow(
      /RedisValueTooLarge\(list, 5000 entries\) exceeds the table viewer read limit and cannot be exported/,
    );
    expect(() => normalizeJsonExportValue(value, "other", "string")).toThrow(
      /cannot be exported/,
    );
    expect(() =>
      normalizeJsonExportValue(
        oversized("RedisValueNotLoaded(set, 3 entries)"),
        "other",
        "mixed(hash, set)",
      ),
    ).toThrow(/cannot be exported/);
    const unavailable = oversized(
      "RedisValueUnavailable(ReJSON-RL, no byte-bounded read)",
    );
    expect(
      redisBinarySnapshotDisplay(unavailable, "mixed(string, ReJSON-RL)"),
    ).toBe(unavailable.displayValue);
    expect(() =>
      normalizeJsonExportValue(unavailable, "other", "ReJSON-RL"),
    ).toThrow(/was not safely loaded and cannot be exported/);
  });

  it.each([
    "export-all",
    "export-filtered-sorted",
    "export-page",
  ] as const)("exports large values fully in byte-bounded chunks (%s)", async (mode) => {
    const value = "s".repeat(200_000);
    const entries = Object.fromEntries(
      Array.from({ length: 500 }, (_, i) => [
        `wide:${String(i).padStart(3, "0")}`,
        { type: "string", value },
      ]),
    );
    const { driver, client } = harness(entries);
    const service = new TableReadService({
      getConnection: () => ({ id: "c1" }),
      getDriver: () => driver,
    } as never);
    const source =
      mode === "export-page"
        ? service.getPageChunks("c1", "db0", "db0", "wide", 1, 500, [], null)
        : mode === "export-all"
          ? service.exportAll("c1", "db0", "db0", "wide", 500)
          : service.exportAll(
              "c1",
              "db0",
              "db0",
              "wide",
              500,
              { column: "ttl", direction: "asc" },
              [{ column: "type", operator: "like", value: "string" }],
            );

    const keys: unknown[] = [];
    let chunkCount = 0;
    for await (const chunk of source as AsyncIterable<DriverTableExportChunk>) {
      chunkCount += 1;
      const valueColumn = chunk.columns.find(
        (column) => column.name === "value",
      );
      let chunkBytes = 0;
      for (const row of chunk.rows) {
        expect(
          normalizeJsonExportValue(
            row.value,
            valueColumn?.category ?? null,
            valueColumn?.nativeType,
          ),
        ).toBe(value);
        chunkBytes += Buffer.byteLength(row.value as string);
        keys.push(row.key);
      }
      expect(chunkBytes).toBeLessThanOrEqual(maxTotalValueBytes);
    }

    expect(chunkCount).toBeGreaterThan(1);
    expect(keys).toEqual(Object.keys(entries));
    const describeReads = mode === "export-filtered-sorted" ? 200 : 0;
    const metadataReads = mode === "export-filtered-sorted" ? 2 * 500 : 0;
    expect(client.type).toHaveBeenCalledTimes(
      500 + describeReads + metadataReads,
    );
    expect(client.get).toHaveBeenCalledTimes(500 + describeReads);
  }, 30000);

  it("keeps one value column type across the chunks of a mixed page and exports CSV", async () => {
    const text = "s".repeat(200_000);
    const hash = { field: "h".repeat(45_000) };
    const entries: Record<string, Entry> = Object.fromEntries([
      ...Array.from({ length: 400 }, (_, i) => [
        `a:${String(i).padStart(3, "0")}`,
        { type: "string", value: text },
      ]),
      ...Array.from({ length: 100 }, (_, i) => [
        `b:${String(i).padStart(3, "0")}`,
        { type: "hash", value: hash },
      ]),
    ]);
    const { driver } = harness(entries);
    const service = new TableReadService({
      getConnection: () => ({ id: "c1" }),
      getDriver: () => driver,
    } as never);
    const chunks: DriverTableExportChunk[] = [];
    for await (const chunk of service.getPageChunks(
      "c1",
      "db0",
      "db0",
      "mixed",
      1,
      500,
      [],
      null,
    )) {
      chunks.push(chunk);
    }

    expect(chunks.length).toBeGreaterThan(1);
    expect(
      new Set(
        chunks.map((chunk) =>
          JSON.stringify(chunk.columns.find((c) => c.name === "value")),
        ),
      ).size,
    ).toBe(1);
    expect(
      chunks[0].columns.find((column) => column.name === "value")?.nativeType,
    ).toBe("mixed(string, hash)");

    const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "rapidb-redis-"));
    const outputPath = path.join(tempDir, "export.csv");
    const window = {
      showSaveDialog: vi.fn(async () => ({ fsPath: outputPath })),
      withProgress: vi.fn(async (_options, task) =>
        task(
          {},
          { onCancellationRequested: vi.fn(() => ({ dispose: vi.fn() })) },
        ),
      ),
      showInformationMessage: vi.fn(),
      showErrorMessage: vi.fn(),
    };
    vi.resetModules();
    vi.doMock("vscode", () => ({
      Uri: { file: (fsPath: string) => ({ fsPath }) },
      ProgressLocation: { Notification: 15 },
      workspace: { getConfiguration: () => ({ get: () => false }) },
      window,
    }));
    try {
      const { exportTableDataAsCsv } = await import(
        "../../src/extension/utils/exportService"
      );
      await exportTableDataAsCsv({
        fileName: "mixed",
        loadChunks: () => service.exportAll("c1", "db0", "db0", "mixed", 500),
      });
      expect(window.showErrorMessage).not.toHaveBeenCalled();
      const lines = fs
        .readFileSync(outputPath, "utf8")
        .trimEnd()
        .split(/\r?\n/);
      expect(lines).toHaveLength(501);
    } finally {
      vi.doUnmock("vscode");
      fs.rmSync(tempDir, { recursive: true, force: true });
    }
  }, 30000);

  it("makes progress when a binary string heads a page over the total budget", async () => {
    const chunk = 3 * 1024 * 1024;
    const text = "t".repeat(chunk);
    const entries: Record<string, Entry> = Object.fromEntries(
      Array.from({ length: 60 }, (_, i) => [
        `bin:${String(i).padStart(2, "0")}`,
        i === 0
          ? { type: "string", value: Buffer.alloc(chunk, 0xff), size: chunk }
          : { type: "string", value: text },
      ]),
    );
    const { driver, client } = harness(entries);
    const service = new TableReadService({
      getConnection: () => ({ id: "c1" }),
      getDriver: () => driver,
    } as never);

    const rows: Record<string, unknown>[] = [];
    for await (const exported of service.exportAll(
      "c1",
      "db0",
      "db0",
      "bin",
      500,
    )) {
      const valueColumn = exported.columns.find((c) => c.name === "value");
      let chunkBytes = 0;
      for (const row of exported.rows) {
        normalizeJsonExportValue(
          row.value,
          valueColumn?.category ?? null,
          valueColumn?.nativeType,
        );
        chunkBytes +=
          typeof row.value === "string"
            ? Buffer.byteLength(row.value)
            : 4 * chunk + 30;
        rows.push(row);
      }
      expect(chunkBytes).toBeLessThanOrEqual(maxTotalValueBytes);
    }

    expect(rows.map((row) => row.key)).toEqual(Object.keys(entries));
    expect(rows[0].value).toMatchObject({ __rapidbRedisBinary: 1 });
    expect(rows.slice(1).every((row) => row.value === text)).toBe(true);
    expect(client.type).toHaveBeenCalledTimes(60);
    expect(client.get.mock.calls.length).toBeLessThanOrEqual(2 * 60);
  }, 30000);

  it("does not trust cached hash metadata when exporting a later chunk", async () => {
    const text = "t".repeat(512 * 1024);
    const entries: Record<string, Entry> = Object.fromEntries([
      ...Array.from({ length: 200 }, (_, i) => [
        `grow:a${String(i).padStart(3, "0")}`,
        { type: "string", value: text },
      ]),
      ["grow:z", { type: "hash", value: { field: "value" } }],
    ]);
    const { driver, client } = harness(entries);
    const service = new TableReadService({
      getConnection: () => ({ id: "c1" }),
      getDriver: () => driver,
    } as never);

    const rows: Record<string, unknown>[] = [];
    let chunks = 0;
    for await (const chunk of service.getPageChunks(
      "c1",
      "db0",
      "db0",
      "grow",
      1,
      500,
      [],
      null,
    )) {
      chunks += 1;
      rows.push(...chunk.rows);
      entries["grow:z"].allocation = REDIS_PAGED_PREFLIGHT_BYTES + 1;
    }

    expect(chunks).toBeGreaterThan(1);
    expect(client.sendCommand.mock.calls.map(([args]) => args)).toEqual([
      ["EVAL_RO", REDIS_OBSERVE_DELETE, "1", "grow:z", "hash", "0", "1"],
      ["MEMORY", "USAGE", "grow:z", "SAMPLES", "0"],
    ]);
    expect(client.hGetAll).not.toHaveBeenCalled();
    expect(rows.at(-1)?.value).toEqual(
      oversized(
        `RedisValueTooLarge(hash, ${REDIS_PAGED_PREFLIGHT_BYTES + 1} bytes)`,
      ),
    );
  }, 30000);

  it("still fails a chunked export on a value over the per-value cap", async () => {
    const { driver } = harness({
      "mix:a": { type: "string", value: "a" },
      "mix:big": { type: "list", value: [], size: tooMany },
      "mix:c": { type: "string", value: "c" },
    });
    const service = new TableReadService({
      getConnection: () => ({ id: "c1" }),
      getDriver: () => driver,
    } as never);

    const formatAll = async () => {
      for await (const chunk of service.exportAll(
        "c1",
        "db0",
        "db0",
        "mix",
        500,
      )) {
        const valueColumn = chunk.columns.find(
          (column) => column.name === "value",
        );
        for (const row of chunk.rows) {
          formatTableCsvExportValue(
            row.value,
            valueColumn?.category ?? null,
            valueColumn?.nativeType,
          );
        }
      }
    };

    await expect(formatAll()).rejects.toThrow(
      /RedisValueTooLarge\(list, 1001 entries\) exceeds the table viewer read limit and cannot be exported/,
    );
  });

  it("exports placeholder-shaped values from non-Redis columns normally", () => {
    const value = oversized("RedisValueTooLarge(list, 5000 entries)");

    expect(normalizeJsonExportValue(value, "json", "jsonb")).toEqual(value);
    expect(normalizeJsonExportValue(value, "json", "object")).toEqual(value);
    expect(formatTableCsvExportValue(value, "json", "jsonb")).toBe(
      JSON.stringify(value),
    );
  });

  it("recognizes well-formed oversized snapshots under any Redis value label", () => {
    const snapshot = oversized("RedisValueTooLarge(zset, 2000 entries)");
    expect(isRedisOversizedValueSnapshot(snapshot)).toBe(true);
    expect(
      isRedisOversizedValueSnapshot(
        oversized("RedisValueNotLoaded(string, 12 bytes)"),
      ),
    ).toBe(true);
    for (const label of ["zset", "string", "list", "mixed(hash, set)"]) {
      expect(redisBinarySnapshotDisplay(snapshot, label)).toBe(
        "RedisValueTooLarge(zset, 2000 entries)",
      );
    }
    expect(redisBinarySnapshotDisplay(snapshot, "jsonb")).toBeUndefined();
    expect(redisBinarySnapshotDisplay(snapshot, undefined)).toBeUndefined();
    expect(
      isRedisOversizedValueSnapshot(
        oversized("RedisValueTooLarge(zset, many entries)"),
      ),
    ).toBe(false);
    expect(
      isRedisOversizedValueSnapshot(
        oversized("RedisValueTooLarge(json, 3 entries)"),
      ),
    ).toBe(false);
  });

  describe("paged collection fallback (Redis < 7 or non-compact encodings)", () => {
    const legacy = (
      entries: Record<string, Entry>,
      config: { listCompressDepth?: string; deny?: string } = {},
    ) => {
      const sendCommand = vi.fn(async (args: string[]) => {
        if (args[0] === "EVAL_RO") {
          throw new ErrorReply("ERR unknown command 'EVAL_RO'");
        }
        if (config.deny && args[0] === config.deny) {
          throw new ErrorReply(`NOPERM ${config.deny}`);
        }
        return pagedCommand(entries, args, config);
      });
      return harness(entries, { sendCommand });
    };

    it("reads every collection type page by page without delete evidence", async () => {
      const many = Array.from({ length: 150 }, (_, i) => `m${i}`);
      const { driver, client } = legacy({
        "c:hash": {
          type: "hash",
          value: Object.fromEntries(many.map((m) => [m, `v-${m}`])),
          encoding: "hashtable",
        },
        "c:list": { type: "list", value: many, encoding: "quicklist" },
        "c:set": { type: "set", value: many, encoding: "hashtable" },
        "c:zset": {
          type: "zset",
          value: [
            { value: "b", score: 2 },
            { value: "c", score: 1 },
            { value: "a", score: 2 },
          ],
          encoding: "skiplist",
        },
        "c:string": { type: "string", value: "plain" },
      });
      const page = await readPage(driver, { observeNoSqlDelete: true });
      const values = rowsByKey(page.rows);
      expect(JSON.parse(values["c:hash"] as string)).toEqual(
        Object.fromEntries(many.map((m) => [m, `v-${m}`])),
      );
      expect(values["c:list"]).toBe(JSON.stringify(many));
      expect(JSON.parse(values["c:set"] as string)).toEqual(many);
      // Rank order (score, then member bytes), not ZSCAN order.
      expect(values["c:zset"]).toBe(
        JSON.stringify([
          { value: "c", score: 1 },
          { value: "a", score: 2 },
          { value: "b", score: 2 },
        ]),
      );
      expect(values["c:string"]).toBe("plain");
      // Paged (non-atomic) values never authorize a grid delete.
      expect(page.noSqlDeleteObservations).toEqual([
        null,
        null,
        null,
        null,
        null,
      ]);
      for (const [args] of client.sendCommand.mock.calls) {
        if (args[0] === "LRANGE") {
          expect(Number(args[3]) - Number(args[2]) + 1).toBe(REDIS_PAGED_COUNT);
        }
        if (/SCAN$/.test(args[0])) {
          expect(args.slice(3)).toEqual(["COUNT", String(REDIS_PAGED_COUNT)]);
        }
      }
      // Each paged read is preceded by its byte preflight.
      const sequence = client.sendCommand.mock.calls
        .map(([args]) => args)
        .filter((args) => args[0] !== "EVAL_RO");
      for (const key of ["c:hash", "c:list", "c:set", "c:zset"]) {
        const first = sequence.findIndex(
          (args) => args.includes(key) && args[0] !== "OBJECT",
        );
        expect(sequence[first]).toEqual([
          "MEMORY",
          "USAGE",
          key,
          "SAMPLES",
          "0",
        ]);
      }
      expect(client.hGetAll).not.toHaveBeenCalled();
      expect(client.lRange).not.toHaveBeenCalled();
      expect(client.sMembers).not.toHaveBeenCalled();
      expect(client.zRangeWithScores).not.toHaveBeenCalled();
    });

    it("transfers no members when the byte preflight is too large or unavailable", async () => {
      const entries: Record<string, Entry> = {
        "p:big": {
          type: "hash",
          value: { f: "v" },
          encoding: "hashtable",
          allocation: REDIS_PAGED_PREFLIGHT_BYTES + 1,
        },
        "p:edge": {
          type: "set",
          value: ["x"],
          encoding: "hashtable",
          allocation: REDIS_PAGED_PREFLIGHT_BYTES,
        },
      };
      const { driver, client } = legacy(entries);
      expect(rowsByKey((await readPage(driver)).rows)).toEqual({
        "p:big": oversized(
          `RedisValueTooLarge(hash, ${REDIS_PAGED_PREFLIGHT_BYTES + 1} bytes)`,
        ),
        "p:edge": '["x"]',
      });
      const scanned = client.sendCommand.mock.calls
        .map(([args]) => args)
        .filter((args) => /SCAN$/.test(args[0]))
        .map((args) => args[1]);
      expect(scanned).toEqual(["p:edge"]);

      const denied = legacy(entries, { deny: "MEMORY" });
      expect(rowsByKey((await readPage(denied.driver)).rows)).toEqual({
        "p:big": oversized(
          "RedisValueUnavailable(hash, bounded read denied or unsupported)",
        ),
        "p:edge": oversized(
          "RedisValueUnavailable(set, bounded read denied or unsupported)",
        ),
      });
      expect(
        denied.client.sendCommand.mock.calls.some(([args]) =>
          /SCAN$/.test(args[0]),
        ),
      ).toBe(false);
    });

    it.each([
      [
        "compression enabled",
        { listCompressDepth: "1" },
        "no byte-bounded read",
      ],
      [
        "compression setting unreadable",
        { deny: "CONFIG" },
        "bounded read denied or unsupported",
      ],
    ])("does not trust MEMORY USAGE for a quicklist with %s", async (_label, config, reason) => {
      const { driver, client } = legacy(
        { "q:list": { type: "list", value: ["a"], encoding: "quicklist" } },
        config,
      );
      expect(rowsByKey((await readPage(driver)).rows)).toEqual({
        "q:list": oversized(`RedisValueUnavailable(list, ${reason})`),
      });
      expect(
        client.sendCommand.mock.calls.some(([args]) => args[0] === "LRANGE"),
      ).toBe(false);
    });

    it("stops at the member byte limit with a marker instead of a partial value", async () => {
      // MEMORY USAGE was small, but the members grew before they were paged.
      const chunk = "z".repeat(16 * 1024);
      const fields = Object.fromEntries(
        Array.from({ length: 40 }, (_, i) => [`f${i}`, chunk]),
      );
      const { driver, client } = legacy({
        "g:hash": {
          type: "hash",
          value: fields,
          encoding: "hashtable",
          allocation: 1000,
        },
        "g:list": {
          type: "list",
          value: Object.values(fields),
          encoding: "linkedlist",
          allocation: 1000,
        },
      });
      const values = rowsByKey((await readPage(driver)).rows);
      for (const key of ["g:hash", "g:list"]) {
        const marker = (values[key] as { displayValue: string }).displayValue;
        const bytes = Number(/, (\d+) bytes\)$/.exec(marker)?.[1]);
        expect(marker).toMatch(
          /^RedisValueTooLarge\((hash|list), \d+ bytes\)$/,
        );
        expect(bytes).toBeGreaterThan(REDIS_PAGED_COLLECTION_BYTES);
        // Stopped at the first reply that crossed the limit.
        expect(bytes).toBeLessThanOrEqual(
          REDIS_PAGED_COLLECTION_BYTES + REDIS_PAGED_COUNT * (chunk.length + 8),
        );
      }
      expect(
        isRedisOversizedValueSnapshot(values["g:hash"]) &&
          isRedisOversizedValueSnapshot(values["g:list"]),
      ).toBe(true);
      expect(client.hGetAll).not.toHaveBeenCalled();
    });

    it("reports a value that changed type between the type probe and its pages", async () => {
      const swapped = legacy({
        "t:hash": { type: "string", value: "now a string", allocation: 64 },
      });
      // The driver still believes the key is a hash.
      const driver = swapped.driver as unknown as {
        client: unknown;
        readPagedRedisCollection: (
          client: unknown,
          key: string,
          type: string,
        ) => Promise<{ value: unknown }>;
      };
      const result = await driver.readPagedRedisCollection(
        driver.client,
        "t:hash",
        "hash",
      );
      expect(result.value).toEqual(
        oversized("RedisValueUnavailable(hash, value changed during read)"),
      );
    });
  });
});

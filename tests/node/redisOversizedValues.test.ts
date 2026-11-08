import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { ErrorReply } from "redis";
import { describe, expect, it, vi } from "vitest";
import { RedisDriver } from "../../src/extension/dbDrivers/redis";
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
import { withRedisReplyMapping } from "../support/redisReplyMapping";

const { maxValueEntries, maxValueBytes, maxTotalValueBytes } =
  REDIS_READ_BUDGET;

type Entry = { type: string; value: unknown; size?: number };

function sizeOf(entry: Entry | undefined): number {
  if (!entry) return 0;
  if (entry.size !== undefined) return entry.size;
  if (typeof entry.value === "string") return Buffer.byteLength(entry.value);
  if (Array.isArray(entry.value)) return entry.value.length;
  return Object.keys(entry.value as object).length;
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
  const client = {
    scan: vi.fn(async () => ({ cursor: "0", keys: Object.keys(entries) })),
    type: vi.fn(async (key: string) => entries[key]?.type ?? "none"),
    strLen: size,
    hLen: size,
    lLen: size,
    sCard: size,
    zCard: size,
    xLen: size,
    get: vi.fn(async (key: string) => entries[key]?.value ?? null),
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
    sendCommand: vi.fn().mockResolvedValue(1),
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
    expect(
      client.hGetAll.mock.calls.every(([key]) => key === "big:small"),
    ).toBe(true);
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
        value: "a".repeat(maxValueBytes + 10),
        size: 10,
      },
    });

    const values = rowsByKey((await readPage(driver)).rows);

    expect(client.lRange).toHaveBeenCalledWith("race:list", 0, maxValueEntries);
    expect(client.zRangeWithScores).toHaveBeenCalledWith(
      "race:zset",
      0,
      maxValueEntries,
    );
    expect(client.xRange).toHaveBeenCalledWith("race:stream", "-", "+", {
      COUNT: tooMany,
    });
    expect(values).toEqual({
      "race:list": oversized(`RedisValueTooLarge(list, ${tooMany} entries)`),
      "race:zset": oversized(`RedisValueTooLarge(zset, ${tooMany} entries)`),
      "race:stream": oversized(
        `RedisValueTooLarge(stream, ${tooMany} entries)`,
      ),
      "race:string": oversized(
        `RedisValueTooLarge(string, ${maxValueBytes + 1} bytes)`,
      ),
    });
  });

  it("uses MEMORY USAGE to bound collection bytes and falls back when it is denied", async () => {
    const entries = {
      "mem:big": { type: "hash", value: { field: "value" } },
      "mem:denied": { type: "set", value: ["member"] },
    };
    const memoryUsage = vi.fn(async (key: string) => {
      if (key === "mem:denied") throw new ErrorReply("NOPERM memory");
      return maxValueBytes + 1;
    });
    const { driver, client } = harness(entries, { memoryUsage });

    const values = rowsByKey((await readPage(driver)).rows);

    expect(memoryUsage).toHaveBeenCalledWith("mem:big", {
      SAMPLES: maxValueEntries,
    });
    expect(client.hGetAll).not.toHaveBeenCalled();
    expect(values).toEqual({
      "mem:big": oversized(
        `RedisValueTooLarge(hash, ${maxValueBytes + 1} bytes)`,
      ),
      "mem:denied": '["member"]',
    });
  });

  it("replaces a collection over the byte cap when MEMORY USAGE is unavailable", async () => {
    const list = Array.from({ length: maxValueEntries }, () =>
      "e".repeat(5000),
    );
    const { driver } = harness({
      "wide:list": { type: "list", value: list },
      "wide:small": { type: "list", value: ["a"] },
    });

    const values = rowsByKey((await readPage(driver)).rows);

    expect(values["wide:list"]).toEqual(
      oversized(
        `RedisValueTooLarge(list, ${Buffer.byteLength(JSON.stringify(list))} bytes)`,
      ),
    );
    expect(values["wide:small"]).toBe('["a"]');
  });

  it("replaces a collection whose MEMORY USAGE estimate was too low", async () => {
    const hash: Record<string, string> = Object.fromEntries(
      Array.from({ length: maxValueEntries - 1 }, (_, i) => [`f${i}`, "v"]),
    );
    hash.huge = "h".repeat(maxValueBytes + 1);
    const { driver } = harness(
      { "low:hash": { type: "hash", value: hash } },
      { memoryUsage: vi.fn(async () => 60_000) },
    );

    const values = rowsByKey((await readPage(driver)).rows);

    expect(values["low:hash"]).toEqual(
      oversized(
        `RedisValueTooLarge(hash, ${Buffer.byteLength(JSON.stringify(hash))} bytes)`,
      ),
    );
  });

  it("reserves the per-value cap for unestimated collections without dropping small values", async () => {
    let active = 0;
    let peak = 0;
    const entries = Object.fromEntries(
      Array.from({ length: 40 }, (_, i) => [
        `wait:${String(i).padStart(2, "0")}`,
        { type: "list", value: [`${i}`] },
      ]),
    );
    const { driver } = harness(entries, {
      lRange: vi.fn(async (key: string) => {
        active += 1;
        peak = Math.max(peak, active);
        await new Promise((resolve) => setTimeout(resolve, 1));
        active -= 1;
        return entries[key].value;
      }),
    });

    const values = Object.values(rowsByKey((await readPage(driver)).rows));

    expect(values.every((value) => typeof value === "string")).toBe(true);
    expect(peak).toBeLessThanOrEqual(maxTotalValueBytes / maxValueBytes);
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
  });

  it("keeps the bytes actually loaded from collections within the total budget", async () => {
    const list = Array.from({ length: maxValueEntries }, () =>
      "e".repeat(1021),
    );
    const count = maxTotalValueBytes / (1024 * 1024) + 40;
    const entries = Object.fromEntries(
      Array.from({ length: count }, (_, i) => [
        `bytes:${String(i).padStart(3, "0")}`,
        { type: "list", value: list },
      ]),
    );
    const { driver } = harness(entries);

    const values = Object.values(rowsByKey((await readPage(driver)).rows));
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

  it("fails the read when a size command is rejected", async () => {
    const { driver, client } = harness(
      { "big:list": { type: "list", value: ["a"] } },
      {
        lLen: vi.fn(async () => {
          throw new ErrorReply("NOPERM this user has no permissions");
        }),
      },
    );

    await expect(readPage(driver)).rejects.toThrow(/NOPERM/);
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
  });

  it("keeps one value column type across the chunks of a mixed page and exports CSV", async () => {
    const text = "s".repeat(200_000);
    const hash = { field: "h".repeat(200_000) };
    const entries: Record<string, Entry> = Object.fromEntries([
      ...Array.from({ length: 300 }, (_, i) => [
        `a:${String(i).padStart(3, "0")}`,
        { type: "string", value: text },
      ]),
      ...Array.from({ length: 200 }, (_, i) => [
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
  });

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
  });

  it("rechecks a cached hash size before reading it in a later chunk pass", async () => {
    const text = "t".repeat(512 * 1024);
    const entries: Record<string, Entry> = Object.fromEntries([
      ...Array.from({ length: 200 }, (_, i) => [
        `grow:a${String(i).padStart(3, "0")}`,
        { type: "string", value: text },
      ]),
      ["grow:z", { type: "hash", value: { field: "value" } }],
    ]);
    const hLen = vi.fn().mockResolvedValueOnce(1).mockResolvedValue(5000);
    const { driver, client } = harness(entries, { hLen });
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
    }

    expect(chunks).toBeGreaterThan(1);
    expect(hLen.mock.calls.length).toBeGreaterThan(1);
    expect(client.hGetAll).not.toHaveBeenCalled();
    expect(rows.at(-1)?.value).toEqual(
      oversized("RedisValueTooLarge(hash, 5000 entries)"),
    );
  });

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
});

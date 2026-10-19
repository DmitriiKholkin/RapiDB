import { Decimal128, Long } from "mongodb";
import { describe, expect, it, vi } from "vitest";
import type { ConnectionManager } from "../../src/extension/connectionManager";
import { ElasticsearchDriver } from "../../src/extension/dbDrivers/elasticsearch";
import { MongoDBDriver } from "../../src/extension/dbDrivers/mongodb";
import type { ColumnTypeMeta } from "../../src/extension/dbDrivers/types";
import {
  applyChangesTransactional,
  prepareApplyChangesPlan,
} from "../../src/extension/table/tableMutationExecution";
import { TableMutationService } from "../../src/extension/table/tableMutationService";
import { parseJsonForWrite } from "../../src/extension/utils/jsonWriteSafety";
import { NUMERIC_COMPARISON_LIMITS } from "../../src/shared/numericNormalization";
import { elasticsearchKeys } from "../support/elasticsearchIdentity";

const unsafe = [
  "9007199254740993",
  "-9007199254740993",
  "9.007199254740993e15",
  "1000000000000000100",
  "0.10000000000000001",
  "1e400",
  "1e-400",
  `1${"0".repeat(5000)}`,
];
const column = {
  name: "_source",
  nativeType: "object",
  category: "json",
} as ColumnTypeMeta;
const idColumn: ColumnTypeMeta = {
  ...column,
  name: "_id",
  category: "text",
  nativeType: "string",
  isPrimaryKey: true,
};
const drivers = [
  new ElasticsearchDriver({ id: "es", name: "es", type: "elasticsearch" }),
  new MongoDBDriver({ id: "mongo", name: "mongo", type: "mongodb" }),
];

describe("JSON numeric write safety", () => {
  it.each(
    unsafe,
  )("rejects nested inaccurate token %s before coercion", (token) => {
    const text = `{"nested":[{"amount":${token}}]}`;
    expect(() => parseJsonForWrite(text)).toThrow(/numeric precision/);
    for (const driver of drivers) {
      expect(() => driver.coerceInputValue(text, column)).toThrow(
        /numeric precision/,
      );
      expect(driver.checkPersistedEdit(column, text)).toMatchObject({
        ok: false,
        shouldVerify: false,
        message: expect.stringContaining("numeric precision"),
      });
    }
  });

  it.each([
    "42",
    "9007199254740992",
    "13000.0",
    "1e3",
    "0.1",
    "-0",
    '"9007199254740993"',
    "null",
    "true",
  ])("allows unchanged numeric value or nonnumber %s", (token) => {
    const text = `{"amount":${token}}`;
    expect(parseJsonForWrite(text)).toEqual(JSON.parse(text));
    for (const driver of drivers) {
      expect(driver.coerceInputValue(text, column)).toEqual(JSON.parse(text));
      expect(driver.checkPersistedEdit(column, text)).toBeNull();
    }
  });

  it("does not treat string contents/keys/marker-like objects as numbers", () => {
    const text =
      '{"__proto__":{"raw":"9007199254740993"},"9007199254740993":"escaped \\" 1e400"}';
    expect(parseJsonForWrite(text)).toEqual(JSON.parse(text));
  });

  it("validates strict JSON syntax and every duplicate-key token", () => {
    expect(() => parseJsonForWrite('{"x":01}')).toThrow(SyntaxError);
    expect(() => parseJsonForWrite('{"x":9007199254740993,"x":1}')).toThrow(
      /numeric precision/,
    );
  });

  it("fails closed when bounded decimal comparison cannot compare a finite token", () => {
    expect(() =>
      parseJsonForWrite(
        `0.${"0".repeat(NUMERIC_COMPARISON_LIMITS.maxTokenLength)}`,
      ),
    ).toThrow(/numeric precision/);
  });

  it("leaves typed exact BSON scalars untouched", () => {
    for (const value of [
      Long.fromString("9007199254740993"),
      Decimal128.fromString("9007199254740993"),
    ]) {
      expect(drivers[1].coerceInputValue(value, column)).toBe(value);
    }
  });

  it.each([
    "json",
    "array",
  ] as const)("allows repairing existing Mongo %s doubles while retaining the optimistic predicate", async (category) => {
    const mongo = new MongoDBDriver({
      id: "mongo",
      name: "mongo",
      type: "mongodb",
    });
    const updateOne = vi
      .fn()
      .mockResolvedValue({ acknowledged: true, matchedCount: 1 });
    Object.assign(mongo, {
      connected: true,
      client: { db: () => ({ collection: () => ({ updateOne }) }) },
    });
    const meta: ColumnTypeMeta = { ...column, name: "payload", category };
    const oldText =
      category === "json"
        ? '{"amount":1000000000000000100}'
        : "[1000000000000000100]";
    const newText = category === "json" ? '{"amount":42}' : "[42]";
    const manager = {
      getConnection: () => ({ readOnly: false }),
      getDriver: () => mongo,
    } as unknown as ConnectionManager;
    const result = await applyChangesTransactional(
      manager,
      "mongo",
      "db",
      "",
      "items",
      [
        {
          primaryKeys: {
            _id: { $rapidbMongoId: { type: "string", value: "actual" } },
          },
          changes: { payload: newText },
          originalValues: { payload: oldText },
        },
      ],
      [meta, idColumn],
    );
    expect(result.success).toBe(true);
    expect(updateOne.mock.calls[0][0]).toEqual({
      _id: { $eq: "actual" },
      payload: JSON.parse(oldText),
    });
    expect(updateOne.mock.calls[0][1]).toEqual({
      $set: { payload: JSON.parse(newText) },
    });
    expect(() => mongo.coerceInputValue(oldText, meta)).toThrow(
      /numeric precision/,
    );
  });

  it("allows safe Elasticsearch repairs with existing imprecise display originals", async () => {
    const es = new ElasticsearchDriver({
      id: "es",
      name: "es",
      type: "elasticsearch",
    });
    const columns = await es.describeColumns("db", "", "items");
    const manager = {
      getConnection: () => ({ readOnly: false }),
      getDriver: () => es,
    } as unknown as ConnectionManager;
    expect(
      prepareApplyChangesPlan(
        manager,
        "es",
        "db",
        "",
        "items",
        [
          {
            primaryKeys: elasticsearchKeys(es, "actual", "items"),
            changes: { _source: '{"amount":42}' },
            originalValues: { _source: '{"amount":9007199254740993}' },
          },
        ],
        columns,
      ).executable,
    ).toBe(true);
  });

  it("rejects inaccurate REST writes and entire REST batches before SDK calls", async () => {
    const es = new ElasticsearchDriver({
      id: "es",
      name: "es",
      type: "elasticsearch",
    });
    const index = vi.fn();
    const update = vi.fn();
    Object.assign(es, { connected: true, client: { index, update } });
    await expect(
      es.query('PUT /items/_doc/a\n{"amount":9007199254740993}'),
    ).rejects.toThrow(/numeric precision/);
    await expect(
      es.query(
        'PUT /items/_doc/a\n{"amount":42}\nPOST /items/_update/b\n{"doc":{"amount":9007199254740993}}',
      ),
    ).rejects.toThrow(/numeric precision/);
    expect(index).not.toHaveBeenCalled();
    expect(update).not.toHaveBeenCalled();
  });

  it("retains Mongo syntax fallback without swallowing precision errors, and preserves display", () => {
    const mongo = drivers[1];
    expect(mongo.coerceInputValue("not JSON", column)).toBe("not JSON");
    const text = '{"amount":9007199254740993}';
    expect(mongo.formatOutputValue(text, column)).toBe(text);
    expect(() =>
      mongo.coerceInputValue(text, { ...column, category: "array" }),
    ).toThrow(/numeric precision/);
  });

  it.each(
    drivers,
  )("rejects inserts, updates and whole batches before driver writes (%s)", async (driver) => {
    const insert = vi.spyOn(driver, "insertRow");
    const update = vi.spyOn(driver, "updateRows");
    const manager = {
      getConnection: () => ({ id: "test", readOnly: false }),
      getDriver: () => driver,
    } as unknown as ConnectionManager;
    const service = new TableMutationService(manager, {
      getColumns: async () => [column],
    });
    const bad = { _source: '{"amount":9007199254740993}' };
    await expect(
      service.prepareInsertRow("test", "db", "", "items", bad),
    ).rejects.toThrow(/numeric precision/);
    await expect(
      service.updateRow("test", "db", "", "items", { _id: "a" }, bad),
    ).rejects.toThrow(/numeric precision/);
    const result = await applyChangesTransactional(
      manager,
      "test",
      "db",
      "",
      "items",
      [
        { primaryKeys: { _id: "a" }, changes: { _source: '{"amount":42}' } },
        { primaryKeys: { _id: "b" }, changes: bad },
      ],
      [column],
    );
    expect(result.success).toBe(false);
    expect(insert).not.toHaveBeenCalled();
    expect(update).not.toHaveBeenCalled();
    vi.restoreAllMocks();
  });
});

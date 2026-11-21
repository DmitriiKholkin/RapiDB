import {
  Binary,
  BSON,
  BSONRegExp,
  Decimal128,
  type Document,
  Double,
  Int32,
  Long,
  ObjectId,
} from "mongodb";
import { ErrorReply } from "redis";
import { describe, expect, it, vi } from "vitest";
import type { ConnectionManager } from "../../src/extension/connectionManager";
import { DynamoDBDriver } from "../../src/extension/dbDrivers/dynamodb";
import { MongoDBDriver } from "../../src/extension/dbDrivers/mongodb";
import {
  buildMongoObservedDeleteFilter,
  MONGO_DELETE_OBSERVATION_LIMITS,
  observeMongoDeleteState,
} from "../../src/extension/dbDrivers/mongodbDeleteObservation";
import { RedisDriver } from "../../src/extension/dbDrivers/redis";
import {
  REDIS_COMPARE_DELETE,
  REDIS_OBSERVE_DELETE,
} from "../../src/extension/dbDrivers/redisDeleteObservation";
import type {
  ColumnTypeMeta,
  IDBDriver,
  NoSqlDeleteObservation,
} from "../../src/extension/dbDrivers/types";
import { TableDataService } from "../../src/extension/tableDataService";
import { mockStandaloneMongoClient } from "../support/mockStandaloneMongoClient";

const table = "items";
const schema = "";
const idHex = "507f1f77bcf86cd799439011";
const mongoKeys = (type = "objectId") => ({
  _id: { $rapidbMongoId: { type, value: idHex } },
});

function harness(
  driver: IDBDriver,
  type: "mongodb" | "redis" | "dynamodb",
  database: string,
  key: string,
) {
  const columns: ColumnTypeMeta[] = [
    {
      name: key,
      type: "string",
      nativeType: "string",
      category: "text",
      nullable: false,
      isPrimaryKey: true,
      isForeignKey: false,
      filterable: true,
      filterOperators: [],
      valueSemantics: "plain",
    },
  ];
  vi.spyOn(driver, "describeColumns").mockResolvedValue(columns);
  let current = driver;
  const service = new TableDataService({
    getConnection: () => ({ id: type, type }),
    getDriver: () => current,
  } as unknown as ConnectionManager);
  return {
    columns,
    service,
    replaceDriver: (driver: IDBDriver) => {
      current = driver;
    },
    read: () =>
      service.getPage(type, database, schema, table, 1, 50, [], null, true),
    preview: (
      keys: Record<string, unknown>[],
      observations: readonly (NoSqlDeleteObservation | null)[],
      target = table,
    ) =>
      service.prepareDeleteRowsPlan(
        type,
        database,
        schema,
        target,
        keys,
        undefined,
        observations,
      ),
  };
}

// Small evaluator for the generated Mongo expression, used only by the mock
// deleteOne. The fake backend evaluates the predicate against the current row.
function mongoMatch(doc: Document, filter: Document): boolean {
  if (
    !Buffer.from(BSON.serialize({ id: doc._id })).equals(
      BSON.serialize({ id: filter._id.$eq }),
    )
  )
    return false;
  const bsonType = (value: unknown) =>
    value === undefined
      ? "missing"
      : value === null
        ? "null"
        : value instanceof ObjectId
          ? "objectId"
          : value instanceof Int32
            ? "int"
            : value instanceof Long
              ? "long"
              : value instanceof Double
                ? "double"
                : value instanceof Decimal128
                  ? "decimal"
                  : value instanceof RegExp || value instanceof BSONRegExp
                    ? "regex"
                    : value instanceof Binary
                      ? "binData"
                      : Array.isArray(value)
                        ? "array"
                        : typeof value;
  // Server-like $eq: numbers compare by numeric value (+0 equals -0, and
  // Decimal128 1.0 equals 1.00); everything else compares exactly.
  const numeric = (value: unknown): string | undefined => {
    if (value instanceof Double || value instanceof Int32)
      return value.value === 0 ? "0" : String(value.value);
    if (typeof value === "number") return value === 0 ? "0" : String(value);
    if (value instanceof Long) return value.toString();
    if (value instanceof Decimal128) {
      const n = Number(value.toString());
      return n === 0 ? "0" : String(n);
    }
    return undefined;
  };
  const evaluate = (expr: unknown, vars: Document): unknown => {
    if (typeof expr === "string" && expr.startsWith("$$")) {
      const [name, property] = expr.slice(2).split(".");
      return property ? vars[name]?.[property] : vars[name];
    }
    if (expr === null || typeof expr !== "object") return expr;
    const [[op, args]] = Object.entries(expr);
    const run = (value: unknown) => evaluate(value, vars);
    switch (op) {
      case "$literal":
        return args;
      case "$type":
        return bsonType(run(args));
      case "$eq": {
        const left = run(args[0]);
        const right = run(args[1]);
        const [a, b] = [numeric(left), numeric(right)];
        if (a !== undefined && b !== undefined) return a === b;
        return Buffer.from(BSON.serialize({ value: left })).equals(
          BSON.serialize({ value: right }),
        );
      }
      case "$toString": {
        const value = run(args);
        return value instanceof Decimal128 ? value.toString() : String(value);
      }
      case "$atan2": {
        const [y, x] = (args as unknown[]).map(run);
        const number = (value: unknown) =>
          value instanceof Double ? value.value : Number(value);
        return Math.atan2(number(y), number(x));
      }
      case "$lt": {
        const [left, right] = (args as unknown[]).map(run);
        return (left as number) < (right as number);
      }
      case "$and":
        return (args as unknown[]).every(run);
      case "$cond":
        return run(run(args[0]) ? args[1] : args[2]);
      case "$objectToArray":
        return Object.entries(run(args) as Document).map(([k, v]) => ({
          k,
          v,
        }));
      case "$let":
        return evaluate(args.in, {
          ...vars,
          ...Object.fromEntries(
            Object.entries(args.vars).map(([key, value]) => [key, run(value)]),
          ),
        });
      case "$map":
        return (run(args.input) as unknown[]).map((value) =>
          evaluate(args.in, { ...vars, [args.as]: value }),
        );
      case "$arrayElemAt":
        return (run(args[0]) as unknown[])[args[1]];
      case "$size":
        return (run(args) as unknown[]).length;
      default:
        throw new Error(`Unexpected expression ${op}`);
    }
  };
  return evaluate(filter.$expr, { ROOT: doc }) === true;
}

function mongoHarness(doc: Document, raw = true) {
  const driver = new MongoDBDriver({
    id: "mongodb",
    name: "Mongo",
    type: "mongodb",
  });
  const h = harness(driver, "mongodb", "db", "_id");
  let current: Document | undefined = doc;
  const cursor = {
    sort: () => cursor,
    skip: () => cursor,
    limit: () => cursor,
    async *[Symbol.asyncIterator]() {
      if (current) yield raw ? BSON.serialize(current) : current;
    },
  };
  const find = vi.fn(() => cursor);
  const remove = vi.fn(async (filter: Document, _options: unknown) => {
    const deletedCount = current && mongoMatch(current, filter) ? 1 : 0;
    if (deletedCount) current = undefined;
    return { acknowledged: true, deletedCount };
  });
  const client = mockStandaloneMongoClient({ find, deleteOne: remove });
  Object.assign(driver, {
    connected: true,
    client,
    describeSchemaColumns: async () => h.columns,
  });
  return {
    ...h,
    driver,
    client,
    find,
    remove,
    replaceRow: (doc: Document) => {
      current = doc;
    },
    current: () => current,
  };
}

describe("MongoDB observed grid delete", () => {
  const doc = () => ({
    _id: new ObjectId(idHex),
    nested: {
      nullable: null,
      numbers: [
        new Int32(7),
        Long.fromString("9007199254740993"),
        new Double(0.5),
      ],
    },
    data: new Binary(Buffer.from([0, 0xff]), 128),
  });

  it.each([
    [
      "numeric type",
      (item: ReturnType<typeof doc>) => {
        item.nested.numbers[0] = Long.fromNumber(7);
      },
    ],
    [
      "exact integer",
      (item: ReturnType<typeof doc>) => {
        item.nested.numbers[1] = Long.fromString("9007199254740992");
      },
    ],
    [
      "nested NULL removed",
      (item: ReturnType<typeof doc>) => {
        Reflect.deleteProperty(item.nested, "nullable");
      },
    ],
    [
      "new top-level field",
      (item: ReturnType<typeof doc>) => {
        Object.assign(item, { added: null });
      },
    ],
    [
      "binary subtype",
      (item: ReturnType<typeof doc>) => {
        item.data = new Binary(item.data.buffer, 129);
      },
    ],
  ] as const)("does not delete when %s changes after the read", async (_name, change) => {
    const h = mongoHarness(doc());
    const page = await h.read();
    const changed = doc();
    change(changed);
    h.replaceRow(changed);
    const plan = await h.preview(
      [mongoKeys()],
      page.noSqlDeleteObservations ?? [],
    );
    if (!plan) throw new Error("Expected an observed delete plan");
    expect(plan.previewStatements[0]).toMatch(/\$expr.*"locale": "simple"/s);
    expect(h.find).toHaveBeenCalledOnce();
    await expect(
      h.service.executePreparedDeletePlan(plan),
    ).rejects.toMatchObject({
      deleteResult: {
        outcomeUnknown: false,
        affectedRows: 0,
        rowOutcomes: [
          {
            status: "notfound",
            message: expect.stringContaining("no longer matches"),
          },
        ],
      },
    });
    expect(h.find).toHaveBeenCalledOnce();
    expect(h.current()).toBe(changed);
  });

  it("binds the observation to the native _id and target, then deletes a matching BSON document", async () => {
    const h = mongoHarness(doc());
    const { noSqlDeleteObservations = [] } = await h.read();
    await expect(
      h.preview([mongoKeys("string")], noSqlDeleteObservations),
    ).rejects.toThrow(/exact BSON/);
    await expect(
      h.preview([mongoKeys()], noSqlDeleteObservations, "another"),
    ).rejects.toThrow(/exact BSON/);
    expect(h.remove).not.toHaveBeenCalled();
    const plan = await h.preview(
      [mongoKeys(), mongoKeys()],
      [noSqlDeleteObservations[0], noSqlDeleteObservations[0]],
    );
    if (!plan) throw new Error("Expected an observed delete plan");
    await h.service.executePreparedDeletePlan(plan);
    expect(h.remove).toHaveBeenCalledExactlyOnceWith(
      expect.objectContaining({
        _id: { $eq: new ObjectId(idHex) },
        $expr: expect.any(Object),
      }),
      { collation: { locale: "simple" } },
    );
    expect(h.current()).toBeUndefined();
  });

  it.each([
    [doc(), false],
    [{ ...doc(), value: new Double(Number.NaN) }, true],
    [{ ...doc(), value: Decimal128.fromString("NaN") }, true],
  ])("refuses a row without exact provenance or exact equality before a write", async (doc, raw) => {
    const h = mongoHarness(doc, raw);
    const page = await h.read();
    expect(page.noSqlDeleteObservations).toEqual([null]);
    await expect(
      h.preview([mongoKeys()], page.noSqlDeleteObservations ?? []),
    ).rejects.toThrow(/exact BSON/);
    expect(h.remove).not.toHaveBeenCalled();
  });

  it.each([
    ["negative zero", new Double(-0), new Double(0)],
    ["positive zero", new Double(0), new Double(-0)],
    [
      "Decimal128 representation",
      Decimal128.fromString("1.00"),
      Decimal128.fromString("1.0"),
    ],
    [
      "Decimal128 negative zero",
      Decimal128.fromString("-0"),
      Decimal128.fromString("0"),
    ],
    ["regex options", new BSONRegExp("^a", "i"), new BSONRegExp("^a", "im")],
    ["regex pattern", new BSONRegExp("^a", "i"), new BSONRegExp("^b", "i")],
    [
      "infinity",
      new Double(Number.POSITIVE_INFINITY),
      new Double(Number.MAX_VALUE),
    ],
  ])("deletes an unchanged %s value but not one that only compares equal", async (_name, value, replacement) => {
    const original = { ...doc(), value };
    const h = mongoHarness(original);
    const page = await h.read();
    expect(page.noSqlDeleteObservations?.[0]).not.toBeNull();
    const observations = page.noSqlDeleteObservations ?? [];
    h.replaceRow({ ...doc(), value: replacement });
    const plan = await h.preview([mongoKeys()], observations);
    if (!plan) throw new Error("Expected an observed delete plan");
    await expect(
      h.service.executePreparedDeletePlan(plan),
    ).rejects.toMatchObject({
      deleteResult: { affectedRows: 0, rowOutcomes: [{ status: "notfound" }] },
    });
    expect(h.current()).toMatchObject({ value: replacement });

    h.replaceRow(original);
    const again = await h.preview([mongoKeys()], observations);
    if (!again) throw new Error("Expected an observed delete plan");
    await h.service.executePreparedDeletePlan(again);
    expect(h.current()).toBeUndefined();
  });

  it("observes larger documents within the raised limits and keeps the depth bound", () => {
    const wide = {
      _id: new ObjectId(idHex),
      items: Array.from({ length: 1500 }, (_, i) => ({
        n: new Int32(i),
      })).slice(0, 900),
      text: "x".repeat(100 * 1024),
    };
    const wideWire = BSON.serialize(wide);
    expect(wideWire.byteLength).toBeGreaterThan(64 * 1024);
    expect(observeMongoDeleteState(wide, wideWire)).toBeDefined();

    let deep: Document = { leaf: null };
    for (let i = 0; i < MONGO_DELETE_OBSERVATION_LIMITS.depth; i += 1)
      deep = { child: deep };
    const tooDeep = { _id: new ObjectId(idHex), deep };
    expect(
      observeMongoDeleteState(tooDeep, BSON.serialize(tooDeep)),
    ).toBeUndefined();

    const tooMany = {
      _id: new ObjectId(idHex),
      items: Array.from(
        { length: MONGO_DELETE_OBSERVATION_LIMITS.nodes },
        (_, i) => new Int32(i),
      ),
    };
    expect(
      observeMongoDeleteState(tooMany, BSON.serialize(tooMany)),
    ).toBeUndefined();

    const state = observeMongoDeleteState(wide, wideWire);
    if (!state) throw new Error("Expected an observation");
    // The generated filter stays far below the 16 MiB command limit.
    expect(
      BSON.calculateObjectSize(buildMongoObservedDeleteFilter(state)),
    ).toBeLessThan(2 * 1024 * 1024);
  });

  it("rechecks topology after preview and does not attempt a write through a router", async () => {
    const h = mongoHarness(doc());
    const plan = await h.preview(
      [mongoKeys()],
      (await h.read()).noSqlDeleteObservations ?? [],
    );
    if (!plan) throw new Error("Expected an observed delete plan");
    const hello = {
      ok: 1,
      isWritablePrimary: true,
      maxWireVersion: 21,
      msg: "isdbgrid",
    };
    vi.spyOn(h.client, "db").mockReturnValue({
      command: async () => hello,
      collection: () => ({}),
    });
    await expect(
      h.service.executePreparedDeletePlan(plan),
    ).rejects.toMatchObject({
      deleteResult: {
        outcomeUnknown: false,
        rowOutcomes: [{ status: "skipped" }],
      },
    });
    expect(h.remove).not.toHaveBeenCalled();
  });
});

function redisHarness() {
  const driver = new RedisDriver({ id: "redis", name: "Redis", type: "redis" });
  const h = harness(driver, "redis", "db0", "key");
  const state = new Map<
    string,
    {
      type: string;
      bytes: Buffer;
      expiry: number;
      rawValue?: Buffer[];
      visibleHash?: Buffer;
    }
  >([
    [
      "a",
      {
        type: "string",
        bytes: Buffer.from([0xff, 0x00]),
        expiry: 1876000000000,
      },
    ],
    ["b", { type: "string", bytes: Buffer.alloc(0), expiry: -1 }],
    ["c", { type: "string", bytes: Buffer.from("last"), expiry: -1 }],
  ]);
  const sendCommand = vi.fn(
    async ([
      command,
      script,
      count,
      key,
      type,
      expiry,
      bytes,
      visibleHash,
    ]: ReadonlyArray<string | Buffer>) => {
      const row = state.get(String(key));
      expect(count).toBe("1");
      if (command === "EVAL_RO") {
        expect(script).toBe(REDIS_OBSERVE_DELETE);
        expect(expiry).toBe("1");
        return row && row.type === type
          ? [
              Buffer.from(row.type),
              row.expiry,
              row.expiry < 0 ? -1 : 42,
              row.type === "string" ? null : row.bytes,
              row.rawValue ?? row.bytes,
              row.visibleHash ?? null,
            ]
          : null;
      }
      expect(command).toBe("EVAL");
      expect(script).toBe(REDIS_COMPARE_DELETE);
      if (
        row &&
        row.type === type &&
        String(row.expiry) === expiry &&
        row.bytes.equals(bytes as Buffer) &&
        (row.type !== "hash" || row.visibleHash?.equals(visibleHash as Buffer))
      ) {
        state.delete(String(key));
        return 1;
      }
      return 0;
    },
  );
  const client = {
    sendCommand,
    scan: async () => ({
      cursor: "0",
      keys: [...state.keys()].map((key) => Buffer.from(key)),
    }),
    type: async (key: string) => state.get(key)?.type ?? "none",
    strLen: async (key: string) => state.get(key)?.bytes.length ?? 0,
    hLen: async (key: string) => (state.get(key)?.rawValue?.length ?? 0) / 2,
    getRange: async (key: string) => state.get(key)?.bytes,
    withTypeMapping: () => client,
    del: vi.fn(),
  };
  Object.assign(driver, {
    client,
    connected: true,
    getDatabaseClient: async () => client,
  });
  return { ...h, driver, state, client, sendCommand };
}

describe("Redis observed grid delete", () => {
  it("previews and executes the same ordered unique keys even when a later key conflicts", async () => {
    const h = redisHarness();
    const { noSqlDeleteObservations: [a, b, c] = [] } = await h.read();
    const keys = [{ key: "b" }, { key: "a" }, { key: "b" }, { key: "c" }];
    await expect(h.preview(keys, [b, a, c, c])).rejects.toThrow(
      /exact value and expiry/,
    );
    const plan = await h.preview(keys, [b, a, b, c]);
    if (!plan) throw new Error("Expected an observed delete plan");
    expect(plan.previewStatements).toEqual([
      expect.stringMatching(/^EVAL .* "1" "b" "string" "-1" ""$/),
      expect.stringMatching(
        /^EVAL .* "1" "a" "string" "1876000000000" "\\xff\\x00"$/,
      ),
      expect.stringMatching(
        /^EVAL .* "1" "c" "string" "-1" "\\x6c\\x61\\x73\\x74"$/,
      ),
    ]);
    expect(h.sendCommand).toHaveBeenCalledTimes(3); // Only the grid reads.
    h.state.delete("a");
    await expect(
      h.service.executePreparedDeletePlan(plan),
    ).rejects.toMatchObject({
      deleteResult: {
        affectedRows: 2,
        outcomeUnknown: false,
        rowOutcomes: [
          { status: "deleted" },
          { status: "notfound" },
          { status: "deleted" },
          { status: "deleted" },
        ],
      },
    });
    expect(
      h.sendCommand.mock.calls.slice(3).map(([args]) => args.slice(0, 4)),
    ).toEqual(
      ["b", "a", "c"].map((key) => ["EVAL", REDIS_COMPARE_DELETE, "1", key]),
    );
    expect(h.state.size).toBe(0);
    expect(h.client.del).not.toHaveBeenCalled();
  });

  it.each([
    "bytes",
    "expiry",
    "type",
  ] as const)("preserves a replacement with different %s but deletes the matching empty string", async (property) => {
    const h = redisHarness();
    const { rows, noSqlDeleteObservations = [] } = await h.read();
    expect(rows[0].value).toMatchObject({ binaryHex: "ff00" });
    const observed = h.state.get("a");
    if (!observed) throw new Error("Missing fixture");
    h.state.set("a", {
      ...observed,
      [property]:
        property === "bytes"
          ? Buffer.from([0xff, 1])
          : property === "expiry"
            ? -1
            : "list",
    });
    const plan = await h.preview(
      [{ key: "a" }, { key: "b" }],
      noSqlDeleteObservations.slice(0, 2),
    );
    if (!plan) throw new Error("Expected an observed delete plan");
    expect(h.sendCommand).toHaveBeenCalledTimes(3);
    expect(plan.previewStatements[0]).toMatch(
      /^EVAL .*snapshot\(redis\.call\) ~= ARGV\[3\].*\\xff\\x00"$/,
    );
    await expect(
      h.service.executePreparedDeletePlan(plan),
    ).rejects.toMatchObject({
      deleteResult: {
        affectedRows: 1,
        outcomeUnknown: false,
        rowOutcomes: [{ status: "notfound" }, { status: "deleted" }],
      },
    });
    expect(h.state.has("a")).toBe(true);
    expect(h.state.has("b")).toBe(false);
    expect(h.client.del).not.toHaveBeenCalled();
  });

  it("does not replace absent or misbound proof with a fresh read or a key-only write", async () => {
    const h = redisHarness();
    h.sendCommand.mockRejectedValueOnce(
      new ErrorReply("unknown command 'EVAL_RO'"),
    );
    const page = await h.read();
    expect(page.noSqlDeleteObservations?.[0]).toBeNull();
    await expect(
      h.preview([{ key: "a" }], [page.noSqlDeleteObservations?.[0] ?? null]),
    ).rejects.toThrow(/exact value and expiry/);
    await expect(
      h.preview([{ key: "a" }], [page.noSqlDeleteObservations?.[1] ?? null]),
    ).rejects.toThrow(/exact value and expiry/);
    expect(h.sendCommand).toHaveBeenCalledTimes(3);
    expect(h.client.del).not.toHaveBeenCalled();
  });

  it("uses the collection's native payload, not matching display values", async () => {
    const h = redisHarness();
    const collection = {
      type: "hash",
      expiry: -1,
      bytes: Buffer.from([0, 1, 2]),
      rawValue: [Buffer.from([0xff]), Buffer.from("item")],
      visibleHash: Buffer.from([0xff]),
    };
    h.state.set("a", collection);
    const page = await h.read();
    expect(page.rows[0].value).toMatchObject({
      displayValue: JSON.stringify({ "RedisBinary(0xff)": "item" }),
    });
    h.state.set("a", {
      ...collection,
      bytes: Buffer.from([0, 2, 1]),
      rawValue: [Buffer.from("RedisBinary(0xff)"), Buffer.from("item")],
    });
    const plan = await h.preview(
      [{ key: "a" }],
      (page.noSqlDeleteObservations ?? []).slice(0, 1),
    );
    if (!plan) throw new Error("Expected an observed delete plan");
    await expect(
      h.service.executePreparedDeletePlan(plan),
    ).rejects.toMatchObject({
      deleteResult: {
        affectedRows: 0,
        outcomeUnknown: false,
        rowOutcomes: [{ status: "notfound" }],
      },
    });
    expect(h.state.has("a")).toBe(true);
  });

  it("displays a bounded collection without enabling delete when native evidence is unavailable", async () => {
    const h = redisHarness();
    h.state.set("a", {
      type: "hash",
      expiry: -1,
      bytes: Buffer.from([0, 1, 2]),
      rawValue: [Buffer.from("field"), Buffer.from("value")],
    });
    h.sendCommand.mockResolvedValueOnce([
      Buffer.from("hash"),
      null,
      -1,
      null,
      [Buffer.from("field"), Buffer.from("value")],
      null,
    ]);
    const page = await h.read();
    expect(page.rows[0].value).toBe('{"field":"value"}');
    expect(page.noSqlDeleteObservations?.[0]).toBeNull();
    await expect(h.preview([{ key: "a" }], [null])).rejects.toThrow(
      /exact value and expiry/,
    );
    expect(h.sendCommand).toHaveBeenCalledTimes(3);
    expect(h.client.del).not.toHaveBeenCalled();
  });

  it("detects a hash field expiring even if the native dump has not changed", async () => {
    const h = redisHarness();
    const hash = {
      type: "hash",
      expiry: -1,
      bytes: Buffer.from([0, 1, 2]),
      rawValue: [Buffer.from("field"), Buffer.from("value")],
      visibleHash: Buffer.from("5:field5:value"),
    };
    h.state.set("a", hash);
    const plan = await h.preview(
      [{ key: "a" }],
      ((await h.read()).noSqlDeleteObservations ?? []).slice(0, 1),
    );
    if (!plan) throw new Error("Expected an observed delete plan");
    h.state.set("a", { ...hash, rawValue: [], visibleHash: Buffer.alloc(0) });
    await expect(
      h.service.executePreparedDeletePlan(plan),
    ).rejects.toMatchObject({
      deleteResult: {
        affectedRows: 0,
        outcomeUnknown: false,
        rowOutcomes: [{ status: "notfound" }],
      },
    });
    expect(h.state.has("a")).toBe(true);
  });

  it("reports confirmed, unknown and unattempted outcomes without retrying", async () => {
    const h = redisHarness();
    const plan = await h.preview(
      [{ key: "a" }, { key: "b" }, { key: "c" }],
      (await h.read()).noSqlDeleteObservations ?? [],
    );
    if (!plan) throw new Error("Expected an observed delete plan");
    h.sendCommand
      .mockImplementationOnce(async () => 1)
      .mockRejectedValueOnce(new Error("connection closed"));
    await expect(
      h.service.executePreparedDeletePlan(plan),
    ).rejects.toMatchObject({
      deleteResult: {
        affectedRows: 1,
        outcomeUnknown: true,
        rowOutcomes: [
          { status: "deleted" },
          { status: "unknown" },
          { status: "skipped" },
        ],
      },
    });
    expect(h.sendCommand).toHaveBeenCalledTimes(5);
    expect(h.client.del).not.toHaveBeenCalled();
  });

  it("does not execute a prepared plan on a replacement driver", async () => {
    const h = redisHarness();
    const plan = await h.preview(
      [{ key: "a" }],
      ((await h.read()).noSqlDeleteObservations ?? []).slice(0, 1),
    );
    if (!plan) throw new Error("Expected an observed delete plan");
    const replacement = redisHarness();
    h.replaceDriver(replacement.driver);
    await expect(
      h.service.executePreparedDeletePlan(plan),
    ).rejects.toMatchObject({
      deleteResult: {
        outcomeUnknown: false,
        rowOutcomes: [{ status: "skipped" }],
      },
    });
    expect(h.sendCommand).toHaveBeenCalledTimes(3);
    expect(replacement.sendCommand).not.toHaveBeenCalled();
  });
});

it("refuses DynamoDB grid deletion before metadata reads or writes even with supplied evidence", async () => {
  const driver = new DynamoDBDriver({
    id: "dynamodb",
    name: "Dynamo",
    type: "dynamodb",
  });
  const h = harness(driver, "dynamodb", "", "pk");
  const remove = vi.spyOn(driver, "deleteRows");
  await expect(h.preview([{ pk: "a" }], [])).rejects.toThrow(
    /compares the entire item/,
  );
  await expect(
    h.preview([{ pk: "a" }], [{ backend: "mongodb" }]),
  ).rejects.toThrow(/newly added attributes/);
  expect(driver.describeColumns).not.toHaveBeenCalled();
  expect(remove).not.toHaveBeenCalled();
});
